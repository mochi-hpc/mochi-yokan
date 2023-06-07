/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include <cstring>
#include <iostream>
#include "client.hpp"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

struct fetch_context {
    std::vector<std::pair<const void*, size_t>> keys;
    yk_keyvalue_callback_t                      cb;
    void*                                       uargs;
};

static yk_return_t yk_fetch_direct(yk_database_handle_t dbh,
                                   int32_t mode,
                                   size_t count,
                                   const void* keys,
                                   const size_t* ksizes,
                                   yk_keyvalue_callback_t cb,
                                   void* uargs,
                                   const yk_fetch_options_t* options)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !cb)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;

    if(!margo_is_listening(mid))
        return YOKAN_ERR_MID_NOT_LISTENING;

    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    fetch_direct_in_t in;
    fetch_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    fetch_context context;
    context.cb     = cb;
    context.uargs  = uargs;
    size_t key_offset = 0;
    for(unsigned i = 0; i < count; ++i) {
        const void* key = ((const char*)keys)+key_offset;
        size_t ksize    = ksizes[i];
        context.keys.emplace_back(key, ksize);
        key_offset += ksize;
    }

    in.db_id        = dbh->database_id;
    in.mode         = mode;
    in.ksizes.sizes = (size_t*)ksizes;
    in.ksizes.count = count;
    in.keys.data    = (char*)keys;
    in.keys.size    = std::accumulate(ksizes, ksizes+count, (size_t)0);
    in.op_ref       = reinterpret_cast<uint64_t>(&context);

    hret = margo_create(mid, dbh->addr, dbh->client->fetch_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

/**
 * The fetch operations use a single bulk handle exposing data as follows:
 * - The first count*sizeof(size_t) bytes expose the list of key sizes
 * - The following count * sizeof(size_t) bytes expose value sizes
 */

extern "C" yk_return_t yk_fetch_bulk(yk_database_handle_t dbh,
                                     int32_t mode,
                                     size_t count,
                                     const char* origin,
                                     hg_bulk_t data,
                                     size_t offset,
                                     size_t size,
                                     yk_keyvalue_callback_t cb,
                                     void* uargs,
                                     const yk_fetch_options_t* options)
{
    if(count != 0 && size == 0)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;

    if(!margo_is_listening(mid))
        return YOKAN_ERR_MID_NOT_LISTENING;

    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    fetch_in_t in;
    fetch_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    fetch_context context;
    context.cb     = cb;
    context.uargs  = uargs;

    std::vector<size_t> ksizes;
    std::vector<char>   keys;

    if(origin) {
        // data bulk exposing keys is remote, we need to pull it here
        // lookup the address
        hg_addr_t origin_addr = HG_ADDR_NULL;
        hret = margo_addr_lookup(mid, origin, &origin_addr);
        CHECK_HRET(hret, margo_addr_lookup);
        DEFER(margo_addr_free(mid, origin_addr));
        // create and expose local buffers
        ksizes.resize(count);
        keys.resize(size - count*sizeof(size_t));
        std::array<void*,2> buffer_ptrs = { ksizes.data(), keys.data() };
        std::array<hg_size_t, 2> buffer_sizes = { ksizes.size()*sizeof(size_t), keys.size() };
        hg_bulk_t buffer_bulk = HG_BULK_NULL;
        hret = margo_bulk_create(mid, 2, buffer_ptrs.data(), buffer_sizes.data(), HG_BULK_WRITE_ONLY, &buffer_bulk);
        CHECK_HRET(hret, margo_bulk_create);
        DEFER(margo_bulk_free(buffer_bulk));
        // transfer data
        hret = margo_bulk_transfer(
            mid, HG_BULK_PULL, origin_addr, data, offset, buffer_bulk, 0, size);
        CHECK_HRET(hret, margo_bulk_transfer);
        // start looping over keys to fill the context
        size_t key_offset = 0;
        for(unsigned i = 0; i < count; ++i) {
            void*  key   = keys.data() + key_offset;
            size_t ksize = ksizes[i];
            context.keys.emplace_back(key, ksize);
            key_offset += ksize;
        }
    } else {
        // data bulk is local, we can access its memory directly
        size_t ksize_offset = offset;
        size_t key_offset   = offset + count*sizeof(size_t);

        for(unsigned i = 0; i < count; ++i) {
            // note: we try to access memory assuming a maximum of 2 segments,
            // the second segment does not matter. If there is more than 1 for
            // the piece of data we try to access, it's an error.
            void*       seg_ptrs[2] = {nullptr, nullptr};
            hg_size_t   seg_size[2] = {0, 0};
            hg_uint32_t seg_count   = 0;
            // access the size of the current key
            hret = margo_bulk_access(
                data, ksize_offset, sizeof(size_t),
                HG_BULK_READ_ONLY, 1, seg_ptrs, seg_size, &seg_count);
            CHECK_HRET(hret, margo_bulk_access);
            if(seg_count != 1)
                return YOKAN_ERR_NONCONTIG;
            size_t ksize = *((size_t*)seg_ptrs[0]);
            // access the current key
            hret = margo_bulk_access(
                data, key_offset, ksize,
                HG_BULK_READ_ONLY, 1, seg_ptrs, seg_size, &seg_count);
            CHECK_HRET(hret, margo_bulk_access);
            if(seg_count != 1)
                return YOKAN_ERR_NONCONTIG;
            void* key = seg_ptrs[0];
            context.keys.emplace_back(key, ksize);
            ksize_offset += sizeof(size_t);
            key_offset   += ksize;
        }
    }

    in.db_id  = dbh->database_id;
    in.mode   = mode;
    in.count  = count;
    in.bulk   = data;
    in.offset = offset;
    in.size   = size;
    in.origin = const_cast<char*>(origin);
    in.op_ref = reinterpret_cast<uint64_t>(&context);

    hret = margo_create(mid, dbh->addr, dbh->client->fetch_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" yk_return_t yk_fetch(yk_database_handle_t dbh,
                                int32_t mode,
                                const void* key,
                                size_t ksize,
                                yk_keyvalue_callback_t cb,
                                void* uargs)

{
    if(ksize == 0)
        return YOKAN_ERR_INVALID_ARGS;
    return yk_fetch_packed(dbh, mode, 1, key, &ksize, cb, uargs, nullptr);
}

extern "C" yk_return_t yk_fetch_multi(yk_database_handle_t dbh,
                                      int32_t mode,
                                      size_t count,
                                      const void* const*keys,
                                      const size_t* ksizes,
                                      yk_keyvalue_callback_t cb,
                                      void* uargs,
                                      const yk_fetch_options_t* options)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !cb)
        return YOKAN_ERR_INVALID_ARGS;

    margo_instance_id mid = dbh->client->mid;
    if(!margo_is_listening(mid))
        return YOKAN_ERR_MID_NOT_LISTENING;

    hg_return_t hret = HG_SUCCESS;
    hg_bulk_t bulk   = HG_BULK_NULL;
    std::vector<void*> ptrs = {
        const_cast<size_t*>(ksizes),
    };
    std::vector<hg_size_t> sizes = {
        count*sizeof(size_t)
    };
    ptrs.reserve(count+1);
    sizes.reserve(count+1);

    for(unsigned i = 0; i < count; i++) {
        if(ksizes[i] == 0)
            return YOKAN_ERR_INVALID_ARGS;
        ptrs.push_back(const_cast<void*>(keys[i]));
        sizes.push_back(ksizes[i]);
    }

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READ_ONLY, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_fetch_bulk(dbh, mode, count, nullptr, bulk, 0, total_size, cb, uargs, options);
}

extern "C" yk_return_t yk_fetch_packed(yk_database_handle_t dbh,
                                       int32_t mode,
                                       size_t count,
                                       const void* keys,
                                       const size_t* ksizes,
                                       yk_keyvalue_callback_t cb,
                                       void* uargs,
                                       const yk_fetch_options_t* options)
{
    if(mode & YOKAN_MODE_NO_RDMA) {
        return yk_fetch_direct(dbh, mode, count, keys, ksizes, cb, uargs, options);
    }

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !cb)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,2> ptrs = { const_cast<size_t*>(ksizes),
                                 const_cast<void*>(keys) };
    std::array<hg_size_t,2> sizes = { count*sizeof(*ksizes),
                                      std::accumulate(ksizes, ksizes+count, (size_t)0) };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    if(sizes[1] == 0)
        return YOKAN_ERR_INVALID_ARGS;

    hret = margo_bulk_create(mid, 2, ptrs.data(), sizes.data(),
                             HG_BULK_READ_ONLY, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_fetch_bulk(dbh, mode, count, nullptr, bulk, 0, total_size, cb, uargs, options);
}

void yk_fetch_back_ult(hg_handle_t h)
{
    hg_return_t hret = HG_SUCCESS;
    fetch_back_in_t in;
    fetch_back_out_t out;

    std::memset(&in, 0, sizeof(in));
    std::memset(&out, 0, sizeof(out));

    out.ret = YOKAN_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    auto info = margo_get_info(h);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    fetch_context* context = reinterpret_cast<fetch_context*>(in.op_ref);

    if(context->keys.size() != in.count) {
        out.ret = YOKAN_ERR_OTHER; // should not be happening
        return;
    }

    // create bulk for the values
    std::vector<size_t>     vsizes(in.count);
    std::vector<char>       values(in.size - in.count*sizeof(size_t));
    std::array<void*,2>     values_ptrs = { vsizes.data(), values.data() };
    std::array<hg_size_t,2> values_sizes = { vsizes.size()*sizeof(size_t), values.size() };
    hg_bulk_t values_bulk  = HG_BULK_NULL;
    hret = margo_bulk_create(mid, 2, values_ptrs.data(), values_sizes.data(), HG_BULK_WRITE_ONLY, &values_bulk);
    CHECK_HRET_OUT(hret, margo_bulk_create);
    DEFER(margo_bulk_free(values_bulk));

    // pull the values
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.bulk, 0, values_bulk, 0, in.size);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    size_t val_offset = 0;
    for(unsigned i = 0; i < in.count; ++i) {
        const void* key = context->keys[i].first;
        size_t ksize    = context->keys[i].second;
        void*  val      = values.data() + val_offset;
        size_t vsize    = vsizes[i];
        out.ret = (context->cb)(context->uargs, in.start + i, key, ksize, val, vsize);
        if(out.ret != YOKAN_SUCCESS)
            break;
        val_offset += vsize <= YOKAN_LAST_VALID_SIZE ? vsize : 0;
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_fetch_back_ult)

void yk_fetch_direct_back_ult(hg_handle_t h)
{
    hg_return_t hret = HG_SUCCESS;
    fetch_direct_back_in_t in;
    fetch_direct_back_out_t out;

    std::memset(&in, 0, sizeof(in));
    std::memset(&out, 0, sizeof(out));

    out.ret = YOKAN_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    fetch_context* context = reinterpret_cast<fetch_context*>(in.op_ref);

    if(context->keys.size() != in.vsizes.count) {
        out.ret = YOKAN_ERR_OTHER; // should not be happening
        return;
    }

    size_t val_offset = 0;
    for(unsigned i = 0; i < in.vsizes.count; ++i) {
        auto key   = context->keys[i].first;
        auto ksize = context->keys[i].second;
        auto vsize = in.vsizes.sizes[i];
        auto val   = ((const char*)in.vals.data) + val_offset;
        out.ret = (context->cb)(context->uargs, in.start + i, key, ksize, val, vsize);
        val_offset += vsize <= YOKAN_LAST_VALID_SIZE ? vsize : 0;
        if(out.ret != YOKAN_SUCCESS)
            break;
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_fetch_direct_back_ult)

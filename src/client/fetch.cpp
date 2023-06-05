/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include <cstring>
#include "client.h"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

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

    in.db_id        = dbh->database_id;
    in.mode         = mode;
    in.ksizes.sizes = (size_t*)ksizes;
    in.ksizes.count = count;
    in.keys.data    = (char*)keys;
    in.keys.size    = std::accumulate(ksizes, ksizes+count, (size_t)0);

    hret = margo_create(mid, dbh->addr, dbh->client->fetch_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    // TODO

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

    in.db_id  = dbh->database_id;
    in.mode   = mode;
    in.count  = count;
    in.bulk   = data;
    in.offset = offset;
    in.size   = size;
    in.origin = const_cast<char*>(origin);

    hret = margo_create(mid, dbh->addr, dbh->client->fetch_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    // TODO

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

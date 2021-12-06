/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include <iostream>
#include "client.h"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

static yk_return_t yk_list_keyvals_direct(yk_database_handle_t dbh,
                                          int32_t mode,
                                          const void* from_key,
                                          size_t from_ksize,
                                          const void* filter,
                                          size_t filter_size,
                                          size_t count,
                                          void* keys,
                                          size_t keys_buf_size,
                                          size_t* ksizes,
                                          void* values,
                                          size_t vals_buf_size,
                                          size_t* vsizes)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    if(from_key == nullptr && from_ksize > 0)
        return YOKAN_ERR_INVALID_ARGS;
    if(filter == nullptr && filter_size > 0)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    list_keyvals_direct_in_t in;
    list_keyvals_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id         = dbh->database_id;
    in.mode          = mode;
    in.count         = count;
    in.from_key.size = from_ksize;
    in.from_key.data = (char*)from_key;
    in.filter.size   = filter_size;
    in.filter.data   = (char*)filter;
    in.keys_buf_size = keys_buf_size;
    in.vals_buf_size = vals_buf_size;

    out.ksizes.sizes = ksizes;
    out.ksizes.count = count;
    out.keys.data    = (char*)keys;
    out.keys.size    = keys_buf_size;
    out.vsizes.sizes = vsizes;
    out.vsizes.count = count;
    out.vals.data    = (char*)values;
    out.vals.size    = vals_buf_size;

    hret = margo_create(mid, dbh->addr, dbh->client->list_keyvals_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    out.ksizes.sizes = nullptr;
    out.ksizes.count = 0;
    out.keys.data    = nullptr;
    out.keys.size    = 0;
    out.vsizes.sizes = nullptr;
    out.vsizes.count = 0;
    out.vals.data    = nullptr;
    out.vals.size    = 0;

    ret = static_cast<yk_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

/**
 * The list operations use a single bulk handle exposing data as follows:
 * - The first from_ksize bytes represent the start key.
 * - The next filter_size byres represent the filter.
 * - The next count * sizeof(size_t) bytes represent the key sizes.
 * - The next count * sizeof(size_t) bytes represent the value sizes.
 * - The next keys_buf_size bytes store keys back to back
 * - The next vals_buf_size bytes store values back to back
 * A "packed" flag is used to indicate whether the server can copy values
 * back to back in the remaining M bytes, or if it should follow the value
 * sizes specified by the sender.
 */

extern "C" yk_return_t yk_list_keyvals_bulk(yk_database_handle_t dbh,
                                              int32_t mode,
                                              size_t from_ksize,
                                              size_t filter_size,
                                              const char* origin,
                                              hg_bulk_t data,
                                              size_t offset,
                                              size_t keys_buf_size,
                                              size_t vals_buf_size,
                                              bool packed,
                                              size_t count)
{
    if(count == 0)
        return YOKAN_SUCCESS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    list_keyvals_in_t in;
    list_keyvals_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id         = dbh->database_id;
    in.mode          = mode;
    in.packed        = packed;
    in.count         = count;
    in.from_ksize    = from_ksize;
    in.filter_size   = filter_size;
    in.offset        = offset;
    in.keys_buf_size = keys_buf_size;
    in.vals_buf_size = vals_buf_size;
    in.origin        = const_cast<char*>(origin);
    in.bulk          = data;

    hret = margo_create(mid, dbh->addr, dbh->client->list_keyvals_id, &handle);
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

extern "C" yk_return_t yk_list_keyvals(yk_database_handle_t dbh,
                                         int32_t mode,
                                         const void* from_key,
                                         size_t from_ksize,
                                         const void* filter,
                                         size_t filter_size,
                                         size_t count,
                                         void* const* keys,
                                         size_t* ksizes,
                                         void* const* values,
                                         size_t* vsizes)
{
    if(mode & YOKAN_MODE_NO_RDMA)
        return YOKAN_ERR_OP_UNSUPPORTED;

    if(count == 0)
        return YOKAN_SUCCESS;
    if(from_key == nullptr && from_ksize > 0)
        return YOKAN_ERR_INVALID_ARGS;
    if(filter == nullptr && filter_size > 0)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    margo_instance_id mid = dbh->client->mid;
    std::vector<void*> ptrs;
    std::vector<hg_size_t> sizes;
    // from_key
    if(from_key && from_ksize) {
        ptrs.push_back(const_cast<void*>(from_key));
        sizes.push_back(from_ksize);
    }
    // filter
    if(filter && filter_size) {
        ptrs.push_back(const_cast<void*>(filter));
        sizes.push_back(filter_size);
    }
    // ksizes
    ptrs.push_back(ksizes);
    sizes.push_back(count*sizeof(*ksizes));
    // vsizes
    ptrs.push_back(vsizes);
    sizes.push_back(count*sizeof(*vsizes));

    ptrs.reserve(ptrs.size() + count*2);
    sizes.reserve(sizes.size() + count*2);
    // keys
    size_t keys_buf_size = 0;
    for(unsigned i = 0; i < count; i++) {
        if(ksizes[i] == 0) continue;
        ptrs.push_back(const_cast<void*>(keys[i]));
        sizes.push_back(ksizes[i]);
        keys_buf_size += ksizes[i];
    }
    // values
    size_t vals_buf_size = 0;
    for(unsigned i = 0; i < count; i++) {
        if(vsizes[i] == 0) continue;
        ptrs.push_back(const_cast<void*>(values[i]));
        sizes.push_back(vsizes[i]);
        vals_buf_size += vsizes[i];
    }

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_list_keyvals_bulk(dbh, mode, from_ksize, filter_size,
                                 nullptr, bulk, 0, keys_buf_size,
                                 vals_buf_size, false, count);
}

extern "C" yk_return_t yk_list_keyvals_packed(yk_database_handle_t dbh,
                                                int32_t mode,
                                                const void* from_key,
                                                size_t from_ksize,
                                                const void* filter,
                                                size_t filter_size,
                                                size_t count,
                                                void* keys,
                                                size_t keys_buf_size,
                                                size_t* ksizes,
                                                void* values,
                                                size_t vals_buf_size,
                                                size_t* vsizes)
{
    if(mode & YOKAN_MODE_NO_RDMA)
        return yk_list_keyvals_direct(dbh, mode, from_key,
                from_ksize, filter, filter_size, count,
                keys, keys_buf_size, ksizes,
                values, vals_buf_size, vsizes);

    if(count == 0)
        return YOKAN_SUCCESS;
    if(from_key == nullptr && from_ksize > 0)
        return YOKAN_ERR_INVALID_ARGS;
    if(filter == nullptr && filter_size > 0)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,6> ptrs;
    std::array<hg_size_t,6> sizes;

    unsigned i = 0;
    if(from_key && from_ksize) {
        ptrs[i]  = const_cast<void*>(from_key);
        sizes[i] = from_ksize;
        i += 1;
    }
    if(filter && filter_size) {
        ptrs[i]  = const_cast<void*>(filter);
        sizes[i] = filter_size;
        i += 1;
    }
    ptrs[i]  = ksizes;
    sizes[i] = count * sizeof(*ksizes);
    i += 1;
    ptrs[i]  = vsizes;
    sizes[i] = count * sizeof(*ksizes);
    i += 1;
    ptrs[i]  = keys;
    sizes[i] = keys_buf_size;
    i += 1;
    ptrs[i]  = values;
    sizes[i] = vals_buf_size;
    i += 1;

    margo_instance_id mid = dbh->client->mid;

    hret = margo_bulk_create(mid, i, ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_list_keyvals_bulk(dbh, mode, from_ksize, filter_size,
                                 nullptr, bulk, 0, keys_buf_size, vals_buf_size,
                                 true, count);
}

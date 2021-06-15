/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include "client.h"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

/**
 * The list operations use a single bulk handle exposing data as follows:
 * - The first from_ksize bytes represent the start key.
 * - The next prefix_size byres represent the prefix.
 * - The next count * sizeof(size_t) bytes represent the key sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * A "packed" flag is used to indicate whether the server can copy values
 * back to back in the remaining M bytes, or if it should follow the value
 * sizes specified by the sender.
 */

extern "C" rkv_return_t rkv_list_keys_bulk(rkv_database_handle_t dbh,
                                           bool inclusive,
                                           size_t from_ksize,
                                           size_t prefix_size,
                                           const char* origin,
                                           hg_bulk_t data,
                                           size_t offset,
                                           size_t keys_buf_size,
                                           bool packed,
                                           size_t count)
{
    if(count == 0)
        return RKV_SUCCESS;

    margo_instance_id mid = dbh->client->mid;
    rkv_return_t ret = RKV_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    list_keys_in_t in;
    list_keys_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id         = dbh->database_id;
    in.count         = count;
    in.bulk          = data;
    in.offset        = offset;
    in.origin        = const_cast<char*>(origin);
    in.packed        = packed;
    in.from_ksize    = from_ksize;
    in.prefix_size   = prefix_size;
    in.keys_buf_size = keys_buf_size;
    in.inclusive     = inclusive;

    hret = margo_create(mid, dbh->addr, dbh->client->list_keys_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<rkv_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" rkv_return_t rkv_list_keys(rkv_database_handle_t dbh,
                                      bool inclusive,
                                      const void* from_key,
                                      size_t from_ksize,
                                      const void* prefix,
                                      size_t prefix_size,
                                      size_t count,
                                      void* const* keys,
                                      size_t* ksizes)
{
    if(count == 0)
        return RKV_SUCCESS;
    if(from_key == nullptr && from_ksize > 0)
        return RKV_ERR_INVALID_ARGS;
    if(prefix == nullptr && prefix_size > 0)
        return RKV_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    margo_instance_id mid = dbh->client->mid;
    std::vector<void*> ptrs;
    std::vector<size_t> sizes;
    // from_key
    if(from_key && from_ksize) {
        ptrs.push_back(const_cast<void*>(from_key));
        sizes.push_back(from_ksize);
    }
    // prefix
    if(prefix && prefix_size) {
        ptrs.push_back(const_cast<void*>(prefix));
        sizes.push_back(prefix_size);
    }
    // ksizes
    ptrs.push_back(ksizes);
    sizes.push_back(count*sizeof(*ksizes));
    // keys
    ptrs.reserve(ptrs.size() + count);
    sizes.reserve(sizes.size() + count);
    for(unsigned i = 0; i < count; i++) {
        if(ksizes[i] == 0)
            return RKV_ERR_INVALID_ARGS;
        ptrs.push_back(const_cast<void*>(keys[i]));
        sizes.push_back(ksizes[i]);
    }

    size_t keys_buf_size = std::accumulate(ksizes, ksizes+count, 0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_list_keys_bulk(dbh, inclusive, from_ksize, prefix_size,
                              nullptr, bulk, 0, keys_buf_size,
                              false, count);
}

extern "C" rkv_return_t rkv_list_keys_packed(rkv_database_handle_t dbh,
                                             bool inclusive,
                                             const void* from_key,
                                             size_t from_ksize,
                                             const void* prefix,
                                             size_t prefix_size,
                                             size_t count,
                                             void* keys,
                                             size_t keys_buf_size,
                                             size_t* ksizes)
{
    if(count == 0)
        return RKV_SUCCESS;
    if(from_key == nullptr && from_ksize > 0)
        return RKV_ERR_INVALID_ARGS;
    if(prefix == nullptr && prefix_size > 0)
        return RKV_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,4> ptrs;
    std::array<hg_size_t,4> sizes;

    unsigned i = 0;
    if(from_key && from_ksize) {
        ptrs[i]  = const_cast<void*>(from_key);
        sizes[i] = from_ksize;
        i += 1;
    }
    if(prefix && prefix_size) {
        ptrs[i]  = const_cast<void*>(prefix);
        sizes[i] = prefix_size;
        i += 1;
    }
    ptrs[i]  = ksizes;
    sizes[i] = count * sizeof(*ksizes);
    i += 1;
    ptrs[i]  = keys;
    sizes[i] = keys_buf_size;
    i += 1;

    margo_instance_id mid = dbh->client->mid;

    hret = margo_bulk_create(mid, i, ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_list_keys_bulk(dbh, inclusive, from_ksize, prefix_size,
                              nullptr, bulk, 0, keys_buf_size,
                              true, count);
}

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
 * The length operations use a single bulk handle exposing data as follows:
 * - The first count*sizeof(size_t) bytes expose the list of key sizes
 * - The following N bytes expose keys (packed back to back), where
 *   N = sum of key sizes
 * - The following count * sizeof(size_t) bytes expose value sizes.
 * The server will pull the key sizes, compute N, then pull the keys,
 * get the length of each value, then push the value sizes back to the sender.
 */

extern "C" rkv_return_t rkv_length_bulk(rkv_database_handle_t dbh,
                                        size_t count,
                                        const char* origin,
                                        hg_bulk_t data,
                                        size_t offset,
                                        size_t size)
{
    if(count != 0 && size == 0)
        return RKV_ERR_INVALID_ARGS;

    margo_instance_id mid = dbh->client->mid;
    rkv_return_t ret = RKV_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    length_in_t in;
    length_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id  = dbh->database_id;
    in.count  = count;
    in.bulk   = data;
    in.offset = offset;
    in.size   = size;
    in.origin = const_cast<char*>(origin);

    hret = margo_create(mid, dbh->addr, dbh->client->length_id, &handle);
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

extern "C" rkv_return_t rkv_length(rkv_database_handle_t dbh,
                                   const void* key,
                                   size_t ksize,
                                   size_t* vsize)
{
    if(ksize == 0)
        return RKV_ERR_INVALID_ARGS;
    rkv_return_t ret = rkv_length_packed(dbh, 1, key, &ksize, vsize);
    if(ret == RKV_SUCCESS) {
        if(*vsize == RKV_KEY_NOT_FOUND) ret = RKV_ERR_KEY_NOT_FOUND;
    }
    return ret;
}

extern "C" rkv_return_t rkv_length_multi(rkv_database_handle_t dbh,
                                      size_t count,
                                      const void* const* keys,
                                      const size_t* ksizes,
                                      size_t* vsizes)
{
    if(count == 0)
        return RKV_SUCCESS;
    else if(!keys || !ksizes || !vsizes)
        return RKV_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::vector<void*> ptrs = {
        const_cast<size_t*>(ksizes),
    };
    ptrs.reserve(count+2);
    std::vector<hg_size_t> sizes = {
        count*sizeof(*ksizes),
    };
    sizes.reserve(count+2);
    margo_instance_id mid = dbh->client->mid;

    for(unsigned i = 0; i < count; i++) {
        if(ksizes[i] == 0)
            return RKV_ERR_INVALID_ARGS;
        ptrs.push_back(const_cast<void*>(keys[i]));
        sizes.push_back(ksizes[i]);
    }
    ptrs.push_back(vsizes);
    sizes.push_back(count*sizeof(*vsizes));

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), 0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_length_bulk(dbh, count, nullptr, bulk, 0, total_size);
}

extern "C" rkv_return_t rkv_length_packed(rkv_database_handle_t dbh,
                                       size_t count,
                                       const void* keys,
                                       const size_t* ksizes,
                                       size_t* vsizes)
{
    if(count == 0)
        return RKV_SUCCESS;
    else if(!keys || !ksizes || !vsizes)
        return RKV_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,3> ptrs = { const_cast<size_t*>(ksizes),
                                 const_cast<void*>(keys),
                                 const_cast<size_t*>(vsizes) };
    std::array<hg_size_t,3> sizes = { count*sizeof(*ksizes),
                                      std::accumulate(ksizes, ksizes+count, (size_t)0),
                                      count*sizeof(*vsizes) };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (hg_size_t)0);

    if(sizes[1] == 0)
        return RKV_ERR_INVALID_ARGS;

    hret = margo_bulk_create(mid, 3, ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_length_bulk(dbh, count, nullptr, bulk, 0, total_size);
}

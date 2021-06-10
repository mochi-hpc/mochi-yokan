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
 * The get operations use a single bulk handle exposing data as follows:
 * - The first count*sizeof(size_t) bytes expose the list of key sizes
 * - The following count * sizeof(size_t) bytes expose value sizes
 * - The following N bytes expose keys (packed back to back), where
 *   N = sum of key sizes
 * - The remaining M bytes are for values.
 * A "packed" flag is used to indicate whether the server can copy values
 * back to back in the remaining M bytes, or if it should follow the value
 * sizes specified by the sender.
 */

extern "C" rkv_return_t rkv_get_bulk(rkv_database_handle_t dbh,
                                     size_t count,
                                     const char* origin,
                                     hg_bulk_t data,
                                     size_t offset,
                                     size_t size,
                                     bool packed)
{
    if(count != 0 && size == 0)
        return RKV_ERR_INVALID_ARGS;

    margo_instance_id mid = dbh->client->mid;
    rkv_return_t ret = RKV_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    get_in_t in;
    get_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id  = dbh->database_id;
    in.count  = count;
    in.bulk   = data;
    in.offset = offset;
    in.size   = size;
    in.origin = const_cast<char*>(origin);
    in.packed = packed;

    hret = margo_create(mid, dbh->addr, dbh->client->get_id, &handle);
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

extern "C" rkv_return_t rkv_get(rkv_database_handle_t dbh,
                                const void* key,
                                size_t ksize,
                                void* value,
                                size_t* vsize)
{
    if(ksize == 0)
        return RKV_ERR_INVALID_ARGS;
    rkv_return_t ret = rkv_get_packed(dbh, 1, key, &ksize, *vsize, value, vsize);
    if(ret != RKV_SUCCESS)
        return ret;
    else if(*vsize == RKV_SIZE_TOO_SMALL)
        return RKV_ERR_BUFFER_SIZE;
    else if(*vsize == RKV_KEY_NOT_FOUND)
        return RKV_ERR_KEY_NOT_FOUND;
    return RKV_SUCCESS;
}

extern "C" rkv_return_t rkv_get_multi(rkv_database_handle_t dbh,
                                      size_t count,
                                      const void* const* keys,
                                      const size_t* ksizes,
                                      void* const* values,
                                      size_t* vsizes)
{
    if(count == 0)
        return RKV_SUCCESS;
    else if(!keys || !ksizes || !values || !vsizes)
        return RKV_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::vector<void*> ptrs = {
        const_cast<size_t*>(ksizes),
        const_cast<size_t*>(vsizes)
    };
    ptrs.reserve(2*count+2);
    std::vector<hg_size_t> sizes = {
        count*sizeof(*ksizes),
        count*sizeof(*vsizes)
    };
    sizes.reserve(2*count+2);
    margo_instance_id mid = dbh->client->mid;

    for(unsigned i = 0; i < count; i++) {
        if(ksizes[i] == 0)
            return RKV_ERR_INVALID_ARGS;
        ptrs.push_back(const_cast<void*>(keys[i]));
        sizes.push_back(ksizes[i]);
    }
    for(unsigned i = 0; i < count; i++) {
        if(vsizes[i] == 0)
            continue;
        ptrs.push_back(const_cast<void*>(values[i]));
        sizes.push_back(vsizes[i]);
    }

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), 0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_get_bulk(dbh, count, nullptr, bulk, 0, total_size, false);
}

extern "C" rkv_return_t rkv_get_packed(rkv_database_handle_t dbh,
                                       size_t count,
                                       const void* keys,
                                       const size_t* ksizes,
                                       size_t vbufsize,
                                       void* values,
                                       size_t* vsizes)
{
    if(count == 0)
        return RKV_SUCCESS;
    else if(!keys || !ksizes || !vsizes || (!values && vbufsize))
        return RKV_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,4> ptrs = { const_cast<size_t*>(ksizes),
                                 const_cast<size_t*>(vsizes),
                                 const_cast<void*>(keys),
                                 const_cast<void*>(values) };
    std::array<hg_size_t,4> sizes = { count*sizeof(*ksizes),
                                      count*sizeof(*vsizes),
                                      std::accumulate(ksizes, ksizes+count, (size_t)0),
                                      vbufsize };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (hg_size_t)0);

    if(sizes[2] == 0)
        return RKV_ERR_INVALID_ARGS;

    if(sizes[3] != 0)
        hret = margo_bulk_create(mid, 4, ptrs.data(), sizes.data(),
                                 HG_BULK_READWRITE, &bulk);
    else
        hret = margo_bulk_create(mid, 3, ptrs.data(), sizes.data(),
                                 HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_get_bulk(dbh, count, nullptr, bulk, 0, total_size, true);
}
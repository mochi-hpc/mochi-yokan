/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include <cstring>
#include "client.hpp"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

static yk_return_t yk_put_direct(yk_database_handle_t dbh,
                                 int32_t mode,
                                 size_t count,
                                 const void* keys,
                                 const size_t* ksizes,
                                 const void* values,
                                 const size_t* vsizes)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !vsizes)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    put_direct_in_t in;
    put_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.mode   = mode;
    in.ksizes.sizes = (size_t*)ksizes;
    in.ksizes.count = count;
    in.vsizes.sizes = (size_t*)vsizes;
    in.vsizes.count = count;
    in.keys.data = (char*)keys;
    in.keys.size = std::accumulate(ksizes, ksizes+count, (size_t)0);
    in.vals.data = (char*)values;
    in.vals.size = std::accumulate(vsizes, vsizes+count, (size_t)0);

    if(in.vals.data == nullptr && in.vals.size != 0)
        return YOKAN_ERR_INVALID_ARGS;

    hret = margo_create(mid, dbh->addr, dbh->client->put_direct_id, &handle);
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

extern "C" yk_return_t yk_put_bulk(yk_database_handle_t dbh,
                                   int32_t mode,
                                   size_t count,
                                   const char* origin,
                                   hg_bulk_t data,
                                   size_t offset,
                                   size_t size)
{
    if(count != 0 && size == 0)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    put_in_t in;
    put_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.mode   = mode;
    in.count  = count;
    in.bulk   = data;
    in.offset = offset;
    in.size   = size;
    in.origin = const_cast<char*>(origin);

    hret = margo_create(mid, dbh->addr, dbh->client->put_id, &handle);
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

extern "C" yk_return_t yk_put(yk_database_handle_t dbh,
                                int32_t mode,
                                const void* key,
                                size_t ksize,
                                const void* value,
                                size_t vsize)
{
    if(ksize == 0)
        return YOKAN_ERR_INVALID_ARGS;
    return yk_put_packed(dbh, mode, 1, key, &ksize, value, &vsize);
}

extern "C" yk_return_t yk_put_multi(yk_database_handle_t dbh,
                                    int32_t mode,
                                    size_t count,
                                    const void* const* keys,
                                    const size_t* ksizes,
                                    const void* const* values,
                                    const size_t* vsizes)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !values || !vsizes)
        return YOKAN_ERR_INVALID_ARGS;

    if(mode & YOKAN_MODE_NO_RDMA) {
        if(count == 1) {
            return yk_put_direct(dbh, mode, count, keys[0], ksizes, values[0], vsizes);
        }
        std::vector<char> packed_keys(std::accumulate(ksizes, ksizes+count, (size_t)0));
        std::vector<char> packed_vals(std::accumulate(vsizes, vsizes+count, (size_t)0));
        size_t koffset = 0, voffset = 0;
        for(size_t i = 0; i < count; i++) {
            std::memcpy(packed_keys.data()+koffset, keys[i], ksizes[i]);
            std::memcpy(packed_vals.data()+voffset, values[i], vsizes[i]);
            koffset += ksizes[i];
            voffset += vsizes[i];
        }
        return yk_put_direct(dbh, mode, count, packed_keys.data(),
                             ksizes, packed_vals.data(), vsizes);
    }

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
            return YOKAN_ERR_INVALID_ARGS;
        ptrs.push_back(const_cast<void*>(keys[i]));
        sizes.push_back(ksizes[i]);
    }
    for(unsigned i = 0; i < count; i++) {
        if(vsizes[i] == 0)
            continue;
        ptrs.push_back(const_cast<void*>(values[i]));
        sizes.push_back(vsizes[i]);
    }

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READ_ONLY, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_put_bulk(dbh, mode, count, nullptr, bulk, 0, total_size);
}

extern "C" yk_return_t yk_put_packed(yk_database_handle_t dbh,
                                       int32_t mode,
                                       size_t count,
                                       const void* keys,
                                       const size_t* ksizes,
                                       const void* values,
                                       const size_t* vsizes)
{
    if(mode & YOKAN_MODE_NO_RDMA) {
        return yk_put_direct(dbh, mode, count, keys, ksizes, values, vsizes);
    }

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !vsizes)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,4> ptrs = { const_cast<size_t*>(ksizes),
                                 const_cast<size_t*>(vsizes),
                                 const_cast<void*>(keys),
                                 const_cast<void*>(values) };
    std::array<hg_size_t,4> sizes = { count*sizeof(*ksizes),
                                      count*sizeof(*vsizes),
                                      std::accumulate(ksizes, ksizes+count, (size_t)0),
                                      std::accumulate(vsizes, vsizes+count, (size_t)0) };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    if(sizes[2] == 0)
        return YOKAN_ERR_INVALID_ARGS;

    if(sizes[3] != 0 && values == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    if(sizes[3] != 0)
        hret = margo_bulk_create(mid, 4, ptrs.data(), sizes.data(),
                                 HG_BULK_READ_ONLY, &bulk);
    else
        hret = margo_bulk_create(mid, 3, ptrs.data(), sizes.data(),
                                 HG_BULK_READ_ONLY, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_put_bulk(dbh, mode, count, nullptr, bulk, 0, total_size);
}

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

static yk_return_t yk_get_direct(yk_database_handle_t dbh,
                                 int32_t mode,
                                 size_t count,
                                 const void* keys,
                                 const size_t* ksizes,
                                 size_t vbufsize,
                                 void* values,
                                 size_t* vsizes)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !vsizes || (!values && vbufsize))
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    get_direct_in_t in;
    get_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id        = dbh->database_id;
    in.mode         = mode;
    in.vbufsize     = vbufsize;
    in.ksizes.sizes = (size_t*)ksizes;
    in.ksizes.count = count;
    in.keys.data    = (char*)keys;
    in.keys.size    = std::accumulate(ksizes, ksizes+count, 0);

    out.vsizes.sizes = vsizes;
    out.vsizes.count = count;
    out.vals.data    = (char*)values;
    out.vals.size    = vbufsize;

    hret = margo_create(mid, dbh->addr, dbh->client->get_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

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

extern "C" yk_return_t yk_get_bulk(yk_database_handle_t dbh,
                                   int32_t mode,
                                   size_t count,
                                   const char* origin,
                                   hg_bulk_t data,
                                   size_t offset,
                                   size_t size,
                                   bool packed)
{
    if(count != 0 && size == 0)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    get_in_t in;
    get_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id  = dbh->database_id;
    in.mode   = mode;
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

    ret = static_cast<yk_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" yk_return_t yk_get(yk_database_handle_t dbh,
                              int32_t mode,
                              const void* key,
                              size_t ksize,
                              void* value,
                              size_t* vsize)
{
    if(ksize == 0)
        return YOKAN_ERR_INVALID_ARGS;
    yk_return_t ret = yk_get_packed(dbh, mode, 1, key, &ksize, *vsize, value, vsize);
    if(ret != YOKAN_SUCCESS) return ret;
    else if(*vsize == YOKAN_SIZE_TOO_SMALL)
        return YOKAN_ERR_BUFFER_SIZE;
    else if(*vsize == YOKAN_KEY_NOT_FOUND)
        return YOKAN_ERR_KEY_NOT_FOUND;
    return YOKAN_SUCCESS;
}

extern "C" yk_return_t yk_get_multi(yk_database_handle_t dbh,
                                    int32_t mode,
                                    size_t count,
                                    const void* const* keys,
                                    const size_t* ksizes,
                                    void* const* values,
                                    size_t* vsizes)
{
    if(mode & YOKAN_MODE_NO_RDMA)
        return YOKAN_ERR_OP_UNSUPPORTED;
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !values || !vsizes)
        return YOKAN_ERR_INVALID_ARGS;

    hg_return_t hret = HG_SUCCESS;
    hg_bulk_t bulk   = HG_BULK_NULL;
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

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), 0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_get_bulk(dbh, mode, count, nullptr, bulk, 0, total_size, false);
}

extern "C" yk_return_t yk_get_packed(yk_database_handle_t dbh,
                                     int32_t mode,
                                     size_t count,
                                     const void* keys,
                                     const size_t* ksizes,
                                     size_t vbufsize,
                                     void* values,
                                     size_t* vsizes)
{
    if(mode & YOKAN_MODE_NO_RDMA) {
        return yk_get_direct(dbh, mode, count, keys, ksizes, vbufsize, values, vsizes);
    }

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !vsizes || (!values && vbufsize))
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
                                      vbufsize };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (hg_size_t)0);

    if(sizes[2] == 0)
        return YOKAN_ERR_INVALID_ARGS;

    int seg_count = sizes[3] != 0 ? 4 : 3;
    hret = margo_bulk_create(mid, seg_count, ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_get_bulk(dbh, mode, count, nullptr, bulk, 0, total_size, true);
}

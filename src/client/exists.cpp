/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include <cstring>
#include <cmath>
#include "client.h"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

static yk_return_t yk_exists_direct(yk_database_handle_t dbh,
                                    int32_t mode,
                                    size_t count,
                                    const void* keys,
                                    const size_t* ksizes,
                                    uint8_t* flags)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !flags)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    exists_direct_in_t in;
    exists_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    out.flags.data = (char*)flags;
    out.flags.size = std::ceil(((double)count)/8.0);

    in.db_id       = dbh->database_id;
    in.mode        = mode;
    in.keys.data   = (char*)keys;
    in.keys.size   = std::accumulate(ksizes, ksizes+count, 0);
    in.sizes.sizes = (size_t*)ksizes;
    in.sizes.count = count;

    hret = margo_create(mid, dbh->addr, dbh->client->exists_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);

    out.flags.data = nullptr;
    out.flags.size = 0;

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

/**
 * The exists operations use a single bulk handle exposing data as follows:
 * - The first count*sizeof(size_t) bytes expose the list of key sizes
 * - The following N bytes expose keys (packed back to back), where
 *   N = sum of key sizes
 * - The following M = ceil(count/8) bytes expose a bit field
 *   storing whether each key exists in the database.
 * The server will pull the key sizes, compute N, then pull the keys,
 * get whether each key exists, then push the result back to the sender.
 *
 * Note: the bitfield uses bytes from left to right, but bits from the
 * least significant to the most signficant. For instance considering 16 keys:
 * [00001001][10000000] indicates that that keys 0, 3 and 15 exist.
 */

extern "C" yk_return_t yk_exists_bulk(yk_database_handle_t dbh,
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
    exists_in_t in;
    exists_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id  = dbh->database_id;
    in.mode   = mode;
    in.count  = count;
    in.bulk   = data;
    in.offset = offset;
    in.size   = size;
    in.origin = const_cast<char*>(origin);

    hret = margo_create(mid, dbh->addr, dbh->client->exists_id, &handle);
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

extern "C" yk_return_t yk_exists(yk_database_handle_t dbh,
                                   int32_t mode,
                                   const void* key,
                                   size_t ksize,
                                   uint8_t* flag)
{
    if(ksize == 0)
        return YOKAN_ERR_INVALID_ARGS;
    return yk_exists_packed(dbh, mode, 1, key, &ksize, flag);
}

extern "C" yk_return_t yk_exists_multi(yk_database_handle_t dbh,
                                       int32_t mode,
                                       size_t count,
                                       const void* const* keys,
                                       const size_t* ksizes,
                                       uint8_t* flags)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !flags)
        return YOKAN_ERR_INVALID_ARGS;

    if(mode & YOKAN_MODE_NO_RDMA) {
        if(count == 1) {
            return yk_exists_direct(dbh, mode, count, keys[0], ksizes, flags);
        }
        auto total_ksizes = std::accumulate(ksizes, ksizes+count, 0);
        std::vector<char> packed_keys(total_ksizes);
        size_t offset = 0;
        for(size_t i=0; i < count; i++) {
            std::memcpy(packed_keys.data()+offset, keys[i], ksizes[i]);
            offset += ksizes[i];
        }
        return yk_exists_direct(dbh, mode, count, packed_keys.data(), ksizes, flags);
    }

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
            return YOKAN_ERR_INVALID_ARGS;
        ptrs.push_back(const_cast<void*>(keys[i]));
        sizes.push_back(ksizes[i]);
    }
    ptrs.push_back(flags);
    sizes.push_back(count*sizeof(*flags));

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), 0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_exists_bulk(dbh, mode, count, nullptr, bulk, 0, total_size);
}

extern "C" yk_return_t yk_exists_packed(yk_database_handle_t dbh,
                                         int32_t mode,
                                         size_t count,
                                         const void* keys,
                                         const size_t* ksizes,
                                         uint8_t* flags)
{
    if(mode & YOKAN_MODE_NO_RDMA)
        return yk_exists_direct(dbh, mode, count, keys, ksizes, flags);

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!keys || !ksizes || !flags)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,3> ptrs = { const_cast<size_t*>(ksizes),
                                 const_cast<void*>(keys),
                                 flags };
    std::array<hg_size_t,3> sizes = { count*sizeof(*ksizes),
                                      std::accumulate(ksizes, ksizes+count, (size_t)0),
                                      count*sizeof(*flags) };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (hg_size_t)0);

    if(sizes[1] == 0)
        return YOKAN_ERR_INVALID_ARGS;

    hret = margo_bulk_create(mid, 3, ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_exists_bulk(dbh, mode, count, nullptr, bulk, 0, total_size);
}

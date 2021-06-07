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

extern "C" rkv_return_t rkv_put_bulk(rkv_database_handle_t dbh,
                                     size_t count,
                                     const char* origin,
                                     hg_bulk_t data,
                                     size_t offset,
                                     size_t size)
{
    margo_instance_id mid = dbh->client->mid;
    rkv_return_t ret = RKV_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    put_in_t in;
    put_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id  = dbh->database_id;
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

    ret = static_cast<rkv_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" rkv_return_t rkv_put(rkv_database_handle_t dbh,
                                const void* key,
                                size_t ksize,
                                const void* value,
                                size_t vsize)
{
    return rkv_put_packed(dbh, 1, key, &ksize, value, &vsize);
}

extern "C" rkv_return_t rkv_put_multi(rkv_database_handle_t dbh,
                                      size_t count,
                                      const void* const* keys,
                                      const size_t* ksizes,
                                      const void* const* values,
                                      const size_t* vsizes)
{
    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::vector<void*> ptrs(2*count+2, nullptr);
    std::vector<hg_size_t> sizes(2*count+2, 0);
    margo_instance_id mid = dbh->client->mid;
    ptrs[0]  = const_cast<size_t*>(ksizes);
    ptrs[1]  = const_cast<size_t*>(vsizes);
    sizes[0] = count*sizeof(*ksizes);
    sizes[1] = count*sizeof(*vsizes);

    for(unsigned i = 0; i < count; i++) {
        ptrs[2+i]        = const_cast<void*>(keys[i]);
        ptrs[2+count+i]  = const_cast<void*>(values[i]);
        sizes[2+i]       = ksizes[i];
        sizes[2+count+i] = vsizes[i];
    }

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), 0);

    hret = margo_bulk_create(mid, 2*count+2, ptrs.data(), sizes.data(),
                             HG_BULK_READ_ONLY, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_put_bulk(dbh, count, nullptr, bulk, 0, total_size);
}

extern "C" rkv_return_t rkv_put_packed(rkv_database_handle_t dbh,
                                       size_t count,
                                       const void* keys,
                                       const size_t* ksizes,
                                       const void* values,
                                       const size_t* vsizes)
{
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

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), 0);

    hret = margo_bulk_create(mid, 4, ptrs.data(), sizes.data(),
                             HG_BULK_READ_ONLY, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return rkv_put_bulk(dbh, count, nullptr, bulk, 0, total_size);
}

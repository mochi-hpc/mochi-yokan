/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <cstring>
#include <numeric>
#include "client.hpp"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

static yk_return_t yk_doc_store_direct(yk_database_handle_t dbh,
                                       const char* collection,
                                       int32_t mode,
                                       size_t count,
                                       const void* records,
                                       const size_t* rsizes,
                                       yk_id_t* ids) {

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(rsizes == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_store_direct_in_t in;
    doc_store_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    out.ids.ids   = ids;
    out.ids.count = count;

    in.mode        = mode;
    in.coll_name   = (char*)collection;
    in.sizes.count = count;
    in.sizes.sizes = (uint64_t*)rsizes;
    in.docs.size   = std::accumulate(rsizes, rsizes+count, (size_t)0);
    in.docs.data   = (char*)records;

    if(records == NULL && in.docs.size != 0)
        return YOKAN_ERR_INVALID_ARGS;


    hret = margo_create(mid, dbh->addr, dbh->client->doc_store_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);

    out.ids.ids = NULL;
    out.ids.count = 0;

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" yk_return_t yk_doc_store_bulk(yk_database_handle_t dbh,
                                         const char* name,
                                         int32_t mode,
                                         size_t count,
                                         const char* origin,
                                         hg_bulk_t data,
                                         size_t offset,
                                         size_t size,
                                         yk_id_t* ids) {
    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_store_in_t in;
    doc_store_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    out.ids.ids   = ids;
    out.ids.count = count;

    in.mode      = mode;
    in.coll_name = (char*)name;
    in.count     = count;
    in.origin    = (char*)origin;
    in.bulk      = data;
    in.offset    = offset;
    in.size      = size;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_store_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);

    out.ids.ids = NULL;
    out.ids.count = 0;

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" yk_return_t yk_doc_store_packed(yk_database_handle_t dbh,
                                           const char* collection,
                                           int32_t mode,
                                           size_t count,
                                           const void* records,
                                           const size_t* rsizes,
                                           yk_id_t* ids) {
    if(mode & YOKAN_MODE_NO_RDMA) {
        return yk_doc_store_direct(dbh, collection, mode,
                                   count, records, rsizes, ids);
    }

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(rsizes == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,2> ptrs = { const_cast<size_t*>(rsizes),
                                 const_cast<void*>(records) };
    std::array<hg_size_t,2> sizes = { count*sizeof(*rsizes),
                                      std::accumulate(rsizes, rsizes+count, (size_t)0) };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    if(sizes[1] != 0 && records == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    if(sizes[1] != 0)
        hret = margo_bulk_create(mid, 2, ptrs.data(), sizes.data(),
                                 HG_BULK_READ_ONLY, &bulk);
    else
        hret = margo_bulk_create(mid, 1, ptrs.data(), sizes.data(),
                                 HG_BULK_READ_ONLY, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_doc_store_bulk(dbh, collection, mode, count, nullptr, bulk, 0, total_size, ids);
}

extern "C" yk_return_t yk_doc_store_multi(yk_database_handle_t dbh,
                                          const char* collection,
                                          int32_t mode,
                                          size_t count,
                                          const void* const* records,
                                          const size_t* rsizes,
                                          yk_id_t* ids) {
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!records || !rsizes)
        return YOKAN_ERR_INVALID_ARGS;

    if(mode & YOKAN_MODE_NO_RDMA) {
        std::vector<char> packed_records;
        if(count > 1) {
            size_t total_size = std::accumulate(rsizes, rsizes+count, (size_t)0);
            packed_records.resize(total_size);
            size_t offset = 0;
            for(size_t i=0; i < count; i++) {
                std::memcpy(packed_records.data()+offset, records[i], rsizes[i]);
                offset += rsizes[i];
            }
            return yk_doc_store_direct(dbh, collection, mode, count,
                                   packed_records.data(), rsizes, ids);
        } else {
            return yk_doc_store_direct(dbh, collection, mode, count,
                                   records[0], rsizes, ids);
        }
    }

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::vector<void*> ptrs = {
        const_cast<size_t*>(rsizes)
    };
    ptrs.reserve(count+1);
    std::vector<hg_size_t> sizes = {
        count*sizeof(*rsizes)
    };
    sizes.reserve(count+1);
    margo_instance_id mid = dbh->client->mid;

    for(unsigned i = 0; i < count; i++) {
        if(rsizes[i] != 0) {
            ptrs.push_back(const_cast<void*>(records[i]));
            sizes.push_back(rsizes[i]);
        }
    }

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READ_ONLY, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_doc_store_bulk(dbh, collection, mode, count, nullptr, bulk, 0, total_size, ids);
}

extern "C" yk_return_t yk_doc_store(yk_database_handle_t dbh,
                                    const char* collection,
                                    int32_t mode,
                                    const void* record,
                                    size_t size,
                                    yk_id_t* id) {
    return yk_doc_store_packed(dbh, collection, mode, 1, record, &size, id);
}

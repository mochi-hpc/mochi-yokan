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

static yk_return_t yk_doc_update_direct(yk_database_handle_t dbh,
                                        const char* collection,
                                        int32_t mode,
                                        size_t count,
                                        const yk_id_t* ids,
                                        const void* records,
                                        const size_t* rsizes) {
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!rsizes || !ids)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_update_direct_in_t in;
    doc_update_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.mode        = mode;
    in.coll_name   = (char*)collection;
    in.ids.count   = count;
    in.ids.ids     = (yk_id_t*)ids;
    in.sizes.count = count;
    in.sizes.sizes = (size_t*)rsizes;
    in.docs.data   = (char*)records;
    in.docs.size   = std::accumulate(rsizes, rsizes+count, (size_t)0);

    if(in.docs.data == nullptr && in.docs.size != 0)
        return YOKAN_ERR_INVALID_ARGS;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_update_direct_id, &handle);
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

extern "C" yk_return_t yk_doc_update_bulk(yk_database_handle_t dbh,
                                          const char* name,
                                          int32_t mode,
                                          size_t count,
                                          const yk_id_t* ids,
                                          const char* origin,
                                          hg_bulk_t data,
                                          size_t offset,
                                          size_t size) {
    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_update_in_t in;
    doc_update_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.mode      = mode;
    in.coll_name = (char*)name;
    in.ids.count = count;
    in.ids.ids   = (yk_id_t*)ids;
    in.origin    = (char*)origin;
    in.bulk      = data;
    in.offset    = offset;
    in.size      = size;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_update_id, &handle);
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

extern "C" yk_return_t yk_doc_update_packed(yk_database_handle_t dbh,
                                            const char* collection,
                                            int32_t mode,
                                            size_t count,
                                            const yk_id_t* ids,
                                            const void* records,
                                            const size_t* rsizes) {
    if(mode & YOKAN_MODE_NO_RDMA)
        return yk_doc_update_direct(dbh, collection, mode, count, ids, records, rsizes);

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!rsizes || !ids)
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

    return yk_doc_update_bulk(dbh, collection, mode, count, ids, nullptr, bulk, 0, total_size);
}

extern "C" yk_return_t yk_doc_update_multi(yk_database_handle_t dbh,
                                           const char* collection,
                                           int32_t mode,
                                           size_t count,
                                           const yk_id_t* ids,
                                           const void* const* records,
                                           const size_t* rsizes) {

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!records || !rsizes || !ids)
        return YOKAN_ERR_INVALID_ARGS;

    if(mode & YOKAN_MODE_NO_RDMA) {
        if(count == 1) {
            return yk_doc_update_direct(dbh, collection, mode,
                                        count, ids, records[0], rsizes);
        }
        std::vector<char> packed_records(std::accumulate(rsizes, rsizes+count, (size_t)0));
        size_t offset = 0;
        for(size_t i = 0; i < count; i++) {
            std::memcpy(packed_records.data()+offset, records[i], rsizes[i]);
            offset += rsizes[i];
        }
        return yk_doc_update_direct(dbh, collection, mode, count, ids,
                                    packed_records.data(), rsizes);
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
        if(rsizes[i] == 0) continue;
        ptrs.push_back(const_cast<void*>(records[i]));
        sizes.push_back(rsizes[i]);
    }

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READ_ONLY, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_doc_update_bulk(dbh, collection, mode, count, ids, nullptr, bulk, 0, total_size);
}

extern "C" yk_return_t yk_doc_update(yk_database_handle_t dbh,
                                     const char* collection,
                                     int32_t mode,
                                     yk_id_t id,
                                     const void* record,
                                     size_t size) {
    return yk_doc_update_packed(dbh, collection, mode, 1, &id, record, &size);
}

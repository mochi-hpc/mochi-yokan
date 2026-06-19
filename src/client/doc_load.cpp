/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include <iostream>
#include "client.hpp"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"
#include "../common/extras.h"

static yk_return_t yk_doc_load_direct(yk_database_handle_t dbh,
                                      const char* collection,
                                      int32_t mode,
                                      size_t count,
                                      const yk_id_t* ids,
                                      size_t rbufsize,
                                      void* records,
                                      size_t* rsizes, ...) {
    YK_EXTRACT_EXTRAS(extras, mode, rsizes);

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!ids || !rsizes || (!records && rbufsize))
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_load_direct_in_t in;
    doc_load_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.mode      = mode;
    in.timeout_ms = extras.timeout_ms;
    in.coll_name = (char*)collection;
    in.ids.count = count;
    in.ids.ids   = (yk_id_t*)ids;
    in.bufsize   = rbufsize;

    out.sizes.sizes = rsizes;
    out.sizes.count = count;
    out.docs.data   = (char*)records;
    out.docs.size   = rbufsize;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_load_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward_timed(dbh->provider_id, handle, &in, extras.timeout_ms);
    CHECK_HRET(hret, margo_provider_forward_timed);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);

    out.sizes.sizes = nullptr;
    out.sizes.count = 0;
    out.docs.data   = nullptr;
    out.docs.size   = 0;

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}


extern "C" yk_return_t yk_doc_load_bulk(yk_database_handle_t dbh,
                                        const char* name,
                                        int32_t mode,
                                        size_t count,
                                        const yk_id_t* ids,
                                        const char* origin,
                                        hg_bulk_t data,
                                        size_t offset,
                                        size_t size,
                                        bool packed, ...) {
    YK_EXTRACT_EXTRAS(extras, mode, packed);

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_load_in_t in;
    doc_load_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.mode      = mode;
    in.timeout_ms = extras.timeout_ms;
    in.coll_name = (char*)name;
    in.ids.count = count;
    in.ids.ids   = (yk_id_t*)ids;
    in.origin    = (char*)origin;
    in.bulk      = data;
    in.offset    = offset;
    in.size      = size;
    in.packed    = packed;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_load_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward_timed(dbh->provider_id, handle, &in, extras.timeout_ms);
    CHECK_HRET(hret, margo_provider_forward_timed);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" yk_return_t yk_doc_load_packed(yk_database_handle_t dbh,
                                          const char* collection,
                                          int32_t mode,
                                          size_t count,
                                          const yk_id_t* ids,
                                          size_t rbufsize,
                                          void* records,
                                          size_t* rsizes, ...) {
    YK_EXTRACT_EXTRAS(extras, mode, rsizes);


    if(mode & YOKAN_MODE_NO_RDMA) {
        return yk_doc_load_direct(dbh, collection, YK_MODE_WITH_EXTRA(mode), count, ids,
                                  rbufsize, records, rsizes, YK_REEMIT_EXTRAS(extras));
    }
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!ids || !rsizes || (!records && rbufsize))
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,2> ptrs = { const_cast<size_t*>(rsizes),
                                 const_cast<void*>(records) };
    std::array<hg_size_t,2> sizes = { count*sizeof(*rsizes),
                                      rbufsize };
    margo_instance_id mid = dbh->client->mid;

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    int seg_count = sizes[1] != 0 ? 2 : 1;
    hret = margo_bulk_create(mid, seg_count, ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_doc_load_bulk(dbh, collection, YK_MODE_WITH_EXTRA(mode), count, ids, nullptr, bulk, 0, total_size, true, YK_REEMIT_EXTRAS(extras));
}

extern "C" yk_return_t yk_doc_load_multi(yk_database_handle_t dbh,
                                         const char* collection,
                                         int32_t mode,
                                         size_t count,
                                         const yk_id_t* ids,
                                         void* const* records,
                                         size_t* rsizes, ...) {
    YK_EXTRACT_EXTRAS(extras, mode, rsizes);

    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!ids || !rsizes || !records)
        return YOKAN_ERR_INVALID_ARGS;

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
        if(rsizes[i] == 0)
            continue;
        ptrs.push_back(const_cast<void*>(records[i]));
        sizes.push_back(rsizes[i]);
    }

    size_t total_size = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_doc_load_bulk(dbh, collection, YK_MODE_WITH_EXTRA(mode), count, ids, nullptr, bulk, 0, total_size, false, YK_REEMIT_EXTRAS(extras));
}

extern "C" yk_return_t yk_doc_load(yk_database_handle_t dbh,
                                   const char* collection,
                                   int32_t mode,
                                   yk_id_t id,
                                   void* record,
                                   size_t* size, ...) {
    YK_EXTRACT_EXTRAS(extras, mode, size);

    if(!size) return YOKAN_ERR_INVALID_ARGS;
    auto ret = yk_doc_load_packed(dbh, collection, YK_MODE_WITH_EXTRA(mode), 1, &id, *size, record, size, YK_REEMIT_EXTRAS(extras));
    if(ret != YOKAN_SUCCESS) return ret;
    else if(*size == YOKAN_SIZE_TOO_SMALL)
        return YOKAN_ERR_BUFFER_SIZE;
    else if(*size == YOKAN_KEY_NOT_FOUND)
        return YOKAN_ERR_KEY_NOT_FOUND;
    return YOKAN_SUCCESS;
}

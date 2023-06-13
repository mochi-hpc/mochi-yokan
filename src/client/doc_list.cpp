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

static yk_return_t yk_doc_list_direct(yk_database_handle_t dbh,
                                      const char* collection,
                                      int32_t mode,
                                      yk_id_t start_id,
                                      const void* filter,
                                      size_t filter_size,
                                      size_t count,
                                      yk_id_t* ids,
                                      size_t bufsize,
                                      void* docs,
                                      size_t* doc_sizes)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    if(filter == nullptr && filter_size > 0) {
        return YOKAN_ERR_INVALID_ARGS;
    }
    if(ids == nullptr || (docs == nullptr && bufsize != 0) || doc_sizes == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_list_direct_in_t in;
    doc_list_direct_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id         = dbh->database_id;
    in.mode          = mode;
    in.count         = count;
    in.from_id       = start_id;
    in.coll_name     = (char*)collection;
    in.filter.data   = (char*)filter;
    in.filter.size   = filter_size;
    in.bufsize       = bufsize;

    out.ids.ids     = ids;
    out.ids.count   = count;
    out.sizes.sizes = doc_sizes;
    out.sizes.count = count;
    out.docs.data   = (char*)docs;
    out.docs.size   = bufsize;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_list_direct_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);
    out.ids.ids     = nullptr;
    out.ids.count   = 0;
    out.sizes.sizes = nullptr;
    out.sizes.count = 0;
    out.docs.data   = nullptr;
    out.docs.size   = 0;

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

/**
 * The list operations use a single bulk handle exposing data as follows:
 * - The first filter_size bytes represent the filter.
 * - The next count * sizeof(size_t) bytes represent the available document sizes.
 * - The next count * sizeof(yk_id_t) is used to send back document ids.
 * - The next docs_buf_size bytes store documents back to back
 * A "packed" flag is used to indicate whether the server can copy documents
 * back to back, or if it should follow the buffer
 * sizes specified by the sender.
 */

extern "C" yk_return_t yk_doc_list_bulk(yk_database_handle_t dbh,
                                        const char* collection,
                                        int32_t mode,
                                        yk_id_t from_id,
                                        size_t filter_size,
                                        const char* origin,
                                        hg_bulk_t data,
                                        size_t offset,
                                        size_t docs_buf_size,
                                        bool packed,
                                        size_t count)
{
    if(count == 0)
        return YOKAN_SUCCESS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_list_in_t in;
    doc_list_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id         = dbh->database_id;
    in.mode          = mode;
    in.coll_name     = (char*)collection;
    in.packed        = packed;
    in.from_id       = from_id;
    in.count         = count;
    in.filter_size   = filter_size;
    in.offset        = offset;
    in.docs_buf_size = docs_buf_size;
    in.origin        = const_cast<char*>(origin);
    in.bulk          = data;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_list_id, &handle);
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

extern "C" yk_return_t yk_doc_list(yk_database_handle_t dbh,
                                   const char* collection,
                                   int32_t mode,
                                   yk_id_t start_id,
                                   const void* filter,
                                   size_t filter_size,
                                   size_t count,
                                   yk_id_t* ids,
                                   void* const* docs,
                                   size_t* doc_sizes)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    if(filter == nullptr && filter_size > 0)
        return YOKAN_ERR_INVALID_ARGS;
    if(ids == nullptr || docs == nullptr || doc_sizes == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    margo_instance_id mid = dbh->client->mid;
    std::vector<void*> ptrs;
    std::vector<hg_size_t> sizes;
    // filter
    if(filter && filter_size) {
        ptrs.push_back(const_cast<void*>(filter));
        sizes.push_back(filter_size);
    }
    // doc sizes
    ptrs.push_back(doc_sizes);
    sizes.push_back(count*sizeof(*doc_sizes));
    // ids
    ptrs.push_back(ids);
    sizes.push_back(count*sizeof(*ids));

    ptrs.reserve(ptrs.size() + count);
    sizes.reserve(sizes.size() + count);

    // docs
    size_t docs_buf_size = 0;
    for(unsigned i = 0; i < count; i++) {
        if(doc_sizes[i] == 0) continue;
        ptrs.push_back(const_cast<void*>(docs[i]));
        sizes.push_back(doc_sizes[i]);
        docs_buf_size += doc_sizes[i];
    }

    hret = margo_bulk_create(mid, ptrs.size(), ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);
    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_doc_list_bulk(dbh, collection, mode, start_id, filter_size,
                            nullptr, bulk, 0, docs_buf_size,
                            false, count);
}

extern "C" yk_return_t yk_doc_list_packed(yk_database_handle_t dbh,
                                          const char* collection,
                                          int32_t mode,
                                          yk_id_t start_id,
                                          const void* filter,
                                          size_t filter_size,
                                          size_t count,
                                          yk_id_t* ids,
                                          size_t bufsize,
                                          void* docs,
                                          size_t* doc_sizes)
{
    if(mode & YOKAN_MODE_NO_RDMA)
        return yk_doc_list_direct(dbh, collection, mode,
                start_id, filter, filter_size, count, ids,
                bufsize, docs, doc_sizes);

    if(count == 0)
        return YOKAN_SUCCESS;
    if(filter == nullptr && filter_size > 0) {
        return YOKAN_ERR_INVALID_ARGS;
    }
    if(ids == nullptr || (docs == nullptr && bufsize != 0) || doc_sizes == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    hg_bulk_t bulk   = HG_BULK_NULL;
    hg_return_t hret = HG_SUCCESS;
    std::array<void*,4> ptrs;
    std::array<hg_size_t,4> sizes;

    unsigned i = 0;
    if(filter && filter_size) {
        ptrs[i]  = const_cast<void*>(filter);
        sizes[i] = filter_size;
        i += 1;
    }
    ptrs[i]  = doc_sizes;
    sizes[i] = count * sizeof(*doc_sizes);
    i += 1;
    ptrs[i]  = ids;
    sizes[i] = count * sizeof(*ids);
    i += 1;
    ptrs[i]  = docs;
    sizes[i] = bufsize;
    i += 1;

    margo_instance_id mid = dbh->client->mid;

    hret = margo_bulk_create(mid, i, ptrs.data(), sizes.data(),
                             HG_BULK_READWRITE, &bulk);

    CHECK_HRET(hret, margo_bulk_create);
    DEFER(margo_bulk_free(bulk));

    return yk_doc_list_bulk(dbh, collection, mode, start_id, filter_size,
                            nullptr, bulk, 0, bufsize,
                            true, count);
}

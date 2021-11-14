/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/logging.h"
#include "../common/checks.h"
#include <numeric>
#include <iostream>

void yk_doc_list_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_list_in_t in;
    doc_list_out_t out;
    hg_addr_t origin_addr = HG_ADDR_NULL;

    out.ret = YOKAN_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    yk_provider_t provider = (yk_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(in.origin) {
        hret = margo_addr_lookup(mid, in.origin, &origin_addr);
        CHECK_HRET_OUT(hret, margo_addr_lookup);
    } else {
        hret = margo_addr_dup(mid, info->addr, &origin_addr);
        CHECK_HRET_OUT(hret, margo_addr_dup);
    }
    DEFER(margo_addr_free(mid, origin_addr));

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    size_t buffer_size = in.filter_size
                       + in.count*sizeof(size_t)
                       + in.count*sizeof(yk_id_t)
                       + in.docs_buf_size;

    yk_buffer_t buffer = provider->bulk_cache.get(
        provider->bulk_cache_data, buffer_size, HG_BULK_READWRITE);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, buffer));

    const size_t doc_sizes_offset = in.filter_size;
    const size_t doc_ids_offset   = doc_sizes_offset + in.count*sizeof(size_t);
    const size_t docs_offset      = doc_ids_offset + in.count*sizeof(yk_id_t);

    size_t size_to_transfer = in.filter_size;
    // transfer doc_sizes only if in.packed is false
    if(!in.packed) size_to_transfer += in.count*sizeof(size_t);

    if(size_to_transfer > 0) {
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                in.bulk, in.offset, buffer->bulk, 0, size_to_transfer);
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }

    // build buffer wrappers
    auto ptr       = buffer->data;
    auto filter_umem = yokan::UserMem{ ptr, in.filter_size };
    auto filter      = yokan::DocFilter::makeFilter(mid, in.mode, filter_umem);
    auto doc_sizes = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr + doc_sizes_offset),
        in.count
    };
    auto ids = yokan::BasicUserMem<yk_id_t>{
        reinterpret_cast<yk_id_t*>(ptr + doc_ids_offset),
        in.count
    };
    auto docs = yokan::UserMem{ ptr + docs_offset, in.docs_buf_size };

    out.ret = static_cast<yk_return_t>(
            database->docList(
                in.coll_name,
                in.mode, in.packed,
                in.from_id, filter,
                ids, docs, doc_sizes));

    if(out.ret == YOKAN_SUCCESS) {
        size_to_transfer = in.count*sizeof(size_t)
                         + in.count*sizeof(yk_id_t)
                         + in.docs_buf_size;
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                in.bulk, in.offset + doc_sizes_offset,
                buffer->bulk, doc_sizes_offset, size_to_transfer);
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_list_ult)

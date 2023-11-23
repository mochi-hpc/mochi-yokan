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

void yk_doc_store_ult(hg_handle_t h)
{
    std::vector<yk_id_t> ids;
    hg_return_t hret;
    doc_store_in_t in;
    doc_store_out_t out;
    hg_addr_t origin_addr = HG_ADDR_NULL;

    out.ret = YOKAN_SUCCESS;
    out.ids.ids = nullptr;
    out.ids.count = 0;

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

    yk_database* database = provider->db;
    CHECK_MODE_SUPPORTED(database, in.mode);

    yk_buffer_t buffer = provider->bulk_cache.get(
        provider->bulk_cache_data, in.size, HG_BULK_READWRITE);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, buffer));

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                               in.bulk, in.offset, buffer->bulk, 0, in.size);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    auto ptr = buffer->data;
    auto sizes_umem = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr),
        in.count
    };
    ptr += in.count*sizeof(size_t);
    auto total_doc_size = std::accumulate(sizes_umem.data, sizes_umem.data + in.count, (size_t)0);
    if(in.size  < in.count*sizeof(size_t) + total_doc_size) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }
    auto docs_umem = yokan::UserMem{
        ptr,
        total_doc_size
    };

    ids.resize(in.count);
    auto ids_umem = yokan::BasicUserMem<yk_id_t>{ids};

    out.ret = static_cast<yk_return_t>(
        database->docStore(in.coll_name, in.mode, docs_umem, sizes_umem, ids_umem));

    if(out.ret == YOKAN_SUCCESS) {
        out.ids.count = in.count;
        out.ids.ids = ids.data();
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_store_ult)

void yk_doc_store_direct_ult(hg_handle_t h)
{
    std::vector<yk_id_t> ids;
    hg_return_t hret;
    doc_store_direct_in_t in;
    doc_store_direct_out_t out;

    in.docs.data   = NULL;
    in.docs.size   = 0;
    in.sizes.sizes = NULL;
    in.sizes.count = 0;

    out.ret = YOKAN_SUCCESS;
    out.ids.ids = nullptr;
    out.ids.count = 0;

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

    auto count = in.sizes.count;

    yk_database* database = provider->db;
    CHECK_MODE_SUPPORTED(database, in.mode);

    auto sizes_umem = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(in.sizes.sizes),
        count
    };
    auto docs_umem = yokan::UserMem{
        in.docs.data,
        in.docs.size
    };

    ids.resize(count);
    auto ids_umem = yokan::BasicUserMem<yk_id_t>{ids};

    out.ret = static_cast<yk_return_t>(
        database->docStore(in.coll_name, in.mode, docs_umem, sizes_umem, ids_umem));

    if(out.ret == YOKAN_SUCCESS) {
        out.ids.count = count;
        out.ids.ids = ids.data();
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_store_direct_ult)

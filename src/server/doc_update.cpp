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

void yk_doc_update_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_update_in_t in;
    doc_update_out_t out;
    hg_addr_t origin_addr = HG_ADDR_NULL;

    in.ids.ids = nullptr;
    in.ids.count = 0;

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

    yk_buffer_t buffer = provider->bulk_cache.get(
        provider->bulk_cache_data, in.size, HG_BULK_READWRITE);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, buffer));

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                               in.bulk, in.offset, buffer->bulk, 0, in.size);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    yk_database* database = provider->db;
    CHECK_MODE_SUPPORTED(database, in.mode);

    auto ptr = buffer->data;
    auto sizes_umem = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr),
        in.ids.count
    };
    ptr += in.ids.count*sizeof(size_t);
    auto total_doc_size = std::accumulate(sizes_umem.data, sizes_umem.data + in.ids.count, (size_t)0);
    if(in.size  < in.ids.count*sizeof(size_t) + total_doc_size) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }
    auto docs_umem = yokan::UserMem{
        ptr,
        total_doc_size
    };
    auto ids_umem = yokan::BasicUserMem<yk_id_t>{
        in.ids.ids,
        in.ids.count
    };

    out.ret = static_cast<yk_return_t>(
        database->docUpdate(in.coll_name, in.mode, ids_umem, docs_umem, sizes_umem));
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_update_ult)

void yk_doc_update_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_update_direct_in_t in;
    doc_update_direct_out_t out;

    in.ids.ids     = nullptr;
    in.ids.count   = 0;
    in.sizes.count = 0;
    in.sizes.sizes = nullptr;
    in.docs.data   = nullptr;
    in.docs.size   = 0;

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

    yk_database* database = provider->db;
    CHECK_MODE_SUPPORTED(database, in.mode);

    auto sizes_umem = yokan::BasicUserMem<size_t>{
        in.sizes.sizes,
        in.sizes.count
    };
    auto docs_umem = yokan::UserMem{
        in.docs.data,
        in.docs.size
    };
    auto ids_umem = yokan::BasicUserMem<yk_id_t>{
        in.ids.ids,
        in.ids.count
    };

    out.ret = static_cast<yk_return_t>(
        database->docUpdate(in.coll_name, in.mode, ids_umem, docs_umem, sizes_umem));
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_update_direct_ult)

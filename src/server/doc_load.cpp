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

void yk_doc_load_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_load_in_t in;
    doc_load_out_t out;
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

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    yk_buffer_t buffer = provider->bulk_cache.get(
        provider->bulk_cache_data, in.size, HG_BULK_READWRITE);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, buffer));

    size_t count = in.ids.count;
    size_t docs_offset = count*sizeof(size_t);

    if(!in.packed) {
        /* transfer available sizes for each document */
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
            in.bulk, in.offset, buffer->bulk, 0, docs_offset);
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }

    yokan::BasicUserMem<yk_id_t> ids{ in.ids.ids, in.ids.count };
    auto sizes_umem = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(buffer->data),
        count
    };
    auto docs_umem = yokan::UserMem{
        buffer->data + docs_offset,
        in.size - docs_offset
    };

    out.ret = static_cast<yk_return_t>(
        database->docLoad(in.coll_name, in.mode, in.packed, ids, docs_umem, sizes_umem));

    if(out.ret == YOKAN_SUCCESS) {
        margo_request req = MARGO_REQUEST_NULL;
        if(docs_umem.size != 0) {
            // transfer docs
            hret = margo_bulk_itransfer(mid, HG_BULK_PUSH, origin_addr,
                    in.bulk, in.offset + docs_offset,
                    buffer->bulk, docs_offset, in.size - docs_offset, &req);
            CHECK_HRET_OUT(hret, margo_bulk_itransfer);
        }
        // transfer doc sizes
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                in.bulk, in.offset,
                buffer->bulk, 0, count*sizeof(size_t));
        CHECK_HRET_OUT(hret, margo_bulk_transfer);

        if(req != MARGO_REQUEST_NULL) {
            hret = margo_wait(req);
            CHECK_HRET_OUT(hret, margo_wait);
        }
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_load_ult)

void yk_doc_load_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_load_direct_in_t in;
    doc_load_direct_out_t out;

    in.ids.ids = nullptr;
    in.ids.count = 0;

    out.ret = YOKAN_SUCCESS;
    out.sizes.sizes = nullptr;
    out.sizes.count = 0;
    out.docs.data = nullptr;
    out.docs.size = 0;

    std::vector<size_t> doc_sizes;
    std::vector<char> doc_data;

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

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    size_t count = in.ids.count;
    doc_sizes.resize(count);
    doc_data.resize(in.bufsize);

    yokan::BasicUserMem<yk_id_t> ids{ in.ids.ids, in.ids.count };
    auto sizes_umem = yokan::BasicUserMem<size_t>{
        doc_sizes.data(), count };
    auto docs_umem = yokan::UserMem{
        doc_data.data(), doc_data.size()
    };

    out.ret = static_cast<yk_return_t>(
        database->docLoad(in.coll_name, in.mode, true, ids, docs_umem, sizes_umem));

    if(out.ret == YOKAN_SUCCESS) {
        out.sizes.sizes = doc_sizes.data();
        out.sizes.count = count;
        out.docs.data   = doc_data.data();
        out.docs.size   = docs_umem.size;
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_load_direct_ult)

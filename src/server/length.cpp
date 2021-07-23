/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/logging.h"
#include "../common/checks.h"
#include <numeric>

void rkv_length_ult(hg_handle_t h)
{
    hg_return_t hret;
    length_in_t in;
    length_out_t out;
    hg_addr_t origin_addr = HG_ADDR_NULL;

    out.ret = RKV_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);
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

    rkv_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    rkv_buffer_t buffer = provider->bulk_cache.get(
        provider->bulk_cache_data, in.size, HG_BULK_READWRITE);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, buffer));

    const size_t keys_offset = in.count * sizeof(size_t);
    size_t vsizes_offset     = 0; // computed later

    // transfer ksizes
    size_t sizes_to_transfer = in.count*sizeof(size_t);

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
            in.bulk, in.offset, buffer->bulk, 0, sizes_to_transfer);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    // build buffer wrappers for key sizes
    auto ptr = buffer->data;
    auto ksizes = rkv::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr),
        in.count
    };

    auto total_ksize = std::accumulate(ksizes.data, ksizes.data + in.count, (size_t)0);
    vsizes_offset = keys_offset + total_ksize;

    // check that there is no key of size 0
    auto min_key_size = std::accumulate(ksizes.data, ksizes.data + in.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = RKV_ERR_INVALID_ARGS;
        return;
    }

    // check that the sizes found is consistent with in.size
    if(in.size < vsizes_offset + in.count*sizeof(size_t)) {
        out.ret = RKV_ERR_INVALID_ARGS;
        return;
    }

    // transfer the actual keys from the client
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
            in.bulk, in.offset + keys_offset,
            buffer->bulk, keys_offset, total_ksize);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    // create memory wrapper for keys
    auto keys = rkv::UserMem{ ptr + keys_offset, total_ksize };

    // create memory wrapper for value sizes
    auto vsizes = rkv::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr + vsizes_offset),
        in.count
    };

    out.ret = static_cast<rkv_return_t>(
            database->length(in.mode, keys, ksizes, vsizes));

    if(out.ret == RKV_SUCCESS) {
        // transfer the vsizes back the client
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                in.bulk, in.offset + vsizes_offset,
                buffer->bulk, vsizes_offset, in.count*sizeof(size_t));
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }
}
DEFINE_MARGO_RPC_HANDLER(rkv_length_ult)

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

void yk_erase_ult(hg_handle_t h)
{
    hg_return_t hret;
    erase_in_t in;
    erase_out_t out;
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

    yk_database* database = provider->db;
    CHECK_DATABASE(database);
    CHECK_MODE_SUPPORTED(database, in.mode);

    yk_buffer_t buffer = provider->bulk_cache.get(
            provider->bulk_cache_data, in.size, HG_BULK_WRITE_ONLY);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(
            provider->bulk_cache_data, buffer));

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                               in.bulk, in.offset, buffer->bulk, 0, in.size);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    auto ptr = buffer->data;
    auto ksizes = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr),
        in.count
    };
    ptr += in.count*sizeof(size_t);

    auto total_ksize = std::accumulate(ksizes.data, ksizes.data + in.count, (size_t)0);
    auto min_key_size = std::accumulate(ksizes.data, ksizes.data + in.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    if(in.size < in.count*sizeof(size_t) + total_ksize) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    auto keys = yokan::UserMem{ ptr, total_ksize };

    out.ret = static_cast<yk_return_t>(
            database->erase(in.mode, keys, ksizes));
}
DEFINE_MARGO_RPC_HANDLER(yk_erase_ult)

void yk_erase_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    erase_direct_in_t in;
    erase_direct_out_t out;

    in.ksizes.sizes = nullptr;
    in.ksizes.count = 0;
    in.keys.data = nullptr;
    in.keys.size = 0;
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
    CHECK_DATABASE(database);
    CHECK_MODE_SUPPORTED(database, in.mode);

    auto ksizes = yokan::BasicUserMem<size_t>{
        in.ksizes.sizes,
        in.ksizes.count
    };

    auto min_key_size = std::accumulate(in.ksizes.sizes, in.ksizes.sizes + in.ksizes.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    auto keys = yokan::UserMem{ in.keys.data, in.keys.size };

    out.ret = static_cast<yk_return_t>(
            database->erase(in.mode, keys, ksizes));
}
DEFINE_MARGO_RPC_HANDLER(yk_erase_direct_ult)

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

void yk_get_ult(hg_handle_t h)
{
    hg_return_t hret;
    get_in_t in;
    get_out_t out;
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
        provider->bulk_cache_data, in.size, HG_BULK_READWRITE);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, buffer));

    const size_t ksizes_offset = 0;
    const size_t vsizes_offset = in.count*sizeof(size_t);
    const size_t keys_offset   = vsizes_offset * 2;
    size_t vals_offset   = 0; // computed later

    // transfer ksizes, and vsizes only if in.packed is false
    size_t sizes_to_transfer = in.count*sizeof(size_t);
    if(!in.packed) sizes_to_transfer *= 2;

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
            in.bulk, in.offset, buffer->bulk, 0, sizes_to_transfer);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    // build buffer wrappers for key sizes
    auto ptr = buffer->data;
    auto ksizes = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr + ksizes_offset),
        in.count
    };
    auto vsizes = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr + vsizes_offset),
        in.count
    };

    auto total_ksize = std::accumulate(ksizes.data, ksizes.data + in.count, (size_t)0);
    vals_offset = keys_offset + total_ksize;

    // check that there is no key of size 0
    auto min_key_size = std::accumulate(ksizes.data, ksizes.data + in.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    // check that the total_ksize found is consistent with in.size
    if(in.size < vals_offset) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    // check that the value sizes also don't exceed in.size, if not in.packed
    // (if in.packed is true, those value sizes are output only)
    if(!in.packed) {
        auto total_vsize = std::accumulate(vsizes.data, vsizes.data + in.count, (size_t)0);
        if(in.size < vals_offset + total_vsize) {
            out.ret = YOKAN_ERR_INVALID_ARGS;
            return;
        }
    }

    // transfer the actual keys from the client
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
            in.bulk, in.offset + keys_offset,
            buffer->bulk, keys_offset, total_ksize);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    // create UserMem wrapper for keys
    auto keys = yokan::UserMem{ ptr + keys_offset, total_ksize };

    // create UserMem wrapper for values
    size_t remaining_vsize = in.size - vals_offset;
    auto vals = yokan::UserMem{ ptr + vals_offset, remaining_vsize };

    out.ret = static_cast<yk_return_t>(
            database->get(in.mode, in.packed, keys, ksizes, vals, vsizes));

    if(out.ret == YOKAN_SUCCESS) {
        // transfer the vsizes and values back the client
        // this is done using two concurrent bulk transfers
        margo_request req = MARGO_REQUEST_NULL;
        if(vals.size != 0) {
            size_t xfer_size = remaining_vsize;
            if(in.count == 1) xfer_size = vsizes[0];
            hret = margo_bulk_itransfer(mid, HG_BULK_PUSH, origin_addr,
                    in.bulk, in.offset + vals_offset,
                    buffer->bulk, vals_offset, xfer_size, &req);
            CHECK_HRET_OUT(hret, margo_bulk_itransfer);
        }

        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                in.bulk, in.offset + vsizes_offset,
                buffer->bulk, vsizes_offset, in.count*sizeof(size_t));
        CHECK_HRET_OUT(hret, margo_bulk_transfer);

        if(req != MARGO_REQUEST_NULL) {
            hret = margo_wait(req);
            CHECK_HRET_OUT(hret, margo_wait);
        }
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_get_ult)

void yk_get_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    get_direct_in_t in;
    get_direct_out_t out;

    std::memset(&in, 0, sizeof(in));
    std::memset(&out, 0, sizeof(out));

    std::vector<char> values;
    std::vector<size_t> vsizes;

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

    auto count = in.ksizes.count;
    auto ksizes_umem = yokan::BasicUserMem<size_t>{
        in.ksizes.sizes, count };
    auto keys_umem = yokan::UserMem{
        in.keys.data, in.keys.size };

    // check that there is no key of size 0
    auto min_key_size = std::accumulate(in.ksizes.sizes, in.ksizes.sizes + in.ksizes.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    values.resize(in.vbufsize);
    vsizes.resize(count, 0);

    auto vsizes_umem = yokan::BasicUserMem<size_t>{
        vsizes.data(), count };
    auto values_umem = yokan::UserMem{
        values.data(), values.size() };

    out.ret = static_cast<yk_return_t>(
            database->get(in.mode, true, keys_umem,
                          ksizes_umem, values_umem, vsizes_umem));

    if(out.ret == YOKAN_SUCCESS) {
        out.vsizes.sizes = vsizes.data();
        out.vsizes.count = count;
        out.vals.data = values.data();
        out.vals.size = values_umem.size;
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_get_direct_ult)

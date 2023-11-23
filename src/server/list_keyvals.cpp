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

void yk_list_keyvals_ult(hg_handle_t h)
{
    hg_return_t hret;
    list_keyvals_in_t in;
    list_keyvals_out_t out;
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
    CHECK_MODE_SUPPORTED(database, in.mode);

    size_t buffer_size = in.from_ksize + in.filter_size
                       + 2*in.count*sizeof(size_t)
                       + in.keys_buf_size
                       + in.vals_buf_size;

    yk_buffer_t buffer = provider->bulk_cache.get(
        provider->bulk_cache_data, buffer_size, HG_BULK_READWRITE);
    CHECK_BUFFER(buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, buffer));

    const size_t ksizes_offset = in.from_ksize + in.filter_size;
    const size_t vsizes_offset = ksizes_offset + in.count*sizeof(size_t);
    const size_t keys_offset   = vsizes_offset + in.count*sizeof(size_t);
    const size_t vals_offset   = keys_offset + in.keys_buf_size;

    size_t size_to_transfer = in.from_ksize + in.filter_size;
    // transfer ksizes and vsizes only if in.packed is false
    if(!in.packed) size_to_transfer += 2*in.count*sizeof(size_t);

    if(size_to_transfer > 0) {
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                in.bulk, in.offset, buffer->bulk, 0, size_to_transfer);
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }

    // build buffer wrappers
    auto ptr         = buffer->data;
    auto from_key    = yokan::UserMem{ ptr, in.from_ksize };
    auto filter_umem = yokan::UserMem{ ptr + in.from_ksize, in.filter_size };
    auto filter      = yokan::FilterFactory::makeKeyValueFilter(mid, in.mode, filter_umem);
    auto ksizes      = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr + ksizes_offset),
        in.count
    };
    auto vsizes   = yokan::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr + vsizes_offset),
        in.count
    };
    auto keys = yokan::UserMem{ ptr + keys_offset, in.keys_buf_size };
    auto vals = yokan::UserMem{ ptr + vals_offset, in.vals_buf_size };

    if(!filter) {
        out.ret = YOKAN_ERR_INVALID_FILTER;
        return;
    }

    out.ret = static_cast<yk_return_t>(
            database->listKeyValues(
                in.mode, in.packed,
                from_key, filter,
                keys, ksizes, vals, vsizes));

    if(out.ret == YOKAN_SUCCESS) {
        size_to_transfer = 2*in.count*sizeof(size_t)
                         + in.keys_buf_size + in.vals_buf_size;
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                in.bulk, in.offset + ksizes_offset,
                buffer->bulk, ksizes_offset, size_to_transfer);
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_list_keyvals_ult)

void yk_list_keyvals_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    list_keyvals_direct_in_t in;
    list_keyvals_direct_out_t out;

    std::vector<size_t> ksizes;
    std::vector<char> keys;
    std::vector<size_t> vsizes;
    std::vector<char> vals;

    std::memset(&in, 0, sizeof(in));
    std::memset(&out, 0, sizeof(out));

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

    ksizes.resize(in.count);
    keys.resize(in.keys_buf_size);
    vsizes.resize(in.count);
    vals.resize(in.vals_buf_size);

    // build buffer wrappers
    auto from_key    = yokan::UserMem{ in.from_key.data, in.from_key.size };
    auto filter_umem = yokan::UserMem{ in.filter.data, in.filter.size };
    auto filter      = yokan::FilterFactory::makeKeyValueFilter(mid, in.mode, filter_umem);
    auto keys_umem   = yokan::UserMem{keys};
    auto ksizes_umem = yokan::BasicUserMem<size_t>{ksizes};
    auto vals_umem   = yokan::UserMem{vals};
    auto vsizes_umem = yokan::BasicUserMem<size_t>{vsizes};

    if(!filter) {
        out.ret = YOKAN_ERR_INVALID_FILTER;
        return;
    }

    out.ret = static_cast<yk_return_t>(
            database->listKeyValues(in.mode, true, from_key, filter,
                keys_umem, ksizes_umem, vals_umem, vsizes_umem));

    if(out.ret == YOKAN_SUCCESS) {
        out.ksizes.sizes = ksizes.data();
        out.ksizes.count = in.count;
        out.keys.data    = keys.data();
        out.keys.size    = keys_umem.size;
        out.vsizes.sizes = vsizes.data();
        out.vsizes.count = in.count;
        out.vals.data    = vals.data();
        out.vals.size    = vals_umem.size;
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_list_keyvals_direct_ult)

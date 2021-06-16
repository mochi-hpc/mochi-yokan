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
#include <iostream>
#include <numeric>

void rkv_list_keys_ult(hg_handle_t h)
{
    hg_return_t hret;
    list_keys_in_t in;
    list_keys_out_t out;
    hg_bulk_t local_bulk = HG_BULK_NULL;
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

    size_t buffer_size = in.from_ksize + in.prefix_size
                       + in.count*sizeof(size_t)
                       + in.keys_buf_size;

    std::vector<char> buffer(buffer_size);
    void* segptrs[1] = { buffer.data() };
    hg_size_t segsizes[1] = { buffer_size };

    hret = margo_bulk_create(mid, 1, segptrs, segsizes,
                             HG_BULK_READWRITE, &local_bulk);
    CHECK_HRET_OUT(hret, margo_bulk_create);
    DEFER(margo_bulk_free(local_bulk));

    const size_t ksizes_offset = in.from_ksize + in.prefix_size;
    const size_t keys_offset   = ksizes_offset + in.count*sizeof(size_t);;

    // transfer ksizes only if in.packed is false
    size_t size_to_transfer = in.from_ksize + in.prefix_size;
    if(!in.packed) size_to_transfer += in.count*sizeof(size_t);

    if(size_to_transfer > 0) {
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                in.bulk, in.offset, local_bulk, 0, size_to_transfer);
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }

    // build buffer wrappers
    auto ptr      = buffer.data();
    auto from_key = rkv::UserMem{ ptr, in.from_ksize };
    auto prefix   = rkv::UserMem{ ptr + in.from_ksize, in.prefix_size };
    auto ksizes   = rkv::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr + ksizes_offset),
        in.count
    };
    auto keys = rkv::UserMem{ ptr + keys_offset, in.keys_buf_size };

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

    out.ret = static_cast<rkv_return_t>(
            database->listKeys(in.packed, from_key, in.inclusive, prefix, keys, ksizes));

    if(out.ret == RKV_SUCCESS) {
        size_to_transfer = in.count*sizeof(size_t)
                         + keys.size;
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                in.bulk, in.offset + ksizes_offset,
                local_bulk, ksizes_offset, size_to_transfer);
        CHECK_HRET_OUT(hret, margo_bulk_transfer);
    }
}
DEFINE_MARGO_RPC_HANDLER(rkv_list_keys_ult)

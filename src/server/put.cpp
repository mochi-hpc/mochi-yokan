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

void rkv_put_ult(hg_handle_t h)
{
    hg_return_t hret;
    put_in_t in;
    put_out_t out;
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

    std::vector<char> buffer(in.size);
    void* segptrs[1] = { buffer.data() };
    hg_size_t segsizes[1] = { in.size };


    hret = margo_bulk_create(mid, 1, segptrs, segsizes,
                             HG_BULK_WRITE_ONLY, &local_bulk);
    CHECK_HRET_OUT(hret, margo_bulk_create);
    DEFER(margo_bulk_free(local_bulk));

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                               in.bulk, in.offset, local_bulk, 0, in.size);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    auto ptr = buffer.data();
    auto ksizes = rkv::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr),
        in.count
    };
    ptr += in.count*sizeof(size_t);

    auto vsizes = rkv::BasicUserMem<size_t>{
        reinterpret_cast<size_t*>(ptr),
        in.count
    };
    ptr += in.count*sizeof(size_t);

    auto total_ksize = std::accumulate(ksizes.data, ksizes.data + in.count, (size_t)0);
    auto total_vsize = std::accumulate(vsizes.data, vsizes.data + in.count, (size_t)0);
    auto min_key_size = std::accumulate(ksizes.data, ksizes.data + in.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = RKV_ERR_INVALID_ARGS;
        return;
    }

    if(in.size < 2*in.count*sizeof(size_t) + total_ksize + total_vsize) {
        out.ret = RKV_ERR_INVALID_ARGS;
        return;
    }

    auto keys = rkv::UserMem{ ptr, total_ksize };
    ptr += total_ksize;

    auto vals = rkv::UserMem{ ptr, total_vsize };

    out.ret = static_cast<rkv_return_t>(
            database->putPacked(keys, ksizes, vals, vsizes));
}
DEFINE_MARGO_RPC_HANDLER(rkv_put_ult)

/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/logging.h"
#include "../common/checks.h"

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
    DEFER(margo_addr_free(origin_addr));

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

    size_t* sizes = reinterpret_cast<size_t*>(buffer.data());
    std::vector<rkv::UserMem> keys(in.count);
    size_t current_offset = in.count*sizeof(size_t);
    for(size_t i = 0; i < in.count; i++) {
        keys[i].data = buffer.data() + current_offset;
        keys[i].size = sizes[i];
        current_offset += sizes[i];
    }
    std::vector<rkv::UserMem> values(in.count);
    size_t j = in.count;
    for(size_t i = 0; i < in.count; i++, j++) {
        values[i].data = buffer.data() + current_offset;
        values[i].size = sizes[j];
        current_offset += sizes[j];
    }

    if(in.count == 1) {
        database->put(keys[0], values[0]);
    } else if(in.count > 1) {
        database->putMulti(keys, values);
    }
}
DEFINE_MARGO_RPC_HANDLER(rkv_put_ult)

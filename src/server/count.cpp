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

void rkv_count_ult(hg_handle_t h)
{
    hg_return_t hret;
    count_in_t in;
    count_out_t out;

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

    rkv_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    out.ret = static_cast<rkv_return_t>(
            database->count(in.mode, &out.count));
}
DEFINE_MARGO_RPC_HANDLER(rkv_count_ult)

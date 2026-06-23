/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/logging.h"
#include "../common/checks.h"

void yk_erase_range_ult(hg_handle_t h)
{
    hg_return_t hret;
    erase_range_in_t in;
    erase_range_out_t out;

    in.prefix.data = nullptr;
    in.prefix.size = 0;
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
    const double timeout_ms = in.timeout_ms;
    (void)timeout_ms;
    DEFER(margo_free_input(h, &in));

    yk_database* database = provider->db;
    CHECK_DATABASE(database);
    CHECK_MODE_SUPPORTED(database, in.mode);

    auto prefix = yokan::UserMem{ in.prefix.data, in.prefix.size };

    out.ret = static_cast<yk_return_t>(
            database->eraseRange(in.mode, prefix));
}
DEFINE_MARGO_RPC_HANDLER(yk_erase_range_ult)

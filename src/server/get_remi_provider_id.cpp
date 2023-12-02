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
#ifdef YOKAN_HAS_REMI
#include <remi/remi-server.h>
#endif

void yk_get_remi_provider_id_ult(hg_handle_t h)
{
    get_remi_provider_id_out_t out = {YOKAN_SUCCESS, 0};

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    yk_provider_t provider = (yk_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

#ifdef YOKAN_HAS_REMI
    if(provider->remi.provider) {
        int ret = remi_provider_get_provider_id(
                provider->remi.provider,
                &out.provider_id);
        if(ret != REMI_SUCCESS) {
            out.ret = YOKAN_ERR_FROM_REMI;
        }
    } else {
        out.ret = YOKAN_ERR_OP_UNSUPPORTED;
    }
#else
    out.ret = YOKAN_ERR_OP_UNSUPPORTED;
#endif
}
DEFINE_MARGO_RPC_HANDLER(yk_get_remi_provider_id_ult)

/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <utility>
#include "client.hpp"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"
#include "../common/extras.h"

extern "C" yk_return_t yk_erase_range(yk_database_handle_t dbh,
                                      int32_t mode,
                                      const void* prefix,
                                      size_t prefix_size, ...)
{
    YK_EXTRACT_EXTRAS(extras, mode, prefix_size);

    if(prefix_size != 0 && prefix == nullptr)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    erase_range_in_t in;
    erase_range_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.mode         = mode;
    in.timeout_ms   = extras.timeout_ms;
    in.prefix.data  = (char*)prefix;
    in.prefix.size  = prefix_size;

    hret = margo_create(mid, dbh->addr, dbh->client->erase_range_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward_timed(dbh->provider_id, handle, &in, extras.timeout_ms);
    CHECK_HRET(hret, margo_provider_forward_timed);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

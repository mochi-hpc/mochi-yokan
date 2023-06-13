/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include "client.hpp"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

extern "C" yk_return_t yk_collection_last_id(yk_database_handle_t dbh,
                                             const char* name,
                                             int32_t mode,
                                             yk_id_t* id) {
    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    coll_last_id_in_t in;
    coll_last_id_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id     = dbh->database_id;
    in.mode      = mode;
    in.coll_name = (char*)name;

    hret = margo_create(mid, dbh->addr, dbh->client->coll_last_id_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);
    if(ret == YOKAN_SUCCESS && id)
        *id = out.last_id;

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

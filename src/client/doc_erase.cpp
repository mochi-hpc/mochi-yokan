/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include "client.h"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

extern "C" yk_return_t yk_doc_erase_multi(
        yk_database_handle_t dbh,
        const char* collection,
        int32_t mode,
        size_t count,
        const yk_id_t* ids) {

    if(count == 0)
        return YOKAN_SUCCESS;
    if(ids == nullptr)
        return YOKAN_ERR_INVALID_ARGS;
    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_erase_in_t in;
    doc_erase_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    in.db_id     = dbh->database_id;
    in.mode      = mode;
    in.coll_name = (char*)collection;
    in.ids.count = count;
    in.ids.ids   = (yk_id_t*)ids;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_erase_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" yk_return_t yk_doc_erase(yk_database_handle_t dbh,
                                    const char* name,
                                    int32_t mode,
                                    yk_id_t id) {
    return yk_doc_erase_multi(dbh, name, mode, 1, &id);
}

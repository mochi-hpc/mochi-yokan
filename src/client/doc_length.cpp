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

extern "C" yk_return_t yk_doc_length_multi(yk_database_handle_t dbh,
                                         const char* collection,
                                         int32_t mode,
                                         size_t count,
                                         const yk_id_t* ids,
                                         size_t* rsizes) {
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!ids || !rsizes)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;
    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_length_in_t in;
    doc_length_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    out.sizes.sizes = rsizes;
    out.sizes.count = count;

    in.mode      = mode;
    in.coll_name = (char*)collection;
    in.ids.count = count;
    in.ids.ids   = (yk_id_t*)ids;

    hret = margo_create(mid, dbh->addr, dbh->client->doc_length_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);

    out.sizes.sizes = nullptr;
    out.sizes.count = 0;

    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

extern "C" yk_return_t yk_doc_length(yk_database_handle_t dbh,
                                   const char* collection,
                                   int32_t mode,
                                   yk_id_t id,
                                   size_t* size) {
    if(size == nullptr) return YOKAN_ERR_INVALID_ARGS;
    auto ret = yk_doc_length_multi(dbh, collection, mode, 1, &id, size);
    if(ret == YOKAN_SUCCESS && *size == YOKAN_KEY_NOT_FOUND)
        return YOKAN_ERR_KEY_NOT_FOUND;
    return ret;
}

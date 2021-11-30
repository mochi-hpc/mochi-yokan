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

void yk_doc_length_ult(hg_handle_t h)
{
    std::vector<size_t> sizes;
    hg_return_t hret;
    doc_length_in_t in;
    doc_length_out_t out;

    in.ids.ids = nullptr;
    in.ids.count = 0;

    out.ret = YOKAN_SUCCESS;
    out.sizes.sizes = NULL;
    out.sizes.count = 0;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    yk_provider_t provider = (yk_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    yokan::BasicUserMem<yk_id_t> ids{ in.ids.ids, in.ids.count };
    sizes.resize(in.ids.count);
    yokan::BasicUserMem<size_t> sizes_umem{sizes};
    out.ret = static_cast<yk_return_t>(
        database->docSize(in.coll_name, in.mode, ids, sizes_umem));
    if(out.ret == YOKAN_SUCCESS) {
        out.sizes.sizes = sizes.data();
        out.sizes.count = sizes.size();
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_length_ult)

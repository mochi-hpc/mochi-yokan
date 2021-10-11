/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "../common/types.h"
#include "admin.h"
#include "yokan/admin.h"

yk_return_t yk_admin_init(margo_instance_id mid, yk_admin_t* admin)
{
    yk_admin_t a = (yk_admin_t)calloc(1, sizeof(*a));
    if(!a) return YOKAN_ERR_ALLOCATION;

    a->mid = mid;

    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(mid, "yk_open_database", &id, &flag);

    if(flag == HG_TRUE) {
        margo_registered_name(mid, "yk_open_database", &a->open_database_id, &flag);
        margo_registered_name(mid, "yk_close_database", &a->close_database_id, &flag);
        margo_registered_name(mid, "yk_destroy_database", &a->destroy_database_id, &flag);
        margo_registered_name(mid, "yk_list_databases", &a->list_databases_id, &flag);
        /* Get more existing RPCs... */
    } else {
        a->open_database_id =
            MARGO_REGISTER(mid, "yk_open_database",
            open_database_in_t, open_database_out_t, NULL);
        a->close_database_id =
            MARGO_REGISTER(mid, "yk_close_database",
            close_database_in_t, close_database_out_t, NULL);
        a->destroy_database_id =
            MARGO_REGISTER(mid, "yk_destroy_database",
            destroy_database_in_t, destroy_database_out_t, NULL);
        a->list_databases_id =
            MARGO_REGISTER(mid, "yk_list_databases",
            list_databases_in_t, list_databases_out_t, NULL);
        /* Register more RPCs ... */
    }

    *admin = a;
    return YOKAN_SUCCESS;
}

yk_return_t yk_admin_finalize(yk_admin_t admin)
{
    free(admin);
    return YOKAN_SUCCESS;
}

yk_return_t yk_open_database(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        const char* type,
        const char* config,
        yk_database_id_t* id)
{
    hg_handle_t h;
    open_database_in_t  in;
    open_database_out_t out;
    hg_return_t hret;
    yk_return_t ret;

    in.type   = (char*)type;
    in.config = (char*)config;
    in.token  = (char*)token;

    hret = margo_create(admin->mid, address, admin->open_database_id, &h);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_provider_forward(provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    ret = out.ret;

    if(ret != YOKAN_SUCCESS) {
        margo_free_output(h, &out);
        margo_destroy(h);
        return ret;
    }

    memcpy(id, &out.id, sizeof(*id));

    margo_free_output(h, &out);
    margo_destroy(h);
    return YOKAN_SUCCESS;
}

yk_return_t yk_close_database(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        yk_database_id_t id)
{
    hg_handle_t h;
    close_database_in_t  in;
    close_database_out_t out;
    hg_return_t hret;
    yk_return_t ret;

    memcpy(&in.id, &id, sizeof(id));
    in.token  = (char*)token;

    hret = margo_create(admin->mid, address, admin->close_database_id, &h);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_provider_forward(provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    ret = out.ret;

    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

yk_return_t yk_destroy_database(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        yk_database_id_t id)
{
    hg_handle_t h;
    destroy_database_in_t  in;
    destroy_database_out_t out;
    hg_return_t hret;
    yk_return_t ret;

    memcpy(&in.id, &id, sizeof(id));
    in.token  = (char*)token;

    hret = margo_create(admin->mid, address, admin->destroy_database_id, &h);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_provider_forward(provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    ret = out.ret;

    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

yk_return_t yk_list_databases(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        yk_database_id_t* ids,
        size_t* count)
{
    hg_handle_t h;
    list_databases_in_t  in;
    list_databases_out_t out;
    yk_return_t ret;
    hg_return_t hret;

    in.token  = (char*)token;
    in.max_ids = *count;

    hret = margo_create(admin->mid, address, admin->list_databases_id, &h);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_provider_forward(provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        margo_destroy(h);
        return YOKAN_ERR_FROM_MERCURY;
        // LCOV_EXCL_STOP
    }

    ret = out.ret;
    if(ret == YOKAN_SUCCESS) {
        *count = out.count;
        memcpy(ids, out.ids, out.count*sizeof(*ids));
    }

    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

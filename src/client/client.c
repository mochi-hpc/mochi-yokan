/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "../common/types.h"
#include "client.h"
#include "yokan/client.h"
#include <stdio.h>

yk_return_t yk_client_init(margo_instance_id mid, yk_client_t* client)
{
    yk_client_t c = (yk_client_t)calloc(1, sizeof(*c));
    if(!c) return YOKAN_ERR_ALLOCATION;

    c->mid = mid;

    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(mid, "yk_exists", &id, &flag);

    if(flag == HG_TRUE) {

        margo_registered_name(mid, "yk_count",        &c->count_id,        &flag);
        margo_registered_name(mid, "yk_exists",       &c->exists_id,       &flag);
        margo_registered_name(mid, "yk_length",       &c->length_id,       &flag);
        margo_registered_name(mid, "yk_put",          &c->put_id,          &flag);
        margo_registered_name(mid, "yk_get",          &c->get_id,          &flag);
        margo_registered_name(mid, "yk_erase",        &c->erase_id,        &flag);
        margo_registered_name(mid, "yk_list_keys",    &c->list_keys_id,    &flag);
        margo_registered_name(mid, "yk_list_keyvals", &c->list_keyvals_id, &flag);

        margo_registered_name(mid, "yk_coll_create",  &c->coll_create_id,  &flag);
        margo_registered_name(mid, "yk_coll_drop",    &c->coll_drop_id,    &flag);
        margo_registered_name(mid, "yk_coll_exists",  &c->coll_exists_id,  &flag);
        margo_registered_name(mid, "yk_coll_erase",   &c->coll_erase_id,   &flag);
        margo_registered_name(mid, "yk_coll_last_id", &c->coll_last_id_id, &flag);
        margo_registered_name(mid, "yk_coll_size",    &c->coll_size_id,    &flag);
        margo_registered_name(mid, "yk_coll_load",    &c->coll_load_id,    &flag);
        margo_registered_name(mid, "yk_coll_store",   &c->coll_store_id,   &flag);
        margo_registered_name(mid, "yk_coll_update",  &c->coll_update_id,  &flag);

    } else {

        c->count_id =
            MARGO_REGISTER(mid, "yk_count",
                           count_in_t, count_out_t, NULL);
        c->exists_id =
            MARGO_REGISTER(mid, "yk_exists",
                           exists_in_t, exists_out_t, NULL);
        c->length_id =
            MARGO_REGISTER(mid, "yk_length",
                           length_in_t, length_out_t, NULL);
        c->put_id =
            MARGO_REGISTER(mid, "yk_put",
                           put_in_t, put_out_t, NULL);
        c->get_id =
            MARGO_REGISTER(mid, "yk_get",
                           get_in_t, get_out_t, NULL);
        c->erase_id =
            MARGO_REGISTER(mid, "yk_erase",
                           erase_in_t, erase_out_t, NULL);
        c->list_keys_id =
            MARGO_REGISTER(mid, "yk_list_keys",
                           list_keys_in_t, list_keys_out_t, NULL);
        c->list_keyvals_id =
            MARGO_REGISTER(mid, "yk_list_keyvals",
                           list_keyvals_in_t, list_keyvals_out_t, NULL);

        c->coll_create_id =
            MARGO_REGISTER(mid, "yk_coll_create",
                           coll_create_in_t, coll_create_out_t, NULL);
        c->coll_drop_id =
            MARGO_REGISTER(mid, "yk_coll_drop",
                           coll_drop_in_t, coll_drop_out_t, NULL);
        c->coll_exists_id =
            MARGO_REGISTER(mid, "yk_coll_exists",
                           coll_exists_in_t, coll_exists_out_t, NULL);
        c->coll_erase_id =
            MARGO_REGISTER(mid, "yk_coll_erase",
                           coll_erase_in_t, coll_erase_out_t, NULL);
        c->coll_last_id_id =
            MARGO_REGISTER(mid, "yk_coll_last_id",
                           coll_last_id_in_t, coll_last_id_out_t, NULL);
        c->coll_size_id =
            MARGO_REGISTER(mid, "yk_coll_size",
                           coll_size_in_t, coll_size_out_t, NULL);
        c->coll_load_id =
            MARGO_REGISTER(mid, "yk_coll_load",
                           coll_load_in_t, coll_load_out_t, NULL);
        c->coll_store_id =
            MARGO_REGISTER(mid, "yk_coll_store",
                           coll_store_in_t, coll_store_out_t, NULL);
        c->coll_update_id =
            MARGO_REGISTER(mid, "yk_coll_update",
                           coll_update_in_t, coll_update_out_t, NULL);
    }

    *client = c;
    return YOKAN_SUCCESS;
}

yk_return_t yk_client_finalize(yk_client_t client)
{
    if(client->num_database_handles != 0) {
        // LCOV_EXCL_START
        margo_warning(client->mid,
            "Warning: %ld database handles not released when yk_client_finalize was called",
            client->num_database_handles);
        // LCOV_EXCL_STOP
    }
    free(client);
    return YOKAN_SUCCESS;
}

yk_return_t yk_database_handle_create(
        yk_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        yk_database_id_t database_id,
        yk_database_handle_t* handle)
{
    if(client == YOKAN_CLIENT_NULL)
        return YOKAN_ERR_INVALID_ARGS;

    yk_database_handle_t rh =
        (yk_database_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return YOKAN_ERR_ALLOCATION;

    hg_return_t ret = margo_addr_dup(client->mid, addr, &(rh->addr));
    if(ret != HG_SUCCESS) {
        free(rh);
        return YOKAN_ERR_FROM_MERCURY;
    }

    rh->client      = client;
    rh->provider_id = provider_id;
    rh->database_id = database_id;
    rh->refcount    = 1;

    client->num_database_handles += 1;

    *handle = rh;
    return YOKAN_SUCCESS;
}

yk_return_t yk_database_handle_ref_incr(
        yk_database_handle_t handle)
{
    if(handle == YOKAN_DATABASE_HANDLE_NULL)
        return YOKAN_ERR_INVALID_ARGS;
    handle->refcount += 1;
    return YOKAN_SUCCESS;
}

yk_return_t yk_database_handle_release(yk_database_handle_t handle)
{
    if(handle == YOKAN_DATABASE_HANDLE_NULL)
        return YOKAN_ERR_INVALID_ARGS;
    handle->refcount -= 1;
    if(handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        handle->client->num_database_handles -= 1;
        free(handle);
    }
    return YOKAN_SUCCESS;
}

/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "../common/types.h"
#include "client.hpp"
#include "yokan/client.h"
#include <stdio.h>

extern "C" yk_return_t yk_client_init(margo_instance_id mid, yk_client_t* client)
{
    yk_client_t c = (yk_client_t)calloc(1, sizeof(*c));
    if(!c) return YOKAN_ERR_ALLOCATION;

    c->mid = mid;

    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(mid, "yk_exists", &id, &flag);

    if(flag == HG_TRUE) {

        margo_registered_name(mid, "yk_count",               &c->count_id,               &flag);
        margo_registered_name(mid, "yk_exists",              &c->exists_id,              &flag);
        margo_registered_name(mid, "yk_exists_direct",       &c->exists_direct_id,       &flag);
        margo_registered_name(mid, "yk_length",              &c->length_id,              &flag);
        margo_registered_name(mid, "yk_length_direct",       &c->length_direct_id,       &flag);
        margo_registered_name(mid, "yk_put",                 &c->put_id,                 &flag);
        margo_registered_name(mid, "yk_put_direct",          &c->put_direct_id,          &flag);
        margo_registered_name(mid, "yk_get",                 &c->get_id,                 &flag);
        margo_registered_name(mid, "yk_get_direct",          &c->get_direct_id,          &flag);
        margo_registered_name(mid, "yk_fetch",               &c->fetch_id,               &flag);
        margo_registered_name(mid, "yk_fetch_direct",        &c->fetch_direct_id,        &flag);
        margo_registered_name(mid, "yk_erase",               &c->erase_id,               &flag);
        margo_registered_name(mid, "yk_erase_direct",        &c->erase_direct_id,        &flag);
        margo_registered_name(mid, "yk_list_keys",           &c->list_keys_id,           &flag);
        margo_registered_name(mid, "yk_list_keys_direct",    &c->list_keys_direct_id,    &flag);
        margo_registered_name(mid, "yk_list_keyvals",        &c->list_keyvals_id,        &flag);
        margo_registered_name(mid, "yk_list_keyvals_direct", &c->list_keyvals_direct_id, &flag);
        margo_registered_name(mid, "yk_iter",                &c->iter_id,                &flag);
        margo_registered_name(mid, "yk_iter_irect",          &c->iter_direct_id,         &flag);

        margo_registered_name(mid, "yk_coll_create",      &c->coll_create_id,      &flag);
        margo_registered_name(mid, "yk_coll_drop",        &c->coll_drop_id,        &flag);
        margo_registered_name(mid, "yk_coll_exists",      &c->coll_exists_id,      &flag);
        margo_registered_name(mid, "yk_coll_last_id",     &c->coll_last_id_id,     &flag);
        margo_registered_name(mid, "yk_coll_size",        &c->coll_size_id,        &flag);
        margo_registered_name(mid, "yk_doc_load",         &c->doc_load_id,         &flag);
        margo_registered_name(mid, "yk_doc_load_direct",  &c->doc_load_direct_id,  &flag);
        margo_registered_name(mid, "yk_doc_fetch",        &c->doc_fetch_id,        &flag);
        margo_registered_name(mid, "yk_doc_erase",        &c->doc_erase_id,        &flag);
        margo_registered_name(mid, "yk_doc_store",        &c->doc_store_id,        &flag);
        margo_registered_name(mid, "yk_doc_store_direct", &c->doc_store_direct_id, &flag);
        margo_registered_name(mid, "yk_doc_update",       &c->doc_update_id,       &flag);
        margo_registered_name(mid, "yk_doc_update_direct",&c->doc_update_direct_id,&flag);
        margo_registered_name(mid, "yk_doc_length",       &c->doc_length_id,       &flag);
        margo_registered_name(mid, "yk_doc_list",         &c->doc_list_id,         &flag);
        margo_registered_name(mid, "yk_doc_list_direct",  &c->doc_list_direct_id,  &flag);
        margo_registered_name(mid, "yk_doc_iter",         &c->doc_iter_id,         &flag);
        margo_registered_name(mid, "yk_doc_iter_direct",  &c->doc_iter_direct_id,  &flag);

    } else {

        c->count_id =
            MARGO_REGISTER(mid, "yk_count",
                           count_in_t, count_out_t, NULL);
        c->exists_id =
            MARGO_REGISTER(mid, "yk_exists",
                           exists_in_t, exists_out_t, NULL);
        c->exists_direct_id =
            MARGO_REGISTER(mid, "yk_exists_direct",
                           exists_direct_in_t, exists_direct_out_t, NULL);
        c->length_id =
            MARGO_REGISTER(mid, "yk_length",
                           length_in_t, length_out_t, NULL);
        c->length_direct_id =
            MARGO_REGISTER(mid, "yk_length_direct",
                           length_direct_in_t, length_direct_out_t, NULL);
        c->put_id =
            MARGO_REGISTER(mid, "yk_put",
                           put_in_t, put_out_t, NULL);
        c->put_direct_id =
            MARGO_REGISTER(mid, "yk_put_direct",
                           put_direct_in_t, put_direct_out_t, NULL);
        c->get_id =
            MARGO_REGISTER(mid, "yk_get",
                           get_in_t, get_out_t, NULL);
        c->get_direct_id =
            MARGO_REGISTER(mid, "yk_get_direct",
                           get_direct_in_t, get_direct_out_t, NULL);
        c->fetch_id =
            MARGO_REGISTER(mid, "yk_fetch",
                           fetch_in_t, fetch_out_t, NULL);
        c->fetch_direct_id =
            MARGO_REGISTER(mid, "yk_fetch_direct",
                           fetch_direct_in_t, fetch_direct_out_t, NULL);
        c->erase_id =
            MARGO_REGISTER(mid, "yk_erase",
                           erase_in_t, erase_out_t, NULL);
        c->erase_direct_id =
            MARGO_REGISTER(mid, "yk_erase_direct",
                           erase_direct_in_t, erase_direct_out_t, NULL);
        c->list_keys_id =
            MARGO_REGISTER(mid, "yk_list_keys",
                           list_keys_in_t, list_keys_out_t, NULL);
        c->list_keys_direct_id =
            MARGO_REGISTER(mid, "yk_list_keys_direct",
                           list_keys_direct_in_t, list_keys_direct_out_t, NULL);
        c->list_keyvals_id =
            MARGO_REGISTER(mid, "yk_list_keyvals",
                           list_keyvals_in_t, list_keyvals_out_t, NULL);
        c->list_keyvals_direct_id =
            MARGO_REGISTER(mid, "yk_list_keyvals_direct",
                           list_keyvals_direct_in_t, list_keyvals_direct_out_t, NULL);
        c->iter_id =
            MARGO_REGISTER(mid, "yk_iter",
                           iter_in_t, iter_out_t, NULL);
        c->iter_direct_id =
            MARGO_REGISTER(mid, "yk_iter_direct",
                           iter_in_t, iter_out_t, NULL);

        c->coll_create_id =
            MARGO_REGISTER(mid, "yk_coll_create",
                           coll_create_in_t, coll_create_out_t, NULL);
        c->coll_drop_id =
            MARGO_REGISTER(mid, "yk_coll_drop",
                           coll_drop_in_t, coll_drop_out_t, NULL);
        c->coll_exists_id =
            MARGO_REGISTER(mid, "yk_coll_exists",
                           coll_exists_in_t, coll_exists_out_t, NULL);
        c->coll_last_id_id =
            MARGO_REGISTER(mid, "yk_coll_last_id",
                           coll_last_id_in_t, coll_last_id_out_t, NULL);
        c->coll_size_id =
            MARGO_REGISTER(mid, "yk_coll_size",
                           coll_size_in_t, coll_size_out_t, NULL);
        c->doc_erase_id =
            MARGO_REGISTER(mid, "yk_doc_erase",
                           doc_erase_in_t, doc_erase_out_t, NULL);
        c->doc_load_id =
            MARGO_REGISTER(mid, "yk_doc_load",
                           doc_load_in_t, doc_load_out_t, NULL);
        c->doc_load_direct_id =
            MARGO_REGISTER(mid, "yk_doc_load_direct",
                           doc_load_direct_in_t, doc_load_direct_out_t, NULL);
        c->doc_fetch_id =
            MARGO_REGISTER(mid, "yk_doc_fetch",
                           doc_fetch_in_t, doc_fetch_out_t, NULL);
        c->doc_store_id =
            MARGO_REGISTER(mid, "yk_doc_store",
                           doc_store_in_t, doc_store_out_t, NULL);
        c->doc_store_direct_id =
            MARGO_REGISTER(mid, "yk_doc_store_direct",
                           doc_store_direct_in_t, doc_store_direct_out_t, NULL);
        c->doc_update_id =
            MARGO_REGISTER(mid, "yk_doc_update",
                           doc_update_in_t, doc_update_out_t, NULL);
        c->doc_update_direct_id =
            MARGO_REGISTER(mid, "yk_doc_update_direct",
                           doc_update_direct_in_t, doc_update_direct_out_t, NULL);
        c->doc_length_id =
            MARGO_REGISTER(mid, "yk_doc_length",
                           doc_length_in_t, doc_length_out_t, NULL);
        c->doc_list_id =
            MARGO_REGISTER(mid, "yk_doc_list",
                           doc_list_in_t, doc_list_out_t, NULL);
        c->doc_list_direct_id =
            MARGO_REGISTER(mid, "yk_doc_list_direct",
                           doc_list_direct_in_t, doc_list_direct_out_t, NULL);
        c->doc_iter_id =
            MARGO_REGISTER(mid, "yk_doc_iter",
                           doc_iter_in_t, doc_iter_out_t, NULL);
        c->doc_iter_direct_id =
            MARGO_REGISTER(mid, "yk_doc_iter_direct",
                           doc_iter_in_t, doc_iter_out_t, NULL);
    }

    // The RPCs bellow should be registered regardless of whether they already were registered

    c->fetch_back_id =
        MARGO_REGISTER(mid, "yk_fetch_back",
                       fetch_back_in_t, fetch_back_out_t, yk_fetch_back_ult);
    c->fetch_direct_back_id =
        MARGO_REGISTER(mid, "yk_fetch_direct_back",
                       fetch_direct_back_in_t, fetch_direct_back_out_t, yk_fetch_direct_back_ult);
    c->iter_back_id =
        MARGO_REGISTER(mid, "yk_iter_back",
                       iter_back_in_t, iter_back_out_t, yk_iter_back_ult);
    c->iter_direct_back_id =
        MARGO_REGISTER(mid, "yk_iter_direct_back",
                       iter_direct_back_in_t, iter_direct_back_out_t, yk_iter_direct_back_ult);
    c->doc_fetch_back_id =
        MARGO_REGISTER(mid, "yk_doc_fetch_back",
                       doc_fetch_back_in_t, doc_fetch_back_out_t, yk_doc_fetch_back_ult);
    c->doc_fetch_direct_back_id =
        MARGO_REGISTER(mid, "yk_doc_fetch_direct_back",
                       doc_fetch_direct_back_in_t, doc_fetch_back_out_t, yk_doc_fetch_direct_back_ult);
    c->doc_iter_back_id =
        MARGO_REGISTER(mid, "yk_doc_iter_back",
                       doc_iter_back_in_t, doc_iter_back_out_t, yk_doc_iter_back_ult);
    c->doc_iter_direct_back_id =
        MARGO_REGISTER(mid, "yk_doc_iter_direct_back",
                       doc_iter_direct_back_in_t, doc_iter_direct_back_out_t, yk_doc_iter_direct_back_ult);

    *client = c;
    return YOKAN_SUCCESS;
}

extern "C" yk_return_t yk_client_finalize(yk_client_t client)
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

extern "C" yk_return_t yk_database_handle_create(
        yk_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        bool check,
        yk_database_handle_t* handle)
{
    if(client == YOKAN_CLIENT_NULL || addr == HG_ADDR_NULL)
        return YOKAN_ERR_INVALID_ARGS;

    hg_return_t ret;

    if(check) {
        char buffer[sizeof("yokan")];
        size_t bufsize = sizeof("yokan");
        ret = margo_provider_get_identity(client->mid, addr, provider_id, buffer, &bufsize);
        if(ret != HG_SUCCESS || strcmp("yokan", buffer) != 0)
            return YOKAN_ERR_INVALID_PROVIDER;
    }

    yk_database_handle_t rh =
        (yk_database_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return YOKAN_ERR_ALLOCATION;

    ret = margo_addr_dup(client->mid, addr, &(rh->addr));
    if(ret != HG_SUCCESS) {
        free(rh);
        return YOKAN_ERR_FROM_MERCURY;
    }

    rh->client      = client;
    rh->provider_id = provider_id;
    rh->refcount    = 1;

    client->num_database_handles += 1;

    *handle = rh;
    return YOKAN_SUCCESS;
}

extern "C" yk_return_t yk_database_handle_get_info(
        yk_database_handle_t handle,
        yk_client_t* client,
        hg_addr_t* addr,
        uint16_t* provider_id)
{
    if(handle == YOKAN_DATABASE_HANDLE_NULL)
        return YOKAN_ERR_INVALID_ARGS;
    if(client) *client = handle->client;
    if(addr) margo_addr_dup(handle->client->mid, handle->addr, addr);
    if(provider_id) *provider_id = handle->provider_id;
    return YOKAN_SUCCESS;
}

extern "C" yk_return_t yk_database_handle_ref_incr(
        yk_database_handle_t handle)
{
    if(handle == YOKAN_DATABASE_HANDLE_NULL)
        return YOKAN_ERR_INVALID_ARGS;
    handle->refcount += 1;
    return YOKAN_SUCCESS;
}

extern "C" yk_return_t yk_database_handle_release(yk_database_handle_t handle)
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

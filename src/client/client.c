/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "../common/types.h"
#include "client.h"
#include "rkv/rkv-client.h"

rkv_return_t rkv_client_init(margo_instance_id mid, rkv_client_t* client)
{
    rkv_client_t c = (rkv_client_t)calloc(1, sizeof(*c));
    if(!c) return RKV_ERR_ALLOCATION;

    c->mid = mid;

    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(mid, "rkv_exists", &id, &flag);

    if(flag == HG_TRUE) {

        margo_registered_name(mid, "rkv_exists",        &c->exists_id,        &flag);
        margo_registered_name(mid, "rkv_exists_multi",  &c->exists_multi_id,  &flag);
        margo_registered_name(mid, "rkv_exists_packed", &c->exists_packed_id, &flag);

        margo_registered_name(mid, "rkv_length",        &c->length_id,        &flag);
        margo_registered_name(mid, "rkv_length_multi",  &c->length_multi_id,  &flag);
        margo_registered_name(mid, "rkv_length_packed", &c->length_packed_id, &flag);

        margo_registered_name(mid, "rkv_put",        &c->put_id,        &flag);

        margo_registered_name(mid, "rkv_get",        &c->get_id,        &flag);
        margo_registered_name(mid, "rkv_get_multi",  &c->get_multi_id,  &flag);
        margo_registered_name(mid, "rkv_get_packed", &c->get_packed_id, &flag);

        margo_registered_name(mid, "rkv_erase",        &c->erase_id,        &flag);

        margo_registered_name(mid, "rkv_list_keys",        &c->list_keys_id,        &flag);
        margo_registered_name(mid, "rkv_list_keys_packed", &c->list_keys_packed_id, &flag);

        margo_registered_name(mid, "rkv_list_keyvals",        &c->list_keyvals_id,        &flag);
        margo_registered_name(mid, "rkv_list_keyvals_packed", &c->list_keyvals_packed_id, &flag);

    } else {

//        c->exists_id = MARGO_REGISTER(mid, "rkv_exists", exists_in_t, exists_out_t, NULL);
//        c->exists_multi_id = MARGO_REGISTER(mid, "rkv_exists_multi", exists_multi_in_t, exists_multi_out_t, NULL);
//        c->exists_packed_id = MARGO_REGISTER(mid, "rkv_exists_packed", exists_packed_in_t, exists_packed_out_t, NULL);

//        c->length_id = MARGO_REGISTER(mid, "rkv_length", length_in_t, length_out_t, NULL);
//        c->length_multi_id = MARGO_REGISTER(mid, "rkv_length_multi", length_multi_in_t, length_multi_out_t, NULL);
//        c->length_packed_id = MARGO_REGISTER(mid, "rkv_length_packed", length_packed_in_t, length_packed_out_t, NULL);

        c->put_id = MARGO_REGISTER(mid, "rkv_put", put_in_t, put_out_t, NULL);

//        c->get_id = MARGO_REGISTER(mid, "rkv_get", get_in_t, get_out_t, NULL);
//        c->get_multi_id = MARGO_REGISTER(mid, "rkv_get_multi", get_multi_in_t, get_multi_out_t, NULL);
//        c->get_packed_id = MARGO_REGISTER(mid, "rkv_get_packed", get_packed_in_t, get_packed_out_t, NULL);

        c->erase_id = MARGO_REGISTER(mid, "rkv_erase", erase_in_t, erase_out_t, NULL);

//        c->list_keys_id = MARGO_REGISTER(mid, "rkv_list_keys", list_keys_in_t, list_keys_out_t, NULL);
//        c->list_keys_packed_id = MARGO_REGISTER(mid, "rkv_list_keys_packed", list_keys_packed_in_t, list_keys_packed_out_t, NULL);

//        c->list_keyvals_id = MARGO_REGISTER(mid, "rkv_list_keyvals", list_keyvals_in_t, list_keyvals_out_t, NULL);
//        c->list_keyvals_packed_id = MARGO_REGISTER(mid, "rkv_list_keyvals_packed", list_keyvals_packed_in_t, list_keyvals_packed_out_t, NULL);

    }

    *client = c;
    return RKV_SUCCESS;
}

rkv_return_t rkv_client_finalize(rkv_client_t client)
{
    if(client->num_database_handles != 0) {
        margo_warning(client->mid,
            "Warning: %ld database handles not released when rkv_client_finalize was called",
            client->num_database_handles);
    }
    free(client);
    return RKV_SUCCESS;
}

rkv_return_t rkv_database_handle_create(
        rkv_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        rkv_database_id_t database_id,
        rkv_database_handle_t* handle)
{
    if(client == RKV_CLIENT_NULL)
        return RKV_ERR_INVALID_ARGS;

    rkv_database_handle_t rh =
        (rkv_database_handle_t)calloc(1, sizeof(*rh));

    if(!rh) return RKV_ERR_ALLOCATION;

    hg_return_t ret = margo_addr_dup(client->mid, addr, &(rh->addr));
    if(ret != HG_SUCCESS) {
        free(rh);
        return RKV_ERR_FROM_MERCURY;
    }

    rh->client      = client;
    rh->provider_id = provider_id;
    rh->database_id = database_id;
    rh->refcount    = 1;

    client->num_database_handles += 1;

    *handle = rh;
    return RKV_SUCCESS;
}

rkv_return_t rkv_database_handle_ref_incr(
        rkv_database_handle_t handle)
{
    if(handle == RKV_DATABASE_HANDLE_NULL)
        return RKV_ERR_INVALID_ARGS;
    handle->refcount += 1;
    return RKV_SUCCESS;
}

rkv_return_t rkv_database_handle_release(rkv_database_handle_t handle)
{
    if(handle == RKV_DATABASE_HANDLE_NULL)
        return RKV_ERR_INVALID_ARGS;
    handle->refcount -= 1;
    if(handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        handle->client->num_database_handles -= 1;
        free(handle);
    }
    return RKV_SUCCESS;
}

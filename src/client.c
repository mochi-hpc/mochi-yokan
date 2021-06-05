/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "types.h"
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
#if 0
        margo_registered_name(mid, "rkv_sum", &c->sum_id, &flag);
        margo_registered_name(mid, "rkv_hello", &c->hello_id, &flag);
#endif
    } else {
#if 0
        c->sum_id = MARGO_REGISTER(mid, "rkv_sum", sum_in_t, sum_out_t, NULL);
        c->hello_id = MARGO_REGISTER(mid, "rkv_hello", hello_in_t, void, NULL);
        margo_registered_disable_response(mid, c->hello_id, HG_TRUE);
#endif
    }

    *client = c;
    return RKV_SUCCESS;
}

rkv_return_t rkv_client_finalize(rkv_client_t client)
{
    if(client->num_database_handles != 0) {
        fprintf(stderr,  
                "Warning: %ld database handles not released when rkv_client_finalize was called\n",
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

#if 0
rkv_return_t rkv_say_hello(rkv_database_handle_t handle)
{
    hg_handle_t   h;
    hello_in_t     in;
    hg_return_t ret;

    memcpy(&in.database_id, &(handle->database_id), sizeof(in.database_id));

    ret = margo_create(handle->client->mid, handle->addr, handle->client->hello_id, &h);
    if(ret != HG_SUCCESS)
        return RKV_ERR_FROM_MERCURY;

    ret = margo_provider_forward(handle->provider_id, h, &in);
    if(ret != HG_SUCCESS) {
        margo_destroy(h);
        return RKV_ERR_FROM_MERCURY;
    }

    margo_destroy(h);
    return RKV_SUCCESS;
}

rkv_return_t rkv_compute_sum(
        rkv_database_handle_t handle,
        int32_t x,
        int32_t y,
        int32_t* result)
{
    hg_handle_t   h;
    sum_in_t     in;
    sum_out_t   out;
    hg_return_t hret;
    rkv_return_t ret;

    memcpy(&in.database_id, &(handle->database_id), sizeof(in.database_id));
    in.x = x;
    in.y = y;

    hret = margo_create(handle->client->mid, handle->addr, handle->client->sum_id, &h);
    if(hret != HG_SUCCESS)
        return RKV_ERR_FROM_MERCURY;

    hret = margo_provider_forward(handle->provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        ret = RKV_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        ret = RKV_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;
    if(ret == RKV_SUCCESS)
        *result = out.result;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}
#endif

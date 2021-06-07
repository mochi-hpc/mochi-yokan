/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/module.h>
#include <string.h>
#include "rkv/rkv-server.h"
#include "rkv/rkv-client.h"
#include "rkv/rkv-admin.h"
#include "rkv/rkv-provider-handle.h"
#include "../client/client.h"

static int rkv_register_provider(
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);

    struct rkv_provider_args rkv_args = { 0 };
    rkv_args.config = bedrock_args_get_config(args);
    rkv_args.pool   = bedrock_args_get_pool(args);

    return rkv_provider_register(mid, provider_id, &rkv_args,
                                   (rkv_provider_t*)provider);
}

static int rkv_deregister_provider(
        bedrock_module_provider_t provider)
{
    return rkv_provider_destroy((rkv_provider_t)provider);
}

static char* rkv_get_provider_config(
        bedrock_module_provider_t provider) {
    (void)provider;
    // TODO
    return strdup("{}");
}

static int rkv_init_client(
        bedrock_args_t args,
        bedrock_module_client_t* client)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    return rkv_client_init(mid, (rkv_client_t*)client);
}

static int rkv_finalize_client(
        bedrock_module_client_t client)
{
    return rkv_client_finalize((rkv_client_t)client);
}

static char* rkv_get_client_config(
        bedrock_module_client_t client) {
    (void)client;
    // TODO
    return strdup("{}");
}

static int rkv_create_provider_handle(
        bedrock_module_client_t client,
        hg_addr_t address,
        uint16_t provider_id,
        bedrock_module_provider_handle_t* ph)
{
    rkv_client_t c = (rkv_client_t)client;
    rkv_provider_handle_t tmp = calloc(1, sizeof(*tmp));
    margo_addr_dup(c->mid, address, &(tmp->addr));
    tmp->provider_id = provider_id;
    *ph = (bedrock_module_provider_handle_t)tmp;
    return BEDROCK_SUCCESS;
}

static int rkv_destroy_provider_handle(
        bedrock_module_provider_handle_t ph)
{
    rkv_provider_handle_t tmp = (rkv_provider_handle_t)ph;
    margo_addr_free(tmp->mid, tmp->addr);
    free(tmp);
    return BEDROCK_SUCCESS;
}

static struct bedrock_module rkv = {
    .register_provider       = rkv_register_provider,
    .deregister_provider     = rkv_deregister_provider,
    .get_provider_config     = rkv_get_provider_config,
    .init_client             = rkv_init_client,
    .finalize_client         = rkv_finalize_client,
    .get_client_config       = rkv_get_client_config,
    .create_provider_handle  = rkv_create_provider_handle,
    .destroy_provider_handle = rkv_destroy_provider_handle,
    .provider_dependencies   = NULL,
    .client_dependencies     = NULL
};

BEDROCK_REGISTER_MODULE(rkv, rkv)

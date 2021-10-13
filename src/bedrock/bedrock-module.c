/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/module.h>
#include <string.h>
#include "yokan/server.h"
#include "yokan/client.h"
#include "yokan/admin.h"
#include "yokan/provider-handle.h"
#include "../client/client.h"

static int yk_register_provider(
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);

    struct yk_provider_args yk_args = { 0 };
    yk_args.config = bedrock_args_get_config(args);
    yk_args.pool   = bedrock_args_get_pool(args);

    return yk_provider_register(mid, provider_id, &yk_args,
                                   (yk_provider_t*)provider);
}

static int yk_deregister_provider(
        bedrock_module_provider_t provider)
{
    return yk_provider_destroy((yk_provider_t)provider);
}

static char* yk_get_provider_config(
        bedrock_module_provider_t provider) {
    return yk_provider_get_config(provider);
}

static int yk_init_client(
        bedrock_args_t args,
        bedrock_module_client_t* client)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    return yk_client_init(mid, (yk_client_t*)client);
}

static int yk_finalize_client(
        bedrock_module_client_t client)
{
    return yk_client_finalize((yk_client_t)client);
}

static char* yk_get_client_config(
        bedrock_module_client_t client) {
    (void)client;
    // TODO
    return strdup("{}");
}

static int yk_create_provider_handle(
        bedrock_module_client_t client,
        hg_addr_t address,
        uint16_t provider_id,
        bedrock_module_provider_handle_t* ph)
{
    yk_client_t c = (yk_client_t)client;
    yk_provider_handle_t tmp = calloc(1, sizeof(*tmp));
    margo_addr_dup(c->mid, address, &(tmp->addr));
    tmp->provider_id = provider_id;
    *ph = (bedrock_module_provider_handle_t)tmp;
    return BEDROCK_SUCCESS;
}

static int yk_destroy_provider_handle(
        bedrock_module_provider_handle_t ph)
{
    yk_provider_handle_t tmp = (yk_provider_handle_t)ph;
    margo_addr_free(tmp->mid, tmp->addr);
    free(tmp);
    return BEDROCK_SUCCESS;
}

static struct bedrock_module yokan = {
    .register_provider       = yk_register_provider,
    .deregister_provider     = yk_deregister_provider,
    .get_provider_config     = yk_get_provider_config,
    .init_client             = yk_init_client,
    .finalize_client         = yk_finalize_client,
    .get_client_config       = yk_get_client_config,
    .create_provider_handle  = yk_create_provider_handle,
    .destroy_provider_handle = yk_destroy_provider_handle,
    .provider_dependencies   = NULL,
    .client_dependencies     = NULL
};

BEDROCK_REGISTER_MODULE(yokan, yokan)

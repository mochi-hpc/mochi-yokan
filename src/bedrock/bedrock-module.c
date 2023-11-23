/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/module.h>
#include <string.h>
#include <stdlib.h>
#include "yokan/server.h"
#include "yokan/client.h"
#include "yokan/database.h"
#include "../client/client.hpp"

static int yk_register_provider(
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);

    struct yk_provider_args yk_args = { 0 };
    const char* config = bedrock_args_get_config(args);
    yk_args.pool       = bedrock_args_get_pool(args);

    // these are optional, but if they are not present,
    // bedrock_args_get_dependency will return NULL.
    yk_args.remi.provider = bedrock_args_get_dependency(args, "remi_provider", 0);
    yk_args.remi.provider = bedrock_args_get_dependency(args, "remi_client", 0);

    return yk_provider_register(mid, provider_id, config, &yk_args,
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
    yk_database_handle_t db;
    yk_return_t ret = yk_database_handle_create(c, address, provider_id, &db);
    if(ret != YOKAN_SUCCESS) return ret;
    *ph = (bedrock_module_provider_handle_t)db;
    return BEDROCK_SUCCESS;
}

static int yk_destroy_provider_handle(
        bedrock_module_provider_handle_t ph)
{
    yk_database_handle_t db = (yk_database_handle_t)ph;
    return yk_database_handle_release(db);
}

static struct bedrock_dependency yokan_provider_dependencies[] = {
    { "remi_provider", "remi", BEDROCK_OPTIONAL },
    { "remi_client", "remi", BEDROCK_OPTIONAL },
    BEDROCK_NO_MORE_DEPENDENCIES
};

static struct bedrock_module yokan = {
    .register_provider       = yk_register_provider,
    .deregister_provider     = yk_deregister_provider,
    .get_provider_config     = yk_get_provider_config,
    .init_client             = yk_init_client,
    .finalize_client         = yk_finalize_client,
    .get_client_config       = yk_get_client_config,
    .create_provider_handle  = yk_create_provider_handle,
    .destroy_provider_handle = yk_destroy_provider_handle,
    .provider_dependencies   = yokan_provider_dependencies,
    .client_dependencies     = NULL
};

BEDROCK_REGISTER_MODULE(yokan, yokan)

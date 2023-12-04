/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/module.h>
#include <nlohmann/json.hpp>
#include <string.h>
#include <stdlib.h>
#include "yokan/server.h"
#include "yokan/client.h"
#include "yokan/database.h"
#include "../client/client.hpp"

static int yk_bedrock_register_provider(
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);

    struct yk_provider_args yk_args = YOKAN_PROVIDER_ARGS_INIT;
    const char* config = bedrock_args_get_config(args);
    yk_args.pool       = bedrock_args_get_pool(args);

    // these are optional, but if they are not present,
    // bedrock_args_get_dependency will return NULL.
    yk_args.remi.provider = (remi_provider_t)bedrock_args_get_dependency(args, "remi_provider", 0);
    yk_args.remi.client   = (remi_client_t)bedrock_args_get_dependency(args, "remi_client", 0);

    return yk_provider_register(mid, provider_id, config, &yk_args,
                                (yk_provider_t*)provider);
}

static int yk_bedrock_deregister_provider(
        bedrock_module_provider_t provider)
{
    return yk_provider_destroy((yk_provider_t)provider);
}

static char* yk_bedrock_get_provider_config(
        bedrock_module_provider_t provider) {
    return yk_provider_get_config((yk_provider_t)provider);
}

static int yk_bedrock_init_client(
        bedrock_args_t args,
        bedrock_module_client_t* client)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    return yk_client_init(mid, (yk_client_t*)client);
}

static int yk_bedrock_finalize_client(
        bedrock_module_client_t client)
{
    return yk_client_finalize((yk_client_t)client);
}

static char* yk_bedrock_get_client_config(
        bedrock_module_client_t client) {
    (void)client;
    // TODO
    return strdup("{}");
}

static int yk_bedrock_create_provider_handle(
        bedrock_module_client_t client,
        hg_addr_t address,
        uint16_t provider_id,
        bedrock_module_provider_handle_t* ph)
{
    yk_client_t c = (yk_client_t)client;
    yk_database_handle_t db;
    yk_return_t ret = yk_database_handle_create(c, address, provider_id, true, &db);
    if(ret != YOKAN_SUCCESS) return ret;
    *ph = (bedrock_module_provider_handle_t)db;
    return BEDROCK_SUCCESS;
}

static int yk_bedrock_destroy_provider_handle(
        bedrock_module_provider_handle_t ph)
{
    yk_database_handle_t db = (yk_database_handle_t)ph;
    return yk_database_handle_release(db);
}

static int yk_bedrock_migrate(
        bedrock_module_provider_t p,
        const char* dest_addr,
        uint16_t dest_provider_id,
        const char* migration_config,
        bool remove_source)
{
    yk_provider_t provider = (yk_provider_t)p;
    if(!remove_source) return YOKAN_ERR_OP_UNSUPPORTED;

    struct yk_migration_options options = {nullptr, nullptr, 0};

    using json = nlohmann::json;
    std::string extra_config = "{}";

    json config = migration_config ? json::parse(migration_config) : json::object();
    if(!config.is_object()) return YOKAN_ERR_INVALID_CONFIG;

    if(config.contains("new_root") && config["new_root"].is_string()) {
        options.new_root = config["new_root"].get_ref<const std::string&>().c_str();
    }
    if(config.contains("extra_config")) {
        extra_config = config["extra_config"].dump();
        options.extra_config = extra_config.c_str();
    }
    if(config.contains("xfer_size") && config["xfer_size"].is_number_unsigned()) {
        options.xfer_size = config["xfer_size"].get<size_t>();
    }

    yk_return_t ret = yk_provider_migrate_database(
        provider, dest_addr, dest_provider_id, &options);

    return ret;
}

static struct bedrock_dependency yokan_provider_dependencies[] = {
    { "remi_provider", "remi", BEDROCK_OPTIONAL },
    { "remi_client", "remi", BEDROCK_OPTIONAL },
    BEDROCK_NO_MORE_DEPENDENCIES
};

static struct bedrock_module_v3 yokan = {
    3,
    yk_bedrock_register_provider,
    yk_bedrock_deregister_provider,
    yk_bedrock_get_provider_config,
    yk_bedrock_init_client,
    yk_bedrock_finalize_client,
    yk_bedrock_get_client_config,
    yk_bedrock_create_provider_handle,
    yk_bedrock_destroy_provider_handle,
    yokan_provider_dependencies,
    /* client dependencies */ NULL,
    /* change_provider_pool */ NULL,
    /* snapshot_provider */ NULL,
    /* restore_provider */ NULL,
    yk_bedrock_migrate,
    /* get_provider_dependencies */ NULL,
    /* get_client_dependencies */ NULL
};

extern "C" BEDROCK_REGISTER_MODULE_WITH_VERSION(yokan, yokan, 3)

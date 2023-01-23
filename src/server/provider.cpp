/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <iostream>
#include "config.h"
#include "yokan/server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/linker.hpp"
#include "../common/logging.h"
#include "../common/checks.h"
#include "../buffer/default_bulk_cache.hpp"
#include "../buffer/keep_all_bulk_cache.hpp"
#include "../buffer/lru_bulk_cache.hpp"
#include <string>
#ifdef YOKAN_HAS_REMI
#include <remi/remi-client.h>
#include <remi/remi-server.h>
#include "migration.hpp"
#endif

static void yk_finalize_provider(void* p);

/* Function to check the validity of the token sent by an admin
 * (returns false is the token is incorrect) */
static inline bool check_token(
        yk_provider_t provider,
        const char* token);

static inline bool open_backends_from_config(yk_provider_t provider);

yk_return_t yk_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const struct yk_provider_args* args,
        yk_provider_t* provider)
{
    struct yk_provider_args a = YOKAN_PROVIDER_ARGS_INIT;
    if(args) a = *args;
    yk_provider_t p;
    hg_id_t id;
    hg_bool_t flag;

    YOKAN_LOG_TRACE(mid, "registering YOKAN provider with provider id %u", provider_id);

    flag = margo_is_listening(mid);
    if(flag == HG_FALSE) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(mid, "margo instance is not a server");
        return YOKAN_ERR_INVALID_ARGS;
        // LCOV_EXCL_STOP
    }

    margo_provider_registered_name(mid, "yk_open_database", provider_id, &id, &flag);
    if(flag == HG_TRUE) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(mid, "a provider with id %u is already registered", provider_id);
        return YOKAN_ERR_INVALID_PROVIDER;
        // LCOV_EXCL_STOP
    }

    json config;
    if(a.config != NULL) {
        try {
            config = json::parse(a.config);
        } catch(const std::exception& ex) {
            YOKAN_LOG_ERROR(mid, "failed to parse JSON configuration: %s", ex.what());
            return YOKAN_ERR_INVALID_CONFIG;
        }
    } else {
        config = json::object();
    }

    // checking databases field
    if(not config.contains("databases"))
        config["databases"] = json::array();
    if(not config["databases"].is_array()) {
        YOKAN_LOG_ERROR(mid, "\"databases\" field in configuration is not an array");
        return YOKAN_ERR_INVALID_CONFIG;
    }

    // checking buffer_cache field
    if(not config.contains("buffer_cache")) {
        config["buffer_cache"] = json::object();
        if(a.cache)
            config["buffer_cache"]["type"] = "external";
        else
            config["buffer_cache"]["type"] = "default";
    }
    if(not config["buffer_cache"].is_object()) {
        YOKAN_LOG_ERROR(mid, "\"buffer_cache\" field in configuration is not an object");
        return YOKAN_ERR_INVALID_CONFIG;
    }
    if(not config["buffer_cache"].contains("type")) {
        YOKAN_LOG_ERROR(mid, "\"buffer_cache\" needs a \"type\" field");
        return YOKAN_ERR_INVALID_CONFIG;
    }
    if(not config["buffer_cache"]["type"].is_string()) {
        YOKAN_LOG_ERROR(mid, "\"type\" field in \"buffer_cache\" should be a string");
        return YOKAN_ERR_INVALID_CONFIG;
    }
    if(a.cache && (config["buffer_cache"]["type"] != "external")) {
        YOKAN_LOG_WARNING(mid, "External buffer_cache provided but type is "
            "set to \"%s\" in configuration",
            config["buffer_cache"]["type"].get_ref<const std::string&>().c_str());
        config["buffer_cache"] = json::object();
        config["buffer_cache"]["type"] = "external";
    }

    p = new yk_provider;
    if(p == NULL) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(mid, "Could not allocate memory for provider");
        return YOKAN_ERR_ALLOCATION;
        // LCOV_EXCL_STOP
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;
    p->config = config;
    p->token = (a.token && strlen(a.token)) ? a.token : "";

    /* REMI client and provider */
#ifdef YOKAN_HAS_REMI
    if(a.remi.client && !a.remi.provider) {
        YOKAN_LOG_WARNING(mid,
            "Yokan provider initialized with only a REMI client"
            " will only be able to *send* databases to other providers");
    } else if(!a.remi.client && a.remi.provider) {
        YOKAN_LOG_WARNING(mid,
            "Yokan provider initialized with only a REMI provider"
            " will only be able to *receive* databases from other providers");
    }
    p->remi.client = a.remi.client;
    p->remi.provider = a.remi.provider;
    if(p->remi.provider) {
        char remi_class[16];
        sprintf(remi_class, "yokan/%05u", provider_id);
        int remi_ret = remi_provider_register_migration_class(
            p->remi.provider, remi_class, before_migration_cb,
            after_migration_cb, nullptr, p);
        if(remi_ret != REMI_SUCCESS) {
            YOKAN_LOG_ERROR(mid,
                "Failed to register migration class in REMI:"
                " remi_provider_register_migration_class returned %d",
                remi_ret);
            delete p;
            return YOKAN_ERR_FROM_REMI;
        }
    }
#else
    if(a.remi.client || a.remi.provider) {
        YOKAN_LOG_ERROR(mid,
            "Provided REMI client or provider will be ignored because"
            " Yokan wasn't built with REMI support");
    }
#endif
    /* Bulk cache */
    // TODO find a cache implementation in the configuration
    if(a.cache) {
        p->bulk_cache = *a.cache;
    } else {
        auto& buffer_cache_type = config["buffer_cache"]["type"].get_ref<const std::string&>();
        if(buffer_cache_type == "default")
            p->bulk_cache = yk_default_bulk_cache;
        else if(buffer_cache_type == "keep_all")
            p->bulk_cache = yk_keep_all_bulk_cache;
        else if(buffer_cache_type == "lru")
            p->bulk_cache = yk_lru_bulk_cache;
        else {
            YOKAN_LOG_ERROR(mid, "Invalid buffer_cache type \"%s\"", buffer_cache_type.c_str());
            delete p;
            return YOKAN_ERR_INVALID_CONFIG;
        }
    }
    p->bulk_cache_data = p->bulk_cache.init(mid,
        config["buffer_cache"].dump().c_str());
    if(!p->bulk_cache_data) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(mid, "failed to initialize bulk cache");
        delete p;
        return YOKAN_ERR_ALLOCATION;
        // LCOV_EXCL_STOP
    }

    if(!open_backends_from_config(p)) {
        return YOKAN_ERR_INVALID_CONFIG;
    }

    /* Admin RPCs */
    id = MARGO_REGISTER_PROVIDER(mid, "yk_open_database",
            open_database_in_t, open_database_out_t,
            yk_open_database_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->open_database_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_close_database",
            close_database_in_t, close_database_out_t,
            yk_close_database_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->close_database_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_destroy_database",
            destroy_database_in_t, destroy_database_out_t,
            yk_destroy_database_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->destroy_database_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_list_databases",
            list_databases_in_t, list_databases_out_t,
            yk_list_databases_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_databases_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_migrate_database",
            migrate_database_in_t, migrate_database_out_t,
            yk_migrate_database_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->migrate_database_id = id;

    /* Client RPCs */

    id = MARGO_REGISTER_PROVIDER(mid, "yk_find_by_name",
            find_by_name_in_t, find_by_name_out_t,
            yk_find_by_name_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->find_by_name_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_count",
            count_in_t, count_out_t,
            yk_count_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->count_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_put",
            put_in_t, put_out_t,
            yk_put_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->put_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_put_direct",
            put_direct_in_t, put_direct_out_t,
            yk_put_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->put_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_erase",
            erase_in_t, erase_out_t,
            yk_erase_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->erase_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_erase_direct",
            erase_direct_in_t, erase_direct_out_t,
            yk_erase_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->erase_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_get",
            get_in_t, get_out_t,
            yk_get_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->get_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_get_direct",
            get_direct_in_t, get_direct_out_t,
            yk_get_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->get_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_length",
            length_in_t, length_out_t,
            yk_length_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->length_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_length_direct",
            length_direct_in_t, length_direct_out_t,
            yk_length_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->length_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_exists",
            exists_in_t, exists_out_t,
            yk_exists_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->exists_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_exists_direct",
            exists_direct_in_t, exists_direct_out_t,
            yk_exists_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->exists_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_list_keys",
            list_keys_in_t, list_keys_out_t,
            yk_list_keys_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_keys_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_list_keys_direct",
            list_keys_direct_in_t, list_keys_direct_out_t,
            yk_list_keys_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_keys_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_list_keyvals",
            list_keyvals_in_t, list_keyvals_out_t,
            yk_list_keyvals_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_keyvals_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_list_keyvals_direct",
            list_keyvals_direct_in_t, list_keyvals_direct_out_t,
            yk_list_keyvals_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_keyvals_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_coll_create",
            coll_create_in_t, coll_create_out_t,
            yk_coll_create_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->coll_create_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_coll_drop",
            coll_drop_in_t, coll_drop_out_t,
            yk_coll_drop_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->coll_drop_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_coll_exists",
            coll_exists_in_t, coll_exists_out_t,
            yk_coll_exists_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->coll_exists_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_coll_last_id",
            coll_last_id_in_t, coll_last_id_out_t,
            yk_coll_last_id_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->coll_last_id_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_coll_size",
            coll_size_in_t, coll_size_out_t,
            yk_coll_size_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->coll_size_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_erase",
            doc_erase_in_t, doc_erase_out_t,
            yk_doc_erase_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_erase_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_load",
            doc_load_in_t, doc_load_out_t,
            yk_doc_load_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_load_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_load_direct",
            doc_load_direct_in_t, doc_load_direct_out_t,
            yk_doc_load_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_load_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_store",
            doc_store_in_t, doc_store_out_t,
            yk_doc_store_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_store_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_store_direct",
            doc_store_direct_in_t, doc_store_direct_out_t,
            yk_doc_store_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_store_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_update",
            doc_update_in_t, doc_update_out_t,
            yk_doc_update_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_update_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_update_direct",
            doc_update_direct_in_t, doc_update_direct_out_t,
            yk_doc_update_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_update_direct_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_length",
            doc_length_in_t, doc_length_out_t,
            yk_doc_length_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_length_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_list",
            doc_list_in_t, doc_list_out_t,
            yk_doc_list_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_list_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_list_direct",
            doc_list_direct_in_t, doc_list_direct_out_t,
            yk_doc_list_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_list_direct_id = id;

    margo_provider_push_finalize_callback(mid, p, &yk_finalize_provider, p);

    if(provider)
        *provider = p;
    YOKAN_LOG_INFO(mid, "YOKAN provider registration done");
    return YOKAN_SUCCESS;
}

static void yk_finalize_provider(void* p)
{
    yk_provider_t provider = (yk_provider_t)p;
    margo_instance_id mid = provider->mid;
    for(auto pair : provider->dbs) {
        // LCOV_EXCL_START
        delete pair.second;
        // LCOV_EXCL_STOP
    }
    YOKAN_LOG_TRACE(mid, "Finalizing YOKAN provider");
    margo_deregister(mid, provider->open_database_id);
    margo_deregister(mid, provider->close_database_id);
    margo_deregister(mid, provider->destroy_database_id);
    margo_deregister(mid, provider->list_databases_id);
    margo_deregister(mid, provider->find_by_name_id);
    margo_deregister(mid, provider->count_id);
    margo_deregister(mid, provider->exists_id);
    margo_deregister(mid, provider->exists_direct_id);
    margo_deregister(mid, provider->length_id);
    margo_deregister(mid, provider->length_direct_id);
    margo_deregister(mid, provider->put_id);
    margo_deregister(mid, provider->put_direct_id);
    margo_deregister(mid, provider->get_id);
    margo_deregister(mid, provider->get_direct_id);
    margo_deregister(mid, provider->erase_id);
    margo_deregister(mid, provider->erase_direct_id);
    margo_deregister(mid, provider->list_keys_id);
    margo_deregister(mid, provider->list_keys_direct_id);
    margo_deregister(mid, provider->list_keyvals_id);
    margo_deregister(mid, provider->coll_create_id);
    margo_deregister(mid, provider->coll_drop_id);
    margo_deregister(mid, provider->coll_exists_id);
    margo_deregister(mid, provider->coll_last_id_id);
    margo_deregister(mid, provider->coll_size_id);
    margo_deregister(mid, provider->doc_erase_id);
    margo_deregister(mid, provider->doc_store_id);
    margo_deregister(mid, provider->doc_store_direct_id);
    margo_deregister(mid, provider->doc_load_id);
    margo_deregister(mid, provider->doc_load_direct_id);
    margo_deregister(mid, provider->doc_update_id);
    margo_deregister(mid, provider->doc_update_direct_id);
    margo_deregister(mid, provider->doc_length_id);
    margo_deregister(mid, provider->doc_list_id);
    margo_deregister(mid, provider->doc_list_direct_id);
    provider->bulk_cache.finalize(provider->bulk_cache_data);
    delete provider;
    YOKAN_LOG_INFO(mid, "YOKAN provider successfuly finalized");
}

yk_return_t yk_provider_destroy(
        yk_provider_t provider)
{
    margo_instance_id mid = provider->mid;
    YOKAN_LOG_TRACE(mid, "destroying YOKAN provider");
    /* pop the finalize callback */
    margo_provider_pop_finalize_callback(provider->mid, provider);
    /* call the callback */
    yk_finalize_provider(provider);
    YOKAN_LOG_TRACE(mid, "YOKAN provider successfuly destroyed");
    return YOKAN_SUCCESS;
}

char* yk_provider_get_config(yk_provider_t provider)
{
    return strdup(provider->config.dump().c_str());
}

void yk_open_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    open_database_in_t  in;
    open_database_out_t out;
    yk_database_id_t id;
    bool has_backend_type;
    yk_database_t database;
    char id_str[37];

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

    if(!check_token(provider, in.token)) {
        YOKAN_LOG_ERROR(mid, "invalid token");
        out.ret = YOKAN_ERR_INVALID_TOKEN;
        return;
    }

    has_backend_type = yokan::DatabaseFactory::hasBackendType(in.type);

    if(!has_backend_type) {
        YOKAN_LOG_ERROR(mid, "could not find backend of type \"%s\"", in.type);
        out.ret = YOKAN_ERR_INVALID_BACKEND;
        return;
    }

    if(in.name && in.name[0]) {
        if(provider->db_names.count(std::string(in.name))) {
            YOKAN_LOG_ERROR(mid, "a database with name %s already exists in this provider", in.name);
            out.ret = YOKAN_ERR_INVALID_ARGS;
            return;
        }
    }

    uuid_generate(id.uuid);

    auto name = std::string{in.name ? in.name : ""};
    auto status = yokan::DatabaseFactory::makeDatabase(in.type, name, in.config, &database);
    if(status != yokan::Status::OK) {
        YOKAN_LOG_ERROR(mid, "failed to open database of type %s", in.type);
        out.ret = static_cast<yk_return_t>(status);
        return;
    }
    provider->dbs[id] = database;
    if(in.name && in.name[0]) {
        provider->db_names[std::string(in.name)] = id;
    }

    out.ret = YOKAN_SUCCESS;
    out.id = id;

    yk_database_id_to_string(id, id_str);
    YOKAN_LOG_INFO(mid, "created database %s of type \"%s\"", id_str, in.type);

}
DEFINE_MARGO_RPC_HANDLER(yk_open_database_ult)

void yk_close_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    close_database_in_t  in;
    close_database_out_t out;
    char id_str[37];

    yk_database_id_to_string(in.id, id_str);

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

    if(!check_token(provider, in.token)) {
        YOKAN_LOG_ERROR(mid, "invalid token");
        out.ret = YOKAN_ERR_INVALID_TOKEN;
        return;
    }

    auto database = find_database(provider, &in.id);
    CHECK_DATABASE(database, in.id);

    delete provider->dbs[in.id];
    provider->dbs.erase(in.id);

    auto it = std::find_if(provider->db_names.begin(),
                           provider->db_names.end(),
                           [&in](auto& p) { return p.second == in.id; });
    if(it != provider->db_names.end())
        provider->db_names.erase(it);

    out.ret = YOKAN_SUCCESS;

    char db_id_str[37];
    yk_database_id_to_string(in.id, db_id_str);
    YOKAN_LOG_INFO(mid, "closed database %s", db_id_str);
}
DEFINE_MARGO_RPC_HANDLER(yk_close_database_ult)

void yk_destroy_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    destroy_database_in_t  in;
    destroy_database_out_t out;

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

    if(!check_token(provider, in.token)) {
        YOKAN_LOG_ERROR(mid, "invalid token");
        out.ret = YOKAN_ERR_INVALID_TOKEN;
        return;
    }

    auto database = find_database(provider, &in.id);
    CHECK_DATABASE(database, in.id);

    database->destroy();
    delete database;
    provider->dbs.erase(in.id);

    auto it = std::find_if(provider->db_names.begin(),
                           provider->db_names.end(),
                           [&in](auto& p) { return p.second == in.id; });
    if(it != provider->db_names.end())
        provider->db_names.erase(it);

    char db_id_str[37];
    yk_database_id_to_string(in.id, db_id_str);
    YOKAN_LOG_INFO(mid, "destroyed database %s", db_id_str);

    out.ret = YOKAN_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(yk_destroy_database_ult)

void yk_list_databases_ult(hg_handle_t h)
{
    hg_return_t hret;
    list_databases_in_t  in;
    list_databases_out_t out;
    out.ids = nullptr;
    out.count = 0;
    unsigned i;

    DEFER(free(out.ids));
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

    if(!check_token(provider, in.token)) {
        YOKAN_LOG_ERROR(mid, "invalid token");
        out.ret = YOKAN_ERR_INVALID_TOKEN;
        return;
    }

    out.ret   = YOKAN_SUCCESS;
    out.count = provider->dbs.size() < in.max_ids ? provider->dbs.size() : in.max_ids;
    out.ids   = (yk_database_id_t*)calloc(out.count, sizeof(*out.ids));

    i = 0;
    for(auto& pair : provider->dbs) {
        out.ids[i] = pair.first;
        i += 1;
        if(i == out.count)
            break;
    }
    out.count = i;
}
DEFINE_MARGO_RPC_HANDLER(yk_list_databases_ult)

void yk_migrate_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    migrate_database_in_t  in;
    migrate_database_out_t out;
    hg_addr_t target_addr = HG_ADDR_NULL;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    yk_provider_t provider = (yk_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

#ifdef YOKAN_HAS_REMI
    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(!check_token(provider, in.token)) {
        YOKAN_LOG_ERROR(mid, "invalid token");
        out.ret = YOKAN_ERR_INVALID_TOKEN;
        return;
    }

    auto database = find_database(provider, &in.origin_id);
    CHECK_DATABASE(database, in.origin_id);

    // lookup target address
    hret = margo_addr_lookup(mid, in.target_address, &target_addr);
    CHECK_HRET_OUT(hret, margo_addr_lookup);
    DEFER(margo_addr_free(mid, target_addr));

    // create REMI provider handle
    remi_provider_handle_t remi_ph = NULL;
    int rret = remi_provider_handle_create(
        provider->remi.client, target_addr, in.target_provider_id,
        &remi_ph);
    CHECK_RRET_OUT(rret, remi_provider_handle_create);
    DEFER(remi_provider_handle_release(remi_ph));

    // create MigrationHandle from the database
    std::unique_ptr<yokan::MigrationHandle> mh;
    auto status = database->startMigration(mh);

    if(status != yokan::Status::OK) {
        out.ret = static_cast<decltype(out.ret)>(status);
        return;
    }

    // create REMI fileset
    remi_fileset_t fileset = REMI_FILESET_NULL;
    char remi_class[16];
    sprintf(remi_class, "yokan/%05u", in.target_provider_id);
    rret = remi_fileset_create(remi_class, mh->getRoot().c_str(), &fileset);
    CHECK_RRET_OUT_CANCEL(rret, remi_fileset_create, mh);
    DEFER(remi_fileset_free(fileset));

    // fill REMI fileset
    for(const auto& file : mh->getFiles()) {
        if(!file.empty() && file.back() == '/') {
            rret = remi_fileset_register_directory(fileset, file.c_str());
            CHECK_RRET_OUT_CANCEL(rret, remi_fileset_register_file, mh);
        } else {
            rret = remi_fileset_register_file(fileset, file.c_str());
            CHECK_RRET_OUT_CANCEL(rret, remi_fileset_register_file, mh);
        }
    }

    // register REMI metadata
    char uuid[37];
    uuid_unparse(in.origin_id.uuid, uuid);
    rret = remi_fileset_register_metadata(fileset,
        "uuid", uuid);
    CHECK_RRET_OUT_CANCEL(rret, remi_fileset_register_metadata, mh);
    rret = remi_fileset_register_metadata(fileset,
        "db_config", database->config().c_str());
    CHECK_RRET_OUT_CANCEL(rret, remi_fileset_register_metadata, mh);
    rret = remi_fileset_register_metadata(fileset,
        "type", database->type().c_str());
    CHECK_RRET_OUT_CANCEL(rret, remi_fileset_register_metadata, mh);
    rret = remi_fileset_register_metadata(fileset,
        "name", database->name().c_str());
    CHECK_RRET_OUT_CANCEL(rret, remi_fileset_register_metadata, mh);
    rret = remi_fileset_register_metadata(fileset,
        "migration_config", in.extra_config);
    CHECK_RRET_OUT_CANCEL(rret, remi_fileset_register_metadata, mh);

    // set xfer size
    if(in.xfer_size) {
        rret = remi_fileset_set_xfer_size(fileset, in.xfer_size);
        CHECK_RRET_OUT_CANCEL(rret, remi_fileset_set_xfer_size, mh);
    }

    // issue migration
    int remi_status = 0;
    rret = remi_fileset_migrate(remi_ph, fileset, in.new_root,
            REMI_KEEP_SOURCE, REMI_USE_MMAP, &remi_status);
    if(remi_status) {
        YOKAN_LOG_ERROR(mid, "REMI migration callback returned %d on target", remi_status);
        YOKAN_LOG_ERROR(mid, "^ target was %s with provider id %d", in.target_address, in.target_provider_id);
        out.ret = remi_status;
        mh->cancel();
        return;
    }
    CHECK_RRET_OUT_CANCEL(rret, remi_fileset_migrate, mh);

    out.target_id = in.origin_id;
    out.ret = YOKAN_SUCCESS;
#else
    out.ret = YOKAN_ERR_OP_UNSUPPORTED;
#endif
}
DEFINE_MARGO_RPC_HANDLER(yk_migrate_database_ult)

void yk_find_by_name_ult(hg_handle_t h)
{
    hg_return_t hret;
    find_by_name_in_t  in;
    find_by_name_out_t out;
    out.ret = YOKAN_SUCCESS;

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

    const auto name = std::string(in.db_name);
    auto it = provider->db_names.find(name);
    if(it == provider->db_names.end())
        out.ret = YOKAN_ERR_INVALID_DATABASE;
    else
        out.db_id = it->second;
}
DEFINE_MARGO_RPC_HANDLER(yk_find_by_name_ult)

static inline bool check_token(
        yk_provider_t provider,
        const char* token)
{
    if(provider->token.empty()) return true;
    return provider->token == token;
}

static inline bool open_backends_from_config(yk_provider_t provider)
{
    auto& config = provider->config;
    auto& databases = config["databases"];
    // Check that all the backend entries are objects and have a "type" field,
    // and an optional "config" field that is an object. If the "config" field
    // is not found, it will be added.
    for(auto& db : databases) {
        if(not db.is_object()) {
            YOKAN_LOG_ERROR(provider->mid,
                "\"databases\" should only contain objects");
            return false;
        }
        if(not db.contains("type")) {
            YOKAN_LOG_ERROR(provider->mid,
                "\"type\" field not found in database configuration");
            return false;
        }
        if(db.contains("name")) {
            if(!db["name"].is_string()) {
                YOKAN_LOG_ERROR(provider->mid,
                    "\"name\" field of database should be a string");
                return false;
            }
        }
        auto full_type = db["type"].get<std::string>();
        auto type = full_type;
        {
            auto p = full_type.find(':');
            if(p != std::string::npos) {
                yokan::Linker::open(full_type.substr(0,p));
                type = full_type.substr(p+1);
            }
        }
        bool has_backend_type = yokan::DatabaseFactory::hasBackendType(type);
        if(!has_backend_type) {
            YOKAN_LOG_ERROR(provider->mid,
                "could not find backend of type \"%s\"", type.c_str());
            return false;
        }
        if(!db.contains("config"))
            db["config"] = json::object();
        auto& db_config = db["config"];
        if(not db_config.is_object()) {
            YOKAN_LOG_ERROR(provider->mid,
                "\"config\" field in database definition should be an object");
            return false;
        }
    }
    // Ok, seems like all the store configurations are good
    for(auto& db : databases) {
        yk_database_id_t id;
        yk_database_t database;
        uuid_generate(id.uuid);
        auto type = db["type"].get<std::string>();
        auto name = db.value("name", "");
        auto& initial_db_config =  db["config"];
        auto config = initial_db_config.dump();
        auto status = yokan::DatabaseFactory::makeDatabase(type, name, config, &database);
        if(status != yokan::Status::OK) {
            YOKAN_LOG_ERROR(provider->mid,
                "failed to open database of type %s", type.c_str());
            return false;
        }
        auto final_db_config = json::parse(database->config());
        for(auto& config_entry : initial_db_config.items()) {
            if(not final_db_config.contains(config_entry.key()))
                final_db_config[config_entry.key()] = config_entry.value();
        }
        char db_id[37];
        yk_database_id_to_string(id, db_id);
        db["__id__"] = std::string(db_id);
        db["config"] = std::move(final_db_config);
        provider->dbs[id] = database;
        if(!name.empty())
            provider->db_names[name] = id;

        YOKAN_LOG_INFO(provider->mid, "opened database %s", db_id);
    }
    return true;
}

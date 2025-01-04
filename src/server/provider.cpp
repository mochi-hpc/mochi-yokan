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

static inline bool open_database_from_config(yk_provider_t provider);

yk_return_t yk_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const char* config_str,
        const struct yk_provider_args* args,
        yk_provider_t* provider)
{
    struct yk_provider_args a = YOKAN_PROVIDER_ARGS_INIT;
    if(args) a = *args;
    yk_provider_t p;
    hg_id_t id;
    hg_bool_t flag;

    YOKAN_LOG_TRACE(mid, "registering YOKAN provider with provider id %u", provider_id);

    /* check if another provider with the same ID is already registered */
    if(margo_provider_registered_identity(mid, provider_id)) {
        YOKAN_LOG_ERROR(mid, "A provider with the same ID is already registered");
        return YOKAN_ERR_INVALID_PROVIDER;
    }

    flag = margo_is_listening(mid);
    if(flag == HG_FALSE) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(mid, "margo instance is not a server");
        return YOKAN_ERR_INVALID_ARGS;
        // LCOV_EXCL_STOP
    }

    json config;
    if(config_str != NULL && config_str[0] != '\0') {
        try {
            config = json::parse(config_str);
        } catch(const std::exception& ex) {
            YOKAN_LOG_ERROR(mid, "failed to parse JSON configuration: %s", ex.what());
            return YOKAN_ERR_INVALID_CONFIG;
        }
    } else {
        YOKAN_LOG_ERROR(mid, "missing configuration string in yk_provider_register");
        return YOKAN_ERR_INVALID_ARGS;
    }

    // checking databases field
    if(config.contains("database")) {
        if(not config["database"].is_object()) {
            YOKAN_LOG_ERROR(mid, "\"database\" field in configuration is not an object");
            return YOKAN_ERR_INVALID_CONFIG;
        }
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

    p = new(std::nothrow) yk_provider;
    if(!p) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(mid, "Could not allocate memory for provider");
        return YOKAN_ERR_ALLOCATION;
        // LCOV_EXCL_STOP
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;
    p->config = config;

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
    p->remi.client   = a.remi.client;
    p->remi.provider = a.remi.provider;
    if(p->remi.provider) {
        int remi_ret = remi_provider_register_provider_migration_class(
            p->remi.provider, "yokan", provider_id, before_migration_cb,
            after_migration_cb, [](void*){}, p);
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

    if(!open_database_from_config(p)) {
        delete p;
        return YOKAN_ERR_INVALID_CONFIG;
    }

    p->db->setBulkCache(&p->bulk_cache, p->bulk_cache_data);

    /* Client RPCs */

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

    id = MARGO_REGISTER_PROVIDER(mid, "yk_fetch",
            fetch_in_t, fetch_out_t,
            yk_fetch_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->fetch_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_fetch_direct",
            fetch_direct_in_t, fetch_direct_out_t,
            yk_fetch_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->fetch_direct_id = id;

    margo_registered_name(mid, "yk_fetch_back", &id, &flag);
    if(flag) p->fetch_back_id = id;
    else p->fetch_back_id = MARGO_REGISTER(
        mid, "yk_fetch_back", fetch_back_in_t, fetch_back_out_t, NULL);

    margo_registered_name(mid, "yk_fetch_direct_back", &id, &flag);
    if(flag) p->fetch_direct_back_id = id;
    else p->fetch_direct_back_id = MARGO_REGISTER(
        mid, "yk_fetch_direct_back", fetch_direct_back_in_t, fetch_direct_back_out_t, NULL);

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

    id = MARGO_REGISTER_PROVIDER(mid, "yk_iter",
            iter_in_t, iter_out_t,
            yk_iter_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->iter_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_iter_direct",
            iter_in_t, iter_out_t,
            yk_iter_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->iter_direct_id = id;

    margo_registered_name(mid, "yk_iter_back", &id, &flag);
    if(flag) p->iter_back_id = id;
    else p->iter_back_id = MARGO_REGISTER(
        mid, "yk_iter_back", iter_back_in_t, iter_back_out_t, NULL);

    margo_registered_name(mid, "yk_iter_direct_back", &id, &flag);
    if(flag) p->iter_direct_back_id = id;
    else p->iter_direct_back_id = MARGO_REGISTER(
        mid, "yk_iter_direct_back", iter_direct_back_in_t, iter_direct_back_out_t, NULL);

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

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_fetch",
            doc_fetch_in_t, doc_fetch_out_t,
            yk_doc_fetch_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_fetch_id = id;

    margo_registered_name(mid, "yk_doc_fetch_back", &id, &flag);
    if(flag) p->doc_fetch_back_id = id;
    else p->doc_fetch_back_id = MARGO_REGISTER(
        mid, "yk_doc_fetch_back", doc_fetch_back_in_t, doc_fetch_back_out_t, NULL);

    margo_registered_name(mid, "yk_doc_fetch_direct_back", &id, &flag);
    if(flag) p->doc_fetch_direct_back_id = id;
    else p->doc_fetch_direct_back_id = MARGO_REGISTER(
        mid, "yk_doc_fetch_direct_back", doc_fetch_direct_back_in_t, doc_fetch_back_out_t, NULL);

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

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_iter",
            doc_iter_in_t, doc_iter_out_t,
            yk_doc_iter_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_iter_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "yk_doc_iter_direct",
            doc_iter_in_t, doc_iter_out_t,
            yk_doc_iter_direct_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->doc_iter_direct_id = id;

    margo_registered_name(mid, "yk_doc_iter_back", &id, &flag);
    if(flag) p->doc_iter_back_id = id;
    else p->doc_iter_back_id = MARGO_REGISTER(
        mid, "yk_doc_iter_back", doc_iter_back_in_t, doc_iter_back_out_t, NULL);

    margo_registered_name(mid, "yk_doc_iter_direct_back", &id, &flag);
    if(flag) p->doc_iter_direct_back_id = id;
    else p->doc_iter_direct_back_id = MARGO_REGISTER(
        mid, "yk_doc_iter_direct_back", doc_iter_direct_back_in_t, doc_iter_back_out_t, NULL);

    id = MARGO_REGISTER_PROVIDER(mid, "yk_get_remi_provider_id",
            void, get_remi_provider_id_out_t,
            yk_get_remi_provider_id_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->get_remi_provider_id = id;

    margo_provider_push_finalize_callback(mid, p, &yk_finalize_provider, p);

    margo_provider_register_identity(mid, provider_id, "yokan");

    if(provider)
        *provider = p;
    YOKAN_LOG_TRACE(mid, "YOKAN provider registration done");
    return YOKAN_SUCCESS;
}

static void yk_finalize_provider(void* p)
{
    yk_provider_t provider = (yk_provider_t)p;
    margo_instance_id mid = provider->mid;
#ifdef YOKAN_HAS_REMI
    if(provider->remi.provider)
        remi_provider_deregister_provider_migration_class(
            provider->remi.provider, "yokan", provider->provider_id);
#endif
    if(provider->db) {
        provider->db->destroy();
        delete provider->db;
    }
    YOKAN_LOG_TRACE(mid, "Finalizing YOKAN provider");
    margo_provider_deregister_identity(provider->mid, provider->provider_id);
    margo_deregister(mid, provider->count_id);
    margo_deregister(mid, provider->exists_id);
    margo_deregister(mid, provider->exists_direct_id);
    margo_deregister(mid, provider->length_id);
    margo_deregister(mid, provider->length_direct_id);
    margo_deregister(mid, provider->put_id);
    margo_deregister(mid, provider->put_direct_id);
    margo_deregister(mid, provider->get_id);
    margo_deregister(mid, provider->get_direct_id);
    margo_deregister(mid, provider->fetch_id);
    margo_deregister(mid, provider->fetch_direct_id);
    margo_deregister(mid, provider->erase_id);
    margo_deregister(mid, provider->erase_direct_id);
    margo_deregister(mid, provider->list_keys_id);
    margo_deregister(mid, provider->list_keys_direct_id);
    margo_deregister(mid, provider->list_keyvals_id);
    margo_deregister(mid, provider->iter_id);
    margo_deregister(mid, provider->iter_direct_id);
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
    margo_deregister(mid, provider->doc_fetch_id);
    margo_deregister(mid, provider->doc_update_id);
    margo_deregister(mid, provider->doc_update_direct_id);
    margo_deregister(mid, provider->doc_length_id);
    margo_deregister(mid, provider->doc_list_id);
    margo_deregister(mid, provider->doc_list_direct_id);
    margo_deregister(mid, provider->doc_iter_id);
    provider->bulk_cache.finalize(provider->bulk_cache_data);
    delete provider;
    YOKAN_LOG_TRACE(mid, "YOKAN provider successfuly finalized");
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

static inline yk_return_t get_remi_provider_id_from_remote(
        yk_provider_t provider,
        hg_addr_t dest_address,
        uint16_t  dest_provider_id,
        uint16_t* remi_provider_id) {

    hg_handle_t handle             = HG_HANDLE_NULL;
    get_remi_provider_id_out_t out = {0,0};
    hg_return_t hret               = HG_SUCCESS;

    hret = margo_create(provider->mid, dest_address, provider->get_remi_provider_id, &handle);
    if(hret != HG_SUCCESS) return YOKAN_ERR_FROM_MERCURY;
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dest_provider_id, handle, NULL);
    if(hret != HG_SUCCESS) return YOKAN_ERR_FROM_MERCURY;

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) return YOKAN_ERR_FROM_MERCURY;
    DEFER(margo_free_output(handle, &out));

    if(out.ret == YOKAN_SUCCESS) *remi_provider_id = out.provider_id;
    return static_cast<yk_return_t>(out.ret);
}

yk_return_t yk_provider_migrate_database(
          yk_provider_t provider,
          const char* dest_addr_str,
          uint16_t dest_provider_id,
          const struct yk_migration_options* options) {

    if(!provider || !dest_addr_str)
        return YOKAN_ERR_INVALID_ARGS;

#ifndef YOKAN_HAS_REMI
    return YOKAN_ERR_OP_UNSUPPORTED;
#else

    yk_return_t ret;
    hg_return_t hret;
    uint16_t    remi_provider_id;
    hg_addr_t   dest_addr;

    // check if there is a database to migrate
    auto database = provider->db;
    if(!database) return YOKAN_ERR_INVALID_DATABASE;

    // lookup destination address
    hret = margo_addr_lookup(provider->mid, dest_addr_str, &dest_addr);
    if(hret != HG_SUCCESS) return YOKAN_ERR_FROM_MERCURY;
    DEFER(margo_addr_free(provider->mid, dest_addr));

    // get the REMI provider Id associated with the destination provider
    ret = get_remi_provider_id_from_remote(
            provider, dest_addr, dest_provider_id,
            &remi_provider_id);
    if(ret != YOKAN_SUCCESS) return ret;

    // create REMI provider handle
    remi_provider_handle_t remi_ph = NULL;
    int rret = remi_provider_handle_create(
        provider->remi.client, dest_addr, remi_provider_id,
        &remi_ph);
    if(rret != REMI_SUCCESS) {
        YOKAN_LOG_ERROR(provider->mid, "remi_provider_handle_create returned %d", rret);
        return YOKAN_ERR_FROM_REMI;
    }
    DEFER(remi_provider_handle_release(remi_ph));

    // create MigrationHandle from the database
    std::unique_ptr<yokan::MigrationHandle> mh;
    auto status = database->startMigration(mh);
    if(status != yokan::Status::OK)
        return static_cast<yk_return_t>(status);

    // create REMI fileset
    remi_fileset_t fileset = REMI_FILESET_NULL;
    rret = remi_fileset_create("yokan", mh->getRoot().c_str(), &fileset);
    if(rret != REMI_SUCCESS) {
        YOKAN_LOG_ERROR(provider->mid, "remi_fileset_create returned %d", rret);
        return YOKAN_ERR_FROM_REMI;
    }
    DEFER(remi_fileset_free(fileset));

    // set destination provider ID
    remi_fileset_set_provider_id(fileset, dest_provider_id);

    // set transfer size
    if(options)
        remi_fileset_set_xfer_size(fileset, options->xfer_size);

    // fill REMI fileset
    for(const auto& file : mh->getFiles()) {
        if(!file.empty() && file.back() == '/') {
            rret = remi_fileset_register_directory(fileset, file.c_str());
            if(rret != REMI_SUCCESS) {
                YOKAN_LOG_ERROR(provider->mid, "remi_fileset_register_directory returned %d", rret);
                return YOKAN_ERR_FROM_REMI;
            }
        } else {
            rret = remi_fileset_register_file(fileset, file.c_str());
            if(rret != REMI_SUCCESS) {
                YOKAN_LOG_ERROR(provider->mid, "remi_fileset_register_file returned %d", rret);
                return YOKAN_ERR_FROM_REMI;
            }
        }
    }

    // register REMI metadata
    rret = remi_fileset_register_metadata(fileset,
        "db_config", database->config().c_str());
    if(rret != REMI_SUCCESS) {
        YOKAN_LOG_ERROR(provider->mid, "remi_fileset_register_metadata returned %d", rret);
        return YOKAN_ERR_FROM_REMI;
    }
    rret = remi_fileset_register_metadata(fileset,
        "type", database->type().c_str());
    if(rret != REMI_SUCCESS) {
        YOKAN_LOG_ERROR(provider->mid, "remi_fileset_register_metadata returned %d", rret);
        return YOKAN_ERR_FROM_REMI;
    }
    rret = remi_fileset_register_metadata(fileset,
        "migration_config", options && options->extra_config ? options->extra_config : "{}");
    if(rret != REMI_SUCCESS) {
        YOKAN_LOG_ERROR(provider->mid, "remi_fileset_register_metadata returned %d", rret);
        return YOKAN_ERR_FROM_REMI;
    }

    // issue migration
    int remi_status = 0;
    auto new_root   = options && options->new_root ? options->new_root : NULL;
    rret = remi_fileset_migrate(remi_ph, fileset,
                                new_root ? new_root : mh->getRoot().c_str(),
                                REMI_REMOVE_SOURCE, REMI_USE_MMAP, &remi_status);
    if(remi_status) {
        YOKAN_LOG_ERROR(provider->mid, "REMI migration callback returned %d on target", remi_status);
        YOKAN_LOG_ERROR(provider->mid, "^ target was %s with provider id %d", dest_addr_str, dest_provider_id);
        mh->cancel();
        return YOKAN_ERR_FROM_REMI;
    }

    if(rret != REMI_SUCCESS) {
        YOKAN_LOG_ERROR(provider->mid, "remi_fileset_migrate returned %d", rret);
        mh->cancel();
        return YOKAN_ERR_FROM_REMI;
    }

    mh.reset();

    // clear database locally
    database->destroy();
    delete provider->db;
    provider->db = nullptr;

    return YOKAN_SUCCESS;
#endif
}

static inline bool open_database_from_config(yk_provider_t provider)
{
    auto& config = provider->config;
    if(!config.contains("database"))
        return true;
    auto& db = config["database"];
    std::string type;
    {
        // Check that we have a "type" field,
        // and an optional "config" field that is an object.
        // If the "config" field is not found, it will be added.
        if(not db.contains("type")) {
            YOKAN_LOG_ERROR(provider->mid,
                    "\"type\" field not found in database configuration");
            return false;
        }
        if(not db["type"].is_string()) {
            YOKAN_LOG_ERROR(provider->mid,
                    "\"type\" field in database configuration should be a string");
            return false;
        }
        auto& full_type = db["type"].get_ref<const std::string&>();
        type = full_type;
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
    // Ok, seems like the configuration is good
    {
        yk_database_t database;
        auto db_type = db["type"].get<std::string>();
        auto& initial_db_config =  db["config"];
        auto db_config = initial_db_config.dump();
        auto status = yokan::DatabaseFactory::makeDatabase(type, db_config, &database);
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
        db["config"] = std::move(final_db_config);
        provider->db = database;
    }
    return true;
}

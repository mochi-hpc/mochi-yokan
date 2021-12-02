/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/linker.hpp"
#include "../common/logging.h"
#include "../common/checks.h"
#include "../buffer/default_bulk_cache.hpp"

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
    if(not config.contains("databases"))
        config["databases"] = json::array();
    if(not config["databases"].is_array()) {
        YOKAN_LOG_ERROR(mid, "\"databases\" field in configuration is not an array");
        return YOKAN_ERR_INVALID_CONFIG;
    }

    p = new yk_provider;
    if(p == NULL) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(mid, "could not allocate memory for provider");
        return YOKAN_ERR_ALLOCATION;
        // LCOV_EXCL_STOP
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;
    p->config = config;
    p->token = (a.token && strlen(a.token)) ? a.token : "";

    /* Bulk cache */
    // TODO find a cache implementation in the configuration
    if(a.cache) {
        p->bulk_cache = *a.cache;
    } else {
        p->bulk_cache = yk_default_bulk_cache;
    }
    // TODO pass a configuration field
    p->bulk_cache_data = p->bulk_cache.init(mid, NULL);
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

    id = MARGO_REGISTER_PROVIDER(mid, "yk_get",
            get_in_t, get_out_t,
            yk_get_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->get_id = id;

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

    id = MARGO_REGISTER_PROVIDER(mid, "yk_list_keyvals",
            list_keyvals_in_t, list_keyvals_out_t,
            yk_list_keyvals_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_keyvals_id = id;

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
    margo_deregister(mid, provider->exists_id);
    margo_deregister(mid, provider->exists_direct_id);
    margo_deregister(mid, provider->length_id);
    margo_deregister(mid, provider->length_direct_id);
    margo_deregister(mid, provider->put_id);
    margo_deregister(mid, provider->put_direct_id);
    margo_deregister(mid, provider->get_id);
    margo_deregister(mid, provider->erase_id);
    margo_deregister(mid, provider->list_keys_id);
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
    margo_deregister(mid, provider->doc_update_id);
    margo_deregister(mid, provider->doc_length_id);
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
        return;;
    }

    has_backend_type = yokan::DatabaseFactory::hasBackendType(in.type);

    if(!has_backend_type) {
        YOKAN_LOG_ERROR(mid, "could not find backend of type \"%s\"", in.type);
        out.ret = YOKAN_ERR_INVALID_BACKEND;
        return;
    }

    uuid_generate(id.uuid);

    auto status = yokan::DatabaseFactory::makeDatabase(in.type, in.config, &database);
    if(status != yokan::Status::OK) {
        YOKAN_LOG_ERROR(mid, "failed to open database of type %s", in.type);
        out.ret = static_cast<yk_return_t>(status);
        return;
    }
    provider->dbs[id] = database;

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
        auto& initial_db_config =  db["config"];
        auto config = initial_db_config.dump();
        auto status = yokan::DatabaseFactory::makeDatabase(type, config, &database);
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

        YOKAN_LOG_INFO(provider->mid, "opened database %s", db_id);
    }
    return true;
}

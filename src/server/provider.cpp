/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/logging.h"
#include "../common/checks.h"
#include "../buffer/default_bulk_cache.hpp"

static void rkv_finalize_provider(void* p);

/* Function to check the validity of the token sent by an admin
 * (returns false is the token is incorrect) */
static inline bool check_token(
        rkv_provider_t provider,
        const char* token);

rkv_return_t rkv_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const struct rkv_provider_args* args,
        rkv_provider_t* provider)
{
    struct rkv_provider_args a = RKV_PROVIDER_ARGS_INIT;
    if(args) a = *args;
    rkv_provider_t p;
    hg_id_t id;
    hg_bool_t flag;

    RKV_LOG_TRACE(mid, "registering RKV provider with provider id %u", provider_id);

    flag = margo_is_listening(mid);
    if(flag == HG_FALSE) {
        // LCOV_EXCL_START
        RKV_LOG_ERROR(mid, "margo instance is not a server");
        return RKV_ERR_INVALID_ARGS;
        // LCOV_EXCL_STOP
    }

    margo_provider_registered_name(mid, "rkv_open_database", provider_id, &id, &flag);
    if(flag == HG_TRUE) {
        // LCOV_EXCL_START
        RKV_LOG_ERROR(mid, "a provider with id %u is already registered", provider_id);
        return RKV_ERR_INVALID_PROVIDER;
        // LCOV_EXCL_STOP
    }

    p = new rkv_provider;
    if(p == NULL) {
        // LCOV_EXCL_START
        RKV_LOG_ERROR(mid, "could not allocate memory for provider");
        return RKV_ERR_ALLOCATION;
        // LCOV_EXCL_STOP
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;
    p->token = (a.token && strlen(a.token)) ? a.token : "";

    /* Bulk cache */
    // TODO find a cache implementation in the configuration
    if(a.cache) {
        p->bulk_cache = *a.cache;
    } else {
        p->bulk_cache = rkv_default_bulk_cache;
    }
    // TODO pass a configuration field
    p->bulk_cache_data = p->bulk_cache.init(mid, NULL);
    if(!p->bulk_cache_data) {
        // LCOV_EXCL_START
        RKV_LOG_ERROR(mid, "failed to initialize bulk cache");
        delete p;
        return RKV_ERR_ALLOCATION;
        // LCOV_EXCL_STOP
    }

    /* Admin RPCs */
    id = MARGO_REGISTER_PROVIDER(mid, "rkv_open_database",
            open_database_in_t, open_database_out_t,
            rkv_open_database_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->open_database_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_close_database",
            close_database_in_t, close_database_out_t,
            rkv_close_database_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->close_database_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_destroy_database",
            destroy_database_in_t, destroy_database_out_t,
            rkv_destroy_database_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->destroy_database_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_list_databases",
            list_databases_in_t, list_databases_out_t,
            rkv_list_databases_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_databases_id = id;

    /* Client RPCs */

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_count",
            count_in_t, count_out_t,
            rkv_count_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->count_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_put",
            put_in_t, put_out_t,
            rkv_put_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->put_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_erase",
            erase_in_t, erase_out_t,
            rkv_erase_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->erase_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_get",
            get_in_t, get_out_t,
            rkv_get_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->get_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_length",
            length_in_t, length_out_t,
            rkv_length_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->length_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_exists",
            exists_in_t, exists_out_t,
            rkv_exists_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->exists_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_list_keys",
            list_keys_in_t, list_keys_out_t,
            rkv_list_keys_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_keys_id = id;

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_list_keyvals",
            list_keyvals_in_t, list_keyvals_out_t,
            rkv_list_keyvals_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->list_keyvals_id = id;

    margo_provider_push_finalize_callback(mid, p, &rkv_finalize_provider, p);

    if(provider)
        *provider = p;
    margo_info(mid, "RKV provider registration done");
    return RKV_SUCCESS;
}

static void rkv_finalize_provider(void* p)
{
    rkv_provider_t provider = (rkv_provider_t)p;
    margo_instance_id mid = provider->mid;
    for(auto pair : provider->dbs) {
        // LCOV_EXCL_START
        delete pair.second;
        // LCOV_EXCL_STOP
    }
    margo_info(mid, "Finalizing RKV provider");
    margo_deregister(mid, provider->open_database_id);
    margo_deregister(mid, provider->close_database_id);
    margo_deregister(mid, provider->destroy_database_id);
    margo_deregister(mid, provider->list_databases_id);
    margo_deregister(mid, provider->exists_id);
    margo_deregister(mid, provider->length_id);
    margo_deregister(mid, provider->put_id);
    margo_deregister(mid, provider->get_id);
    margo_deregister(mid, provider->erase_id);
    margo_deregister(mid, provider->list_keys_id);
    margo_deregister(mid, provider->list_keyvals_id);
    provider->bulk_cache.finalize(provider->bulk_cache_data);
    delete provider;
    margo_info(mid, "RKV provider successfuly finalized");
}

rkv_return_t rkv_provider_destroy(
        rkv_provider_t provider)
{
    margo_instance_id mid = provider->mid;
    RKV_LOG_TRACE(mid, "destroying RKV provider");
    /* pop the finalize callback */
    margo_provider_pop_finalize_callback(provider->mid, provider);
    /* call the callback */
    rkv_finalize_provider(provider);
    RKV_LOG_TRACE(mid, "RKV provider successfuly destroyed");
    return RKV_SUCCESS;
}

void rkv_open_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    open_database_in_t  in;
    open_database_out_t out;
    rkv_database_id_t id;
    bool has_backend_type;
    rkv_database_t database;
    char id_str[37];

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(!check_token(provider, in.token)) {
        RKV_LOG_ERROR(mid, "invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        return;;
    }

    has_backend_type = rkv::KeyValueStoreFactory::hasBackendType(in.type);

    if(!has_backend_type) {
        RKV_LOG_ERROR(mid, "could not find backend of type \"%s\"", in.type);
        out.ret = RKV_ERR_INVALID_BACKEND;
        return;
    }

    uuid_generate(id.uuid);

    auto status = rkv::KeyValueStoreFactory::makeKeyValueStore(in.type, in.config, &database);
    if(status != rkv::Status::OK) {
        RKV_LOG_ERROR(mid, "failed to open database of type %s", in.type);
        out.ret = static_cast<rkv_return_t>(status);
        return;
    }
    provider->dbs[id] = database;

    out.ret = RKV_SUCCESS;
    out.id = id;

    rkv_database_id_to_string(id, id_str);
    RKV_LOG_TRACE(mid, "created database %s of type \"%s\"", id_str, in.type);

}
DEFINE_MARGO_RPC_HANDLER(rkv_open_database_ult)

void rkv_close_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    close_database_in_t  in;
    close_database_out_t out;
    char id_str[37];

    rkv_database_id_to_string(in.id, id_str);

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(!check_token(provider, in.token)) {
        RKV_LOG_ERROR(mid, "invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        return;
    }

    auto database = find_database(provider, &in.id);
    CHECK_DATABASE(database, in.id);

    delete provider->dbs[in.id];
    provider->dbs.erase(in.id);
    out.ret = RKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(rkv_close_database_ult)

void rkv_destroy_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    destroy_database_in_t  in;
    destroy_database_out_t out;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(!check_token(provider, in.token)) {
        RKV_LOG_ERROR(mid, "invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        return;
    }

    auto database = find_database(provider, &in.id);
    CHECK_DATABASE(database, in.id);

    database->destroy();
    delete database;
    provider->dbs.erase(in.id);

    out.ret = RKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(rkv_destroy_database_ult)

void rkv_list_databases_ult(hg_handle_t h)
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
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(!check_token(provider, in.token)) {
        RKV_LOG_ERROR(mid, "invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        return;
    }

    out.ret   = RKV_SUCCESS;
    out.count = provider->dbs.size() < in.max_ids ? provider->dbs.size() : in.max_ids;
    out.ids   = (rkv_database_id_t*)calloc(out.count, sizeof(*out.ids));

    i = 0;
    for(auto& pair : provider->dbs) {
        out.ids[i] = pair.first;
        i += 1;
        if(i == out.count)
            break;
    }
    out.count = i;
}
DEFINE_MARGO_RPC_HANDLER(rkv_list_databases_ult)

static inline bool check_token(
        rkv_provider_t provider,
        const char* token)
{
    if(provider->token.empty()) return true;
    return provider->token == token;
}

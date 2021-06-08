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

static void rkv_finalize_provider(void* p);

/* Function to check the validity of the token sent by an admin
 * (returns false is the token is incorrect) */
static inline bool check_token(
        rkv_provider_t provider,
        const char* token);

int rkv_provider_register(
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
        RKV_LOG_ERROR(mid, "margo instance is not a server");
        return RKV_ERR_INVALID_ARGS;
    }

    margo_provider_registered_name(mid, "rkv_sum", provider_id, &id, &flag);
    if(flag == HG_TRUE) {
        RKV_LOG_ERROR(mid, "a provider with id %u is already registered", provider_id);
        return RKV_ERR_INVALID_PROVIDER;
    }

    p = new rkv_provider;
    if(p == NULL) {
        RKV_LOG_ERROR(mid, "could not allocate memory for provider");
        return RKV_ERR_ALLOCATION;
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;
    p->token = (a.token && strlen(a.token)) ? a.token : "";

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

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_put",
            put_in_t, put_out_t,
            rkv_put_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->put_id = id;

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
        delete pair.second;
    }
    margo_info(mid, "Finalizing RKV provider");
    margo_deregister(mid, provider->open_database_id);
    margo_deregister(mid, provider->close_database_id);
    margo_deregister(mid, provider->destroy_database_id);
    margo_deregister(mid, provider->list_databases_id);
    //margo_deregister(mid, provider->exists_id);
    //margo_deregister(mid, provider->exists_multi_id);
    //margo_deregister(mid, provider->exists_packed_id);
    //margo_deregister(mid, provider->length_id);
    //margo_deregister(mid, provider->length_multi_id);
    //margo_deregister(mid, provider->length_packed_id);
    margo_deregister(mid, provider->put_id);
    //margo_deregister(mid, provider->get_id);
    //margo_deregister(mid, provider->get_multi_id);
    //margo_deregister(mid, provider->get_packed_id);
    //margo_deregister(mid, provider->erase_id);
    //margo_deregister(mid, provider->erase_multi_id);
    //margo_deregister(mid, provider->erase_packed_id);
    //margo_deregister(mid, provider->list_keys_id);
    //margo_deregister(mid, provider->list_keys_packed_id);
    //margo_deregister(mid, provider->list_keyvals_id);
    //margo_deregister(mid, provider->list_keyvals_packed_id);
    delete provider;
    margo_info(mid, "RKV provider successfuly finalized");
}

int rkv_provider_destroy(
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

    database = rkv::KeyValueStoreFactory::makeKeyValueStore(in.type, in.config);
    if(database == nullptr) {
        RKV_LOG_ERROR(mid, "failed to open database of type %s", in.type);
        out.ret = RKV_ERR_OTHER;
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

    if(out.ret != RKV_SUCCESS) {
        RKV_LOG_ERROR(mid, "could not destroy database, database may be left in an invalid state");
    }
}
DEFINE_MARGO_RPC_HANDLER(rkv_destroy_database_ult)

void rkv_list_databases_ult(hg_handle_t h)
{
    hg_return_t hret;
    list_databases_in_t  in;
    list_databases_out_t out;
    out.ids = nullptr;
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
        margo_error(mid, "Invalid token");
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
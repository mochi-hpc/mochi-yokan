/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-server.h"
#include "provider.hpp"
#include "types.h"

static void rkv_finalize_provider(void* p);

/* Function to check the validity of the token sent by an admin
 * (returns 0 is the token is incorrect) */
static inline int check_token(
        rkv_provider_t provider,
        const char* token);

/* Admin RPCs */
static DECLARE_MARGO_RPC_HANDLER(rkv_create_database_ult)
static void rkv_create_database_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(rkv_open_database_ult)
static void rkv_open_database_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(rkv_close_database_ult)
static void rkv_close_database_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(rkv_destroy_database_ult)
static void rkv_destroy_database_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(rkv_list_databases_ult)
static void rkv_list_databases_ult(hg_handle_t h);

/* Client RPCs */
static DECLARE_MARGO_RPC_HANDLER(rkv_hello_ult)
static void rkv_hello_ult(hg_handle_t h);
static DECLARE_MARGO_RPC_HANDLER(rkv_sum_ult)
static void rkv_sum_ult(hg_handle_t h);

/* add other RPC declarations here */

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

    margo_info(mid, "Registering RKV provider with provider id %u", provider_id);

    flag = margo_is_listening(mid);
    if(flag == HG_FALSE) {
        margo_error(mid, "Margo instance is not a server");
        return RKV_ERR_INVALID_ARGS;
    }

    margo_provider_registered_name(mid, "rkv_sum", provider_id, &id, &flag);
    if(flag == HG_TRUE) {
        margo_error(mid, "Provider with the same provider id (%u) already register", provider_id);
        return RKV_ERR_INVALID_PROVIDER;
    }

    p = new rkv_provider;
    if(p == NULL) {
        margo_error(mid, "Could not allocate memory for provider");
        return RKV_ERR_ALLOCATION;
    }

    p->mid = mid;
    p->provider_id = provider_id;
    p->pool = a.pool;
    p->token = (a.token && strlen(a.token)) ? strdup(a.token) : NULL;

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

#if 0
    id = MARGO_REGISTER_PROVIDER(mid, "rkv_hello",
            hello_in_t, void,
            rkv_hello_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->hello_id = id;
    margo_registered_disable_response(mid, id, HG_TRUE);

    id = MARGO_REGISTER_PROVIDER(mid, "rkv_sum",
            sum_in_t, sum_out_t,
            rkv_sum_ult, provider_id, p->pool);
    margo_register_data(mid, id, (void*)p, NULL);
    p->sum_id = id;
#endif
    /* add other RPC registration here */
    /* ... */

    /* add backends available at compiler time (e.g. default/dummy backends) */
    //rkv_provider_register_dummy_backend(p); // function from "dummy/dummy-backend.h"

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
    /* deregister other RPC ids ... */
    free(provider->token);
    delete provider;
    margo_info(mid, "RKV provider successfuly finalized");
}

int rkv_provider_destroy(
        rkv_provider_t provider)
{
    margo_instance_id mid = provider->mid;
    margo_info(mid, "Destroying RKV provider");
    /* pop the finalize callback */
    margo_provider_pop_finalize_callback(provider->mid, provider);
    /* call the callback */
    rkv_finalize_provider(provider);
    margo_info(mid, "RKV provider successfuly destroyed");
    return RKV_SUCCESS;
}

static void rkv_open_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    rkv_return_t ret;
    open_database_in_t  in;
    open_database_out_t out;
    rkv_database_id_t id;
    bool has_backend_type;
    rkv_database_t database;
    char id_str[37];

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the provider */
    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);

    /* deserialize the input */
    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        out.ret = RKV_ERR_FROM_MERCURY;
        goto finish;
    }

    /* check the token sent by the admin */
    if(!check_token(provider, in.token)) {
        margo_error(mid, "Invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        goto finish;
    }

    /* check if we can create a database of this type */
    has_backend_type = rkv::KeyValueStoreFactory::hasBackendType(in.type);

    if(!has_backend_type) {
        margo_error(mid, "Could not find backend of type \"%s\"", in.type);
        out.ret = RKV_ERR_INVALID_BACKEND;
        goto finish;
    }

    /* create a uuid for the new database */
    uuid_generate(id.uuid);

    /* create the new database's context */
    database = rkv::KeyValueStoreFactory::makeKeyValueStore(in.type, in.config);
    if(database == nullptr) {
        margo_error(mid, "Failed to open database of type %s", in.type);
        out.ret = RKV_ERR_OTHER;
        goto finish;
    }
    provider->dbs[id] = database;

    /* set the response */
    out.ret = RKV_SUCCESS;
    out.id = id;

    rkv_database_id_to_string(id, id_str);
    margo_debug(mid, "Created database %s of type \"%s\"", id_str, in.type);

finish:
    hret = margo_respond(h, &out);
    hret = margo_free_input(h, &in);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(rkv_open_database_ult)

static void rkv_close_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    rkv_return_t ret;
    close_database_in_t  in;
    close_database_out_t out;
    char id_str[37];

    rkv_database_id_to_string(in.id, id_str);

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the provider */
    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);

    /* deserialize the input */
    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        out.ret = RKV_ERR_FROM_MERCURY;
        goto finish;
    }

    /* check the token sent by the admin */
    if(!check_token(provider, in.token)) {
        margo_error(mid, "Invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        goto finish;
    }

    /* check if the database exists */
    if(provider->dbs.count(in.id) == 0) {
        margo_error(mid, "Could not find and close database with id %s", id_str);
        out.ret = RKV_ERR_INVALID_DATABASE;
        goto finish;
    }

    /* remove the database */
    delete provider->dbs[in.id];
    provider->dbs.erase(in.id);

    margo_debug(mid, "Closed database with id %s", id_str);

finish:
    hret = margo_respond(h, &out);
    hret = margo_free_input(h, &in);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(rkv_close_database_ult)

static void rkv_destroy_database_ult(hg_handle_t h)
{
    hg_return_t hret;
    destroy_database_in_t  in;
    destroy_database_out_t out;
    rkv_database* database = nullptr;
    char id_str[37];

    rkv_database_id_to_string(in.id, id_str);

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the provider */
    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);

    /* deserialize the input */
    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        out.ret = RKV_ERR_FROM_MERCURY;
        goto finish;
    }

    /* check the token sent by the admin */
    if(!check_token(provider, in.token)) {
        margo_error(mid, "Invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        goto finish;
    }

    /* check if the database exists */
    if(provider->dbs.count(in.id) == 0) {
        margo_error(mid, "Could not find and close database with id %s", id_str);
        out.ret = RKV_ERR_INVALID_DATABASE;
        goto finish;
    }

    /* destroy the database */
    database = provider->dbs[in.id];
    database->destroy();
    delete database;
    provider->dbs.erase(in.id);

    out.ret = RKV_SUCCESS;

    if(out.ret == RKV_SUCCESS) {
        margo_debug(mid, "Destroyed database with id %s", id_str);
    } else {
        margo_error(mid, "Could not destroy database, database may be left in an invalid state");
    }

finish:
    hret = margo_respond(h, &out);
    hret = margo_free_input(h, &in);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(rkv_destroy_database_ult)

static void rkv_list_databases_ult(hg_handle_t h)
{
    hg_return_t hret;
    list_databases_in_t  in;
    list_databases_out_t out;
    out.ids = nullptr;
    unsigned i;

    /* find margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find provider */
    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);

    /* deserialize the input */
    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        out.ret = RKV_ERR_FROM_MERCURY;
        goto finish;
    }

    /* check the token sent by the admin */
    if(!check_token(provider, in.token)) {
        margo_error(mid, "Invalid token");
        out.ret = RKV_ERR_INVALID_TOKEN;
        goto finish;
    }

    /* allocate array of database ids */
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

    margo_debug(mid, "Listed databases");

finish:
    hret = margo_respond(h, &out);
    hret = margo_free_input(h, &in);
    free(out.ids);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(rkv_list_databases_ult)

#if 0
static void rkv_hello_ult(hg_handle_t h)
{
    hg_return_t hret;
    hello_in_t in;

    /* find margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find provider */
    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);

    /* deserialize the input */
    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        goto finish;
    }

    /* find the database */
    rkv_database* database = find_database(provider, &in.database_id);
    if(!database) {
        margo_error(mid, "Could not find requested database");
        goto finish;
    }

    /* call hello on the database's context */
    database->fn->hello(database->ctx);

    margo_debug(mid, "Called hello RPC");

finish:
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(rkv_hello_ult)

static void rkv_sum_ult(hg_handle_t h)
{
    hg_return_t hret;
    sum_in_t     in;
    sum_out_t   out;

    /* find the margo instance */
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* find the provider */
    const struct hg_info* info = margo_get_info(h);
    rkv_provider_t provider = (rkv_provider_t)margo_registered_data(mid, info->id);

    /* deserialize the input */
    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        out.ret = RKV_ERR_FROM_MERCURY;
        goto finish;
    }

#if 0
    /* find the database */
    rkv_database* database = find_database(provider, &in.database_id);
    if(!database) {
        margo_error(mid, "Could not find requested database");
        out.ret = RKV_ERR_INVALID_DATABASE;
        goto finish;
    }

    /* call hello on the database's context */
    out.result = database->fn->sum(database->ctx, in.x, in.y);
    out.ret = RKV_SUCCESS;
#endif

    margo_debug(mid, "Called sum RPC");

finish:
    hret = margo_respond(h, &out);
    hret = margo_free_input(h, &in);
    margo_destroy(h);
}
static DEFINE_MARGO_RPC_HANDLER(rkv_sum_ult)
#endif

static inline int check_token(
        rkv_provider_t provider,
        const char* token)
{
    if(!provider->token) return 1;
    return !strcmp(provider->token, token);
}

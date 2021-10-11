/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <yokan/server.h>
#include <yokan/admin.h>
#include <yokan/client.h>
#include <yokan/database.h>
#include "available-backends.h"
#include "munit/munit.h"
#include <unordered_map>
#include <string>

static size_t g_min_key_size = 8;
static size_t g_max_key_size = 32;
static size_t g_min_val_size = 1;
static size_t g_max_val_size = 1024;
static size_t g_num_keyvals  = 64;

struct test_context {
    margo_instance_id                           mid;
    hg_addr_t                                   addr;
    rkv_admin_t                                 admin;
    rkv_client_t                                client;
    rkv_provider_t                              provider;
    rkv_database_id_t                           id;
    rkv_database_handle_t                       dbh;
    std::unordered_map<std::string,std::string> reference;
    bool                                        empty_values = false;
};

static const uint16_t provider_id = 42;

static void* test_common_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    rkv_return_t      ret;
    margo_instance_id mid;
    hg_addr_t         addr;
    rkv_admin_t       admin;
    rkv_client_t      client;
    rkv_provider_t    provider;
    rkv_database_id_t id;
    rkv_database_handle_t dbh;

    // read parameters
    const char* min_key_size = munit_parameters_get(params, "min-key-size");
    const char* max_key_size = munit_parameters_get(params, "max-key-size");
    const char* min_val_size = munit_parameters_get(params, "min-val-size");
    const char* max_val_size = munit_parameters_get(params, "max-val-size");
    const char* num_keyvals  = munit_parameters_get(params, "num-keyvals");
    const char* backend_type = munit_parameters_get(params, "backend");
    const char* backend_config = find_backend_config_for(backend_type);
    if(min_key_size) g_min_key_size = std::atol(min_key_size);
    if(max_key_size) g_max_key_size = std::atol(max_key_size);
    if(min_val_size) g_min_val_size = std::atol(min_key_size);
    if(max_val_size) g_max_val_size = std::atol(max_key_size);
    if(num_keyvals)  g_num_keyvals  = std::atol(num_keyvals);
    if(strcmp(backend_type, "set") == 0
    || strcmp(backend_type, "unordered_set") == 0) {
        g_max_val_size = 0;
        g_min_val_size = 0;
    }

    margo_init_info margo_args = MARGO_INIT_INFO_INITIALIZER;
    margo_args.json_config = "{ \"handle_cache_size\" : 0 }";

    // create margo instance
    mid = margo_init_ext("ofi+tcp", MARGO_SERVER_MODE, &margo_args);
    munit_assert_not_null(mid);
    // set log level
    margo_set_global_log_level(MARGO_LOG_WARNING);
    margo_set_log_level(mid, MARGO_LOG_WARNING);
    // get address of current process
    hg_return_t hret = margo_addr_self(mid, &addr);
    munit_assert_int(hret, ==, HG_SUCCESS);
    // register rkv provider
    struct rkv_provider_args args = RKV_PROVIDER_ARGS_INIT;
    args.token = NULL;
    ret = rkv_provider_register(
            mid, provider_id, &args,
            &provider);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // create an admin
    ret = rkv_admin_init(mid, &admin);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // open a database using the admin
    ret = rkv_open_database(admin, addr,
            provider_id, NULL, backend_type, backend_config, &id);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // create a client
    ret = rkv_client_init(mid, &client);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // create a database handle
    ret = rkv_database_handle_create(client,
            addr, provider_id, id, &dbh);
    // create test context
    struct test_context* context = new test_context;
    munit_assert_not_null(context);
    context->mid      = mid;
    context->addr     = addr;
    context->admin    = admin;
    context->client   = client;
    context->provider = provider;
    context->id       = id;
    context->dbh      = dbh;
    if(g_max_val_size == 0 && g_min_val_size == 0) {
        context->empty_values = true;
    }
    // create random key/value pairs with an empty value every 8 values
    for(unsigned i = 0; i < g_num_keyvals; i++) {
        std::string key;
        std::string val;
        int ksize = munit_rand_int_range(g_min_key_size, g_max_key_size);
        int vsize;
        if(g_min_val_size == 0 && g_max_val_size == 0) {
            vsize = 0;
        } else {
            vsize = i % 8 == 0 ? 0 : munit_rand_int_range(g_min_val_size, g_max_val_size);
        }
        key.resize(ksize);
        val.resize(vsize);
        for(int j = 0; j < ksize; j++) {
            char c = (char)munit_rand_int_range(33, 126);
            key[j] = c;
        }
        for(int j = 0; j < vsize; j++) {
            char c = (char)munit_rand_int_range(33, 126);
            val[j] = c;
        }
        context->reference.emplace(
                std::move(key),
                std::move(val));
    }

    return context;
}

static void test_common_context_tear_down(void* fixture)
{
    rkv_return_t ret;
    struct test_context* context = (struct test_context*)fixture;
    // destroy the database
    ret = rkv_destroy_database(context->admin,
            context->addr, provider_id, NULL, context->id);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // free the admin
    ret = rkv_admin_finalize(context->admin);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // free the database handle
    ret = rkv_database_handle_release(context->dbh);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // free the client
    ret = rkv_client_finalize(context->client);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // free address
    margo_addr_free(context->mid, context->addr);
    // destroy provider (we could let margo finalize it but
    // by calling this function we increase code coverage
    ret = rkv_provider_destroy(context->provider);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // we are not checking the return value of the above function with
    // munit because we need margo_finalize to be called no matter what.
    margo_finalize(context->mid);

    delete context;
}

/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <rkv/rkv-server.h>
#include <rkv/rkv-admin.h>
#include <rkv/rkv-client.h>
#include <rkv/rkv-database.h>
#include "munit/munit.h"
#include <map>
#include <string>

struct test_context {
    margo_instance_id                 mid;
    hg_addr_t                         addr;
    rkv_admin_t                       admin;
    rkv_client_t                      client;
    rkv_database_id_t                 id;
    rkv_database_handle_t             dbh;
    std::map<std::string,std::string> reference;
};

static const uint16_t provider_id = 42;
static const char* backend_config = "{ \"foo\" : \"bar\" }";

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    rkv_return_t      ret;
    margo_instance_id mid;
    hg_addr_t         addr;
    rkv_admin_t       admin;
    rkv_client_t      client;
    rkv_database_id_t id;
    rkv_database_handle_t dbh;
    // create margo instance
    mid = margo_init("ofi+tcp", MARGO_SERVER_MODE, 0, 0);
    munit_assert_not_null(mid);
    // set log level
    margo_set_global_log_level(MARGO_LOG_CRITICAL);
    margo_set_log_level(mid, MARGO_LOG_CRITICAL);
    // get address of current process
    hg_return_t hret = margo_addr_self(mid, &addr);
    munit_assert_int(hret, ==, HG_SUCCESS);
    // register rkv provider
    struct rkv_provider_args args = RKV_PROVIDER_ARGS_INIT;
    args.token = NULL;
    ret = rkv_provider_register(
            mid, provider_id, &args,
            RKV_PROVIDER_IGNORE);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // create an admin
    ret = rkv_admin_init(mid, &admin);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    // open a database using the admin
    ret = rkv_open_database(admin, addr,
            provider_id, NULL, "map", backend_config, &id);
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
    context->mid    = mid;
    context->addr   = addr;
    context->admin  = admin;
    context->client = client;
    context->id     = id;
    context->dbh    = dbh;
    // create random key/value pairs
    for(unsigned i = 0; i < 32; i++) {
        std::string key;
        std::string val;
        int ksize = munit_rand_int_range(8, 16);
        int vsize = munit_rand_int_range(0, 1024);
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

static void test_context_tear_down(void* fixture)
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
    // we are not checking the return value of the above function with
    // munit because we need margo_finalize to be called no matter what.
    margo_finalize(context->mid);

    delete context;
}

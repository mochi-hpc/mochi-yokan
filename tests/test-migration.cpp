/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "config.h"
#ifdef YOKAN_HAS_REMI

#include <stdio.h>
#include <margo.h>
#include <yokan/server.h>
#include <yokan/admin.h>
#include <yokan/client.h>
#include <yokan/database.h>
#include <remi/remi-client.h>
#include <remi/remi-server.h>
#include "available-backends.h"
#include "munit/munit.h"

struct test_context {
    margo_instance_id mid;
    hg_addr_t         addr;
    const char*       db_name;
    const char*       backend_type;
    const char*       backend_config;
    remi_client_t     remi_client;
    yk_client_t       yokan_client;
    yk_admin_t        yokan_admin;
};

static const char* db_name = "theDB";

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    margo_instance_id mid;
    hg_addr_t         addr;
    hg_return_t       hret;
    yk_return_t       yret;
    int               ret;
    // create margo instance
    mid = margo_init("ofi+tcp", MARGO_SERVER_MODE, 0, 0);
    munit_assert_not_null(mid);
    // set log level
    margo_set_global_log_level(MARGO_LOG_INFO);
    margo_set_log_level(mid, MARGO_LOG_INFO);
    // get address of current process
    hret = margo_addr_self(mid, &addr);
    munit_assert_int(hret, ==, HG_SUCCESS);
    // register remi providers
    remi_provider_t remi_providers[2];
    ret = remi_provider_register(
            mid, ABT_IO_INSTANCE_NULL,
            1, ABT_POOL_NULL, &remi_providers[0]);
    munit_assert_int(ret, ==, REMI_SUCCESS);
    ret = remi_provider_register(
            mid, ABT_IO_INSTANCE_NULL,
            2, ABT_POOL_NULL, &remi_providers[1]);
    munit_assert_int(ret, ==, REMI_SUCCESS);
    // create remi client
    remi_client_t remi_client;
    remi_client_init(mid, ABT_IO_INSTANCE_NULL, &remi_client);
    // register yk provider
    struct yk_provider_args args = YOKAN_PROVIDER_ARGS_INIT;
    args.remi.provider = remi_providers[0];
    args.remi.client = remi_client;
    args.token = NULL;
    yret = yk_provider_register(
            mid, 1, &args,
            YOKAN_PROVIDER_IGNORE);
    munit_assert_int(yret, ==, YOKAN_SUCCESS);
    args.remi.provider = remi_providers[1];
    yret = yk_provider_register(
            mid, 2, &args,
            YOKAN_PROVIDER_IGNORE);
    munit_assert_int(yret, ==, YOKAN_SUCCESS);
    // create a Yokan client object
    yk_client_t yokan_client;
    ret = yk_client_init(mid, &yokan_client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // create an admin object
    yk_admin_t yokan_admin;
    ret = yk_admin_init(mid, &yokan_admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // create test context
    struct test_context* context = (struct test_context*)calloc(1, sizeof(*context));
    munit_assert_not_null(context);
    context->mid  = mid;
    context->addr = addr;
    context->db_name = db_name;
    context->backend_type = munit_parameters_get(params, "backend");
    context->backend_config = find_backend_config_for(context->backend_type);
    context->remi_client = remi_client;
    context->yokan_admin = yokan_admin;
    context->yokan_client = yokan_client;
    return context;
}

static void test_context_tear_down(void* fixture)
{
    struct test_context* context = (struct test_context*)fixture;
    // free address
    margo_addr_free(context->mid, context->addr);
    // free the REMI client
    remi_client_finalize(context->remi_client);
    // free the Yokan admin
    yk_admin_finalize(context->yokan_admin);
    // free the Yokan client
    yk_client_finalize(context->yokan_client);

    // we are not checking the return value of the above function with
    // munit because we need margo_finalize to be called no matter what.
    margo_finalize(context->mid);
}

static MunitResult test_migration(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_return_t ret;

    // open a database in provider 1
    yk_database_id_t db_id1;
    ret = yk_open_named_database(
            context->yokan_admin, context->addr,
            1, NULL, context->db_name,
            context->backend_type,
            context->backend_config, &db_id1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // create a database handle
    yk_database_handle_t dbh;
    ret = yk_database_handle_create(
        context->yokan_client,
        context->addr, 1, db_id1, &dbh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // write some values to it
    for(int i = 0; i < 10; i++) {
        char key[16];
        char value[16];
        sprintf(key, "key%05d", i);
        sprintf(value, "value%05d", i);
        size_t ksize = strlen(key);
        size_t vsize = strlen(value);
        ret = yk_put(dbh, 0, key, ksize, value, vsize);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
    }

    char new_root[128];
    sprintf(new_root, "/tmp/migrated-%s", context->backend_type);

    // migrate the database to provider 2
    yk_database_id_t db_id2;
    yk_migration_options options;
    options.new_root = new_root,
    options.extra_config = "{}",
    options.xfer_size = 0;
    ret = yk_migrate_database(
            context->yokan_admin, context->addr,
            1, db_id1, context->addr, 2, NULL, &options, &db_id2);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // trying to access the database from provider 1 should get us an error
    ret = yk_put(dbh, 0, "abc", 3, "def", 3);
    munit_assert_int(ret, ==, YOKAN_ERR_MIGRATED);

    // release handle
    ret = yk_database_handle_release(dbh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // re-create database handle this time with provider 2
    ret = yk_database_handle_create(
        context->yokan_client,
        context->addr, 2, db_id2, &dbh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that we can read the values from the migrated database
    for(int i = 0; i < 10; i++) {
        char key[16];
        char value[16];
        char expected[16];
        sprintf(key, "key%05d", i);
        sprintf(expected, "value%05d", i);
        size_t ksize = strlen(key);
        size_t vsize = 16;
        ret = yk_get(dbh, 0, key, ksize, value, &vsize);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_string_equal(value, expected);
    }

    // release handle
    ret = yk_database_handle_release(dbh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // close the database in provider 1
    ret = yk_close_database(
            context->yokan_admin, context->addr,
            1, NULL, db_id1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // close the database in provider 2
    ret = yk_close_database(
            context->yokan_admin, context->addr,
            1, NULL, db_id2);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/migration", test_migration,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/admin", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

#else // YOKAN_HAS_REMI

int main(int argc, char* argv[]) {
    return 0;
}

#endif // YOKAN_HAS_REMI

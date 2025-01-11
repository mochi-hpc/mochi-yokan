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
#include <yokan/client.h>
#include <yokan/database.h>
#include <yokan/collection.h>
#include <remi/remi-client.h>
#include <remi/remi-server.h>
#include "available-backends.h"
#include "munit/munit.h"

struct test_context {
    margo_instance_id mid;
    hg_addr_t         addr;
    char*             addr_str;
    remi_client_t     remi_client;
    yk_client_t       yokan_client;
    yk_provider_t     yokan_providers[2];
    const char*       backend;
};

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    margo_instance_id mid;
    hg_addr_t         addr;
    hg_return_t       hret;
    yk_return_t       yret;
    int               ret;
    const char*       backend = munit_parameters_get(params, "backend");

    // create margo instance
    mid = margo_init("ofi+tcp", MARGO_SERVER_MODE, 0, 0);
    munit_assert_not_null(mid);

    // set log level
    margo_set_global_log_level(MARGO_LOG_INFO);
    margo_set_log_level(mid, MARGO_LOG_INFO);

    // get address of current process
    hret = margo_addr_self(mid, &addr);
    munit_assert_int(hret, ==, HG_SUCCESS);

    // get address as a string
    char addr_str[128];
    hg_size_t bufsize = 128;
    hret = margo_addr_to_string(mid, addr_str, &bufsize, addr);

    // register remi provider
    remi_provider_t remi_provider;
    ret = remi_provider_register(
            mid, ABT_IO_INSTANCE_NULL,
            3, ABT_POOL_NULL, &remi_provider);
    munit_assert_int(ret, ==, REMI_SUCCESS);

    // create remi client
    remi_client_t remi_client;
    remi_client_init(mid, ABT_IO_INSTANCE_NULL, &remi_client);

    // register yk provider 1 with a database
    yk_provider_t provider1;
    struct yk_provider_args args = YOKAN_PROVIDER_ARGS_INIT;
    args.remi.provider = REMI_PROVIDER_NULL;
    args.remi.client = remi_client;
    auto provider1_config = make_provider_config(backend);
    yret = yk_provider_register(
            mid, 1, provider1_config.c_str(), &args,
            &provider1);
    munit_assert_int(yret, ==, YOKAN_SUCCESS);

    // register yk provider 2 without a database
    yk_provider_t provider2;
    args.remi.provider = remi_provider;
    args.remi.client = REMI_CLIENT_NULL;
    yret = yk_provider_register(
            mid, 2, "{}", &args,
            &provider2);
    munit_assert_int(yret, ==, YOKAN_SUCCESS);

    // create a Yokan client object
    yk_client_t yokan_client;
    ret = yk_client_init(mid, &yokan_client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // create test context
    struct test_context* context = (struct test_context*)calloc(1, sizeof(*context));
    munit_assert_not_null(context);
    context->mid          = mid;
    context->addr         = addr;
    context->addr_str     = strdup(addr_str);
    context->remi_client  = remi_client;
    context->yokan_client = yokan_client;
    context->backend      = backend;
    context->yokan_providers[0] = provider1;
    context->yokan_providers[1] = provider2;

    return context;
}

static void test_context_tear_down(void* fixture)
{
    struct test_context* context = (struct test_context*)fixture;
    // free address
    margo_addr_free(context->mid, context->addr);
    free(context->addr_str);
    // free the REMI client
    remi_client_finalize(context->remi_client);
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

    const char* backend = munit_parameters_get(params, "backend");
    bool stores_kv = !(strcmp(backend, "log") == 0 || strcmp(backend, "array") == 0);
    bool stores_values = (strcmp(context->backend, "set") != 0)
        && (strcmp(context->backend, "unordered_set") != 0);

    // get a handle to the database in provider 1
    yk_database_handle_t dbh1;
    ret = yk_database_handle_create(
            context->yokan_client,
            context->addr, 1, true,
            &dbh1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    if(stores_kv) {

        // write some values to it
        for(int i = 0; i < 10; i++) {
            char key[16];
            char value[16];
            sprintf(key, "key%05d", i);
            sprintf(value, "value%05d", i);
            size_t ksize = strlen(key);
            size_t vsize = strlen(value) * (stores_values ? 1 : 0);
            ret = yk_put(dbh1, 0, key, ksize, value, vsize);
            if(ret == YOKAN_ERR_OP_UNSUPPORTED) // for array and log backends
                ret = YOKAN_SUCCESS;
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }

    }

    if(stores_values) {

        // create a collection
        ret = yk_collection_create(dbh1, "my_collection", 0);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);

        // add some documents in the collection
        for(int i = 0; i < 10; i++) {
            char doc[16];
            sprintf(doc, "doc%05d", i);
            size_t dsize = strlen(doc);
            yk_id_t id;
            ret = yk_doc_store(dbh1, "my_collection", 0, doc, dsize, &id);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }

    }

    char new_root[128];
    sprintf(new_root, "/tmp/migrated-%s", context->backend);

    // migrate the database to provider 2
    yk_migration_options options;
    options.new_root     = new_root;
    options.extra_config = "{}";
    options.xfer_size    = 0;
    ret = yk_provider_migrate_database(
            context->yokan_providers[0],
            context->addr_str, 2, &options);
    if(ret == YOKAN_ERR_OP_UNSUPPORTED) {
        yk_database_handle_release(dbh1);
        return MUNIT_SKIP;
    }
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    if(stores_kv) {
        // trying to access the database from provider 1 should get us an error
        ret = yk_put(dbh1, 0, "abc", 3, "def", 3 * (stores_values ? 1 : 0));
        munit_assert_int(ret, ==, YOKAN_ERR_INVALID_DATABASE);
    }

    if(stores_values) {
        // trying to create a collection from provider 1 should get us an error
        ret = yk_collection_create(dbh1, "my_collection_2", 0);
        munit_assert_int(ret, ==, YOKAN_ERR_INVALID_DATABASE);
    }

    // release handle
    ret = yk_database_handle_release(dbh1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // create database handle this time with provider 2
    yk_database_handle_t dbh2;
    yk_database_handle_create(
            context->yokan_client,
            context->addr, 2, true,
            &dbh2);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    if(stores_kv) {

        // check that we can read the values from the migrated database
        for(int i = 0; i < 10; i++) {
            char key[16];
            char value[16];
            char expected[16];
            sprintf(key, "key%05d", i);
            sprintf(expected, "value%05d", i);
            memset(value, 0, 16);
            size_t ksize = strlen(key);
            size_t vsize = 16 * stores_values;
            ret = yk_get(dbh2, 0, key, ksize, value, &vsize);
            if(ret == YOKAN_ERR_OP_UNSUPPORTED)
                continue;
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            if(stores_values) {
                munit_assert_int(vsize, ==, strlen(expected));
                munit_assert_string_equal(value, expected);
            }
        }
    }

    if(stores_values) {
        // check that we can read the documents from the migrated database
        for(int i = 0; i < 10; i++) {
            char doc[16];
            char expected[16];
            sprintf(expected, "doc%05d", i);
            memset(doc, 0, 16);
            size_t dsize = 16;
            ret = yk_doc_load(dbh2, "my_collection", 0, i, doc, &dsize);
            if(ret == YOKAN_ERR_OP_UNSUPPORTED)
                continue;
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_int(dsize, ==, strlen(expected));
            doc[dsize] = '\0';
            munit_assert_string_equal(doc, expected);
        }
    }

    // release handle
    ret = yk_database_handle_release(dbh2);
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

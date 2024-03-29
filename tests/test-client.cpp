/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <yokan/server.h>
#include <yokan/client.h>
#include <yokan/database.h>
#include "available-backends.h"
#include "munit/munit.h"

struct test_context {
    margo_instance_id mid;
    hg_addr_t         addr;
};

static const uint16_t provider_id = 42;

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    yk_return_t      ret;
    margo_instance_id mid;
    hg_addr_t         addr;
    const char* backend_type = munit_parameters_get(params, "backend");
    auto provider_config     = make_provider_config(backend_type);
    // create margo instance
    mid = margo_init("ofi+tcp", MARGO_SERVER_MODE, 0, 0);
    munit_assert_not_null(mid);
    // set log level
    margo_set_global_log_level(MARGO_LOG_CRITICAL);
    margo_set_log_level(mid, MARGO_LOG_CRITICAL);
    // get address of current process
    hg_return_t hret = margo_addr_self(mid, &addr);
    munit_assert_int(hret, ==, HG_SUCCESS);
    // register yk provider
    struct yk_provider_args args = YOKAN_PROVIDER_ARGS_INIT;
    ret = yk_provider_register(
            mid, provider_id, provider_config.c_str(), &args,
            YOKAN_PROVIDER_IGNORE);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // create test context
    struct test_context* context = (struct test_context*)calloc(1, sizeof(*context));
    munit_assert_not_null(context);
    context->mid   = mid;
    context->addr  = addr;
    return context;
}

static void test_context_tear_down(void* fixture)
{
    struct test_context* context = (struct test_context*)fixture;
    // free address
    margo_addr_free(context->mid, context->addr);
    // we are not checking the return value of the above function with
    // munit because we need margo_finalize to be called no matter what.
    margo_finalize(context->mid);
}

static MunitResult test_client(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_client_t client;
    yk_return_t ret;
    // test that we can create a client object
    ret = yk_client_init(context->mid, &client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can free the client object
    ret = yk_client_finalize(client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitResult test_two_clients(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_client_t client1, client2;
    yk_return_t ret;
    // test that we can create a client object
    ret = yk_client_init(context->mid, &client1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can create a secondclient object
    ret = yk_client_init(context->mid, &client2);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can free the second client object
    ret = yk_client_finalize(client2);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can first the second client object
    ret = yk_client_finalize(client1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitResult test_database(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_client_t client;
    yk_database_handle_t rh;
    yk_return_t ret;
    // test that we can create a client object
    ret = yk_client_init(context->mid, &client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can create a database handle
    ret = yk_database_handle_create(client,
            context->addr, provider_id, true, &rh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can increase the ref count
    ret = yk_database_handle_ref_incr(rh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can retrieve the information
    {
        yk_client_t client2      = YOKAN_CLIENT_NULL;
        hg_addr_t   addr2        = HG_ADDR_NULL;
        uint16_t    provider_id2 = 0;
        ret = yk_database_handle_get_info(rh, &client2, &addr2, &provider_id2);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_ptr(client2, ==, client);
        munit_assert(margo_addr_cmp(context->mid, addr2, context->addr));
        munit_assert_int(provider_id2, ==, provider_id);
    }
    // test that we can destroy the database handle
    ret = yk_database_handle_release(rh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // ... and a second time because of the increase ref
    ret = yk_database_handle_release(rh);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test calls that should fail properly
    ret = yk_database_handle_create(YOKAN_CLIENT_NULL,
            context->addr, provider_id, true, &rh);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_database_handle_create(client,
            HG_ADDR_NULL, provider_id, true, &rh);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_database_handle_ref_incr(YOKAN_DATABASE_HANDLE_NULL);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_database_handle_release(YOKAN_DATABASE_HANDLE_NULL);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    // test that we can free the client object
    ret = yk_client_finalize(client);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/client", test_client,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/client/two", test_two_clients,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/database", test_database,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/admin", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

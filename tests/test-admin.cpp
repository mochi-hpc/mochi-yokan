/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include <yokan/server.h>
#include <yokan/admin.h>
#include "available-backends.h"
#include "munit/munit.h"

struct test_context {
    margo_instance_id mid;
    hg_addr_t         addr;
    const char*       db_name;
    const char*       backend_type;
    const char*       backend_config;
};

static const char* valid_token = "ABCDEFGH";
static const char* wrong_token = "HGFEDCBA";
static const uint16_t provider_id = 42;
static const char* db_name = "theDB";

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    margo_instance_id mid;
    hg_addr_t         addr;
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
    args.token = valid_token;
    yk_return_t ret = yk_provider_register(
            mid, provider_id, &args,
            YOKAN_PROVIDER_IGNORE);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // create test context
    struct test_context* context = (struct test_context*)calloc(1, sizeof(*context));
    munit_assert_not_null(context);
    context->mid  = mid;
    context->addr = addr;
    context->db_name = db_name;
    context->backend_type = munit_parameters_get(params, "backend");
    context->backend_config = find_backend_config_for(context->backend_type);
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

static MunitResult test_admin(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_admin_t admin;
    yk_return_t ret;
    // test that we can create an admin object
    ret = yk_admin_init(context->mid, &admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // test that we can free the admin object
    ret = yk_admin_finalize(admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitResult test_two_admins(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_admin_t admin1, admin2;
    yk_return_t ret;

    ret = yk_admin_init(context->mid, &admin1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_admin_init(context->mid, &admin2);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_admin_finalize(admin2);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_admin_finalize(admin1);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitResult test_database(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_admin_t admin;
    yk_return_t ret;
    yk_database_id_t id;
    // test that we can create an admin object
    ret = yk_admin_init(context->mid, &admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // test that we can open a database with correct type
    ret = yk_open_named_database(admin, context->addr,
            provider_id, valid_token, context->db_name,
            context->backend_type,
            context->backend_config, &id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // test that we can list the databases
    yk_database_id_t ids[4];
    size_t count = 4;
    ret = yk_list_databases(admin, context->addr,
            provider_id, valid_token, ids, &count);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_ulong(count, ==, 1);
    munit_assert_memory_equal(sizeof(id), ids, &id);

    // test that we can close the database we just created
    ret = yk_close_database(admin, context->addr,
            provider_id, valid_token, id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that the database does not appear anymore
    count = 4;
    ret = yk_list_databases(admin, context->addr,
            provider_id, valid_token, ids, &count);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_ulong(count, ==, 0);

    // reopen a database
    ret = yk_open_named_database(admin, context->addr,
            provider_id, valid_token,
            context->db_name, context->backend_type,
            context->backend_config, &id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // test that we can destroy the database we just created
    ret = yk_destroy_database(admin, context->addr,
            provider_id, valid_token, id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that the database does not appear anymore
    count = 4;
    ret = yk_list_databases(admin, context->addr,
            provider_id, valid_token, ids, &count);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_ulong(count, ==, 0);

    // test that we can free the admin object
    ret = yk_admin_finalize(admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitResult test_invalid(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_admin_t admin;
    yk_return_t ret;
    yk_database_id_t id;
    // test that we can create an admin object
    ret = yk_admin_init(context->mid, &admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // test that calling the wrong provider id leads to an error
    ret = yk_open_database(admin, context->addr,
            provider_id + 1, valid_token, context->backend_type,
            context->backend_config, &id);
    munit_assert_int(ret, ==, YOKAN_ERR_FROM_MERCURY);

    // test that calling with the wrong token leads to an error
    ret = yk_open_database(admin, context->addr,
            provider_id, wrong_token, context->backend_type,
            context->backend_config, &id);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_TOKEN);

    // test that calling with the wrong config leads to an error
    ret = yk_open_database(admin, context->addr,
            provider_id, valid_token, context->backend_type,
            "{ashqw{", &id);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_CONFIG);

    // test that calling with an unknown backend leads to an error
    ret = yk_open_database(admin, context->addr,
            provider_id, valid_token, "blah", context->backend_config, &id);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_BACKEND);

    // this creation should be successful
    ret = yk_open_database(admin, context->addr,
            provider_id, valid_token, context->backend_type,
            context->backend_config, &id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that list with invalid token will fail
    size_t count = 4;
    yk_database_id_t ids[4];
    ret = yk_list_databases(admin, context->addr,
            provider_id, wrong_token, ids, &count);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_TOKEN);

    // test that closing an invalid id leads to an error
    yk_database_id_t wrong_id;
    memset((void*) &wrong_id, 0, sizeof(wrong_id));
    ret = yk_close_database(admin, context->addr, provider_id, valid_token, wrong_id);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_DATABASE);

    // test that closing with an invalid token leads to an error
    ret = yk_close_database(admin, context->addr, provider_id, wrong_token, id);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_TOKEN);

    // test that destroying an invalid id leads to an error
    ret = yk_destroy_database(admin, context->addr, provider_id, valid_token, wrong_id);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_DATABASE);

    // test that destroying with an invalid token leads to an error
    ret = yk_destroy_database(admin, context->addr, provider_id, wrong_token, id);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_TOKEN);

    // correctly destroy the created database
    ret = yk_destroy_database(admin, context->addr, provider_id, valid_token, id);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // test that we can free the admin object
    ret = yk_admin_finalize(admin);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/admin", test_admin,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/admin/two", test_two_admins,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/database", test_database,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/invalid", test_invalid,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/admin", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

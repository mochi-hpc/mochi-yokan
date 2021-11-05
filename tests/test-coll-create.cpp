/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <yokan/collection.h>
#include <algorithm>
#include <numeric>
#include <vector>
#include <array>
#include <iostream>

static MunitResult test_coll_create_exists_drop(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    uint8_t flag;
    ret = yk_collection_exists(dbh, "abcd", 0, &flag);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_int(flag, ==, 0);

    ret = yk_collection_create(dbh, "abcd", 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_collection_exists(dbh, "abcd", 0, &flag);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_int(flag, ==, 1);

    ret = yk_collection_drop(dbh, "abcd", 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_collection_exists(dbh, "abcd", 0, &flag);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_int(flag, ==, 0);

    return MUNIT_OK;
}

static MunitResult test_coll_create_store_size_last_id(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    ret = yk_collection_create(dbh, "abcd", 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    const char* docs[] = {
        "matthieu",
        "phil",
        "rob",
        "shane"
    };
    size_t doc_sizes[] = {
        9, 5, 4, 6 /* null terminator is included */
    };
    size_t size;
    yk_id_t last_id;
    char buffer[10];
    size_t bufsize = 10;
    uint8_t flag;

    ret = yk_collection_size(dbh, "abcd", 0, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(size, ==, 0);

    ret = yk_collection_last_id(dbh, "abcd", 0, &last_id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(last_id, ==, 0);

    for(int i=0; i < 4; i++) {
        yk_id_t id;
        ret = yk_doc_store(dbh, "abcd", 0, docs[i], doc_sizes[i], &id);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(id, ==, (long)i);
    }

    ret = yk_doc_load(dbh,"abcd", 0, 2, buffer, &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_string_equal(buffer, docs[2]);

    ret = yk_collection_size(dbh, "abcd", 0, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(size, ==, 4);

    ret = yk_collection_last_id(dbh, "abcd", 0, &last_id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(last_id, ==, 4);

    ret = yk_collection_drop(dbh, "abcd", 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_collection_exists(dbh, "abcd", 0, &flag);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_int(flag, ==, 0);

    ret = yk_collection_size(dbh, "abcd", 0, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    ret = yk_collection_last_id(dbh, "abcd", 0, &last_id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    bufsize = 10;
    ret = yk_doc_load(dbh, "abcd", 0, 2, buffer, &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    /* coll_create */
    { (char*) "/coll/create_exists_drop", test_coll_create_exists_drop,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/create_store_size_last_id", test_coll_create_store_size_last_id,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

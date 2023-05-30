/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-coll-common-setup.hpp"
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
    struct doc_test_context* context = (struct doc_test_context*)data;
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
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    ret = yk_collection_create(dbh, "abcd", 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    size_t size;
    yk_id_t last_id;
    uint8_t flag;

    ret = yk_collection_size(dbh, "abcd", 0, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(size, ==, 0);

    ret = yk_collection_last_id(dbh, "abcd", 0, &last_id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(last_id, ==, -1);

    for(size_t i=0; i < context->reference.size(); i++) {
        yk_id_t id;
        ret = yk_doc_store(dbh, "abcd", 0,
                context->reference[i].data(),
                context->reference[i].size(), &id);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(id, ==, (long)i);
    }

    ret = yk_collection_size(dbh, "abcd", 0, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(size, ==, context->reference.size());

    ret = yk_collection_last_id(dbh, "abcd", 0, &last_id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(last_id, ==, context->reference.size()-1);

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

    std::vector<char> buffer(g_max_val_size);
    size_t bufsize = g_max_val_size;
    ret = yk_doc_load(dbh, "abcd", 0,
            context->reference.size()/2,
            buffer.data(), &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-items", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    /* coll_create */
    { (char*) "/coll/create_exists_drop", test_coll_create_exists_drop,
        doc_test_common_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/create_store_size_last_id", test_coll_create_store_size_last_id,
        doc_test_common_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

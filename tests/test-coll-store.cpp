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

static MunitResult test_coll_store(const MunitParameter params[], void* data)
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
        nullptr, /* include a null document */
        "rob",
        "shane"
    };
    size_t doc_sizes[] = {
        9, 5, 0, 4, 6 /* null terminator is included */
    };

    for(unsigned i=0; i < 5; i++) {
        yk_id_t id;
        ret = yk_doc_store(dbh, "abcd", 0, docs[i], doc_sizes[i], &id);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(id, ==, (long)i);
    }

    for(unsigned i=0; i < 5; i++) {
        yk_id_t id = (yk_id_t)i;
        char buffer[10];
        size_t bufsize = 10;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer, &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, doc_sizes[i]);
        if(bufsize != 0)
            munit_assert_string_equal(buffer, docs[i]);
    }

    /* erroneous cases */

    /* tries to store nullptr with a non-zero size */
    yk_id_t id;
    ret = yk_doc_store(dbh, "abcd", 0, nullptr, doc_sizes[0], &id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to store in a collection that does not exist */
    ret = yk_doc_store(dbh, "efgh", 0, docs[0], doc_sizes[0], &id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_store_multi(const MunitParameter params[], void* data)
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
        nullptr,
        "rob",
        "shane"
    };
    size_t doc_sizes[] = {
        9, 5, 0, 4, 6 /* null terminator is included */
    };
    yk_id_t ids[5];

    ret = yk_doc_store_multi(dbh, "abcd", 0, 5, (const void* const*)docs, doc_sizes, ids);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    for(unsigned i=0; i < 5; i++) {
        munit_assert_int(i, ==, ids[i]);
    }

    for(unsigned i=0; i < 5; i++) {
        yk_id_t id = (yk_id_t)i;
        char buffer[10];
        size_t bufsize = 10;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer, &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, doc_sizes[i]);
        if(bufsize != 0)
            munit_assert_string_equal(buffer, docs[i]);
    }

    /* erroneous cases */

    /* tries to store nullptr with a non-zero size */
    ret = yk_doc_store_multi(dbh, "abcd", 0, 5, nullptr, doc_sizes, ids);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to store in a collection that does not exist */
    ret = yk_doc_store_multi(dbh, "efgh", 0, 5, (const void* const*)docs, doc_sizes, ids);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_store_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    ret = yk_collection_create(dbh, "abcd", 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    const char* docs =
        "matthieu\0phil\0rob\0shane\0";
    size_t doc_sizes[] = {
        9, 5, 0, 4, 6 /* null terminator is included */
    };
    yk_id_t ids[5];

    ret = yk_doc_store_packed(dbh, "abcd", 0, 5, docs, doc_sizes, ids);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    for(unsigned i=0; i < 5; i++) {
        munit_assert_long(i, ==, ids[i]);
    }

    size_t offset = 0;
    for(unsigned i=0; i < 5; i++) {
        yk_id_t id = (yk_id_t)i;
        char buffer[10];
        size_t bufsize = 10;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer, &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, doc_sizes[i]);
        if(bufsize != 0)
            munit_assert_string_equal(buffer, docs+offset);
        offset += doc_sizes[i];
    }

    /* erroneous cases */

    /* tries to store nullptr with a non-zero size */
    ret = yk_doc_store_packed(dbh, "abcd", 0, 5, nullptr, doc_sizes, ids);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to store in a collection that does not exist */
    ret = yk_doc_store_packed(dbh, "efgh", 0, 5, docs, doc_sizes, ids);
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
    { (char*) "/coll/store", test_coll_store,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/store_multi", test_coll_store_multi,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/store_packed", test_coll_store_packed,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

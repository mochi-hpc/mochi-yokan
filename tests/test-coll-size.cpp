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

static MunitResult test_coll_doc_size(const MunitParameter params[], void* data)
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
        size_t size;
        ret = yk_doc_size(dbh, "abcd", 0, id, &size);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(size, ==, doc_sizes[i]);
    }

    /* erroneous cases */

    size_t size;

    /* tries to get the size using nullptr */
    ret = yk_doc_size(dbh, "abcd", 0, 0, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries get size of a document from a collection that does not exist */
    ret = yk_doc_size(dbh, "efgh", 0, 0, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    /* tries to get size with an invalid id */
    ret = yk_doc_size(dbh, "abcd", 0, 10, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_doc_size_multi(const MunitParameter params[], void* data)
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

    std::vector<size_t> buf_sizes(6);
    std::vector<yk_id_t> ids;
    for(unsigned i=0; i < 6; i++) /* id 6 does not exist */
        ids.push_back(i);

    ret = yk_doc_size_multi(dbh, "abcd", 0, 6, ids.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(unsigned i=0; i < 5; i++) {
        if(i >= 5) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_KEY_NOT_FOUND);
            continue;
        }
        munit_assert_long(buf_sizes[i], ==, doc_sizes[i]);
    }

    /* erroneous cases */

    /* tries to load with nullptr as ids */
    ret = yk_doc_size_multi(dbh, "abcd", 0, 6, nullptr, buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to get size with nullptr as size */
    ret = yk_doc_size_multi(dbh, "abcd", 0, 6, ids.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to get size of doc from a collection that does not exist */
    ret = yk_doc_size_multi(dbh, "efgh", 0, 6, ids.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    for(unsigned i=0; i < 6; i++)
        munit_assert_long(buf_sizes[i], ==, YOKAN_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    /* coll_create */
    { (char*) "/coll/doc_size", test_coll_doc_size,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/doc_size_multi", test_coll_doc_size_multi,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

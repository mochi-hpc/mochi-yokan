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

static MunitResult test_coll_load(const MunitParameter params[], void* data)
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

    char buffer[10];
    size_t bufsize = 10;
    /* tries to load with nullptr for document */
    ret = yk_doc_load(dbh, "abcd", 0, 0, nullptr, &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_load(dbh, "abcd", 0, 0, buffer, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_load(dbh, "efgh", 0, 0, buffer, &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    /* tries to load with not enough memory */
    bufsize = 3;
    ret = yk_doc_load(dbh, "abcd", 0, 0, buffer, &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_BUFFER_SIZE);

    /* tries to load with an invalid id */
    bufsize = 10;
    ret = yk_doc_load(dbh, "abcd", 0, 10, buffer, &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_load_multi(const MunitParameter params[], void* data)
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

    std::vector<std::vector<char>> buffers(6);
    for(auto& v : buffers) v.resize(10);
    std::vector<size_t> buf_sizes(6, 7); /* with 7, "matthieu" won't fit */
    std::vector<void*> buf_ptrs;
    for(auto& v : buffers) buf_ptrs.push_back(v.data());
    std::vector<yk_id_t> ids;
    for(unsigned i=0; i < 6; i++) /* id 6 does not exist */
        ids.push_back(i);

    ret = yk_doc_load_multi(dbh, "abcd", 0, 6, ids.data(),
            buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(unsigned i=0; i < 6; i++) {
        if(i == 5) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_KEY_NOT_FOUND);
            continue;
        }
        if(doc_sizes[i] > 7) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_SIZE_TOO_SMALL);
            continue;
        }
        munit_assert_long(buf_sizes[i], ==, doc_sizes[i]);
        if(buf_sizes[i] != 0)
            munit_assert_string_equal(buffers[i].data(), docs[i]);
    }

    /* erroneous cases */

    /* tries to load with nullptr as ids */
    ret = yk_doc_load_multi(dbh, "abcd", 0, 6, nullptr, buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr for document */
    ret = yk_doc_load_multi(dbh, "abcd", 0, 6, ids.data(), nullptr, buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_load_multi(dbh, "abcd", 0, 6, ids.data(), buf_ptrs.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_load_multi(dbh, "efgh", 0, 6, ids.data(),
            buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_load_packed(const MunitParameter params[], void* data)
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

    std::vector<char> buffer(60);
    std::vector<size_t> sizes(6);
    std::vector<yk_id_t> ids;
    for(unsigned i=0; i < 6; i++) /* id 6 does not exist */
        ids.push_back(i);

    ret = yk_doc_load_packed(dbh, "abcd", 0, 6, ids.data(),
            60, buffer.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    size_t offset = 0;
    for(unsigned i=0; i < 6; i++) {
        if(i == 5) {
            munit_assert_long(sizes[i], ==, YOKAN_KEY_NOT_FOUND);
            continue;
        }
        munit_assert_long(sizes[i], ==, doc_sizes[i]);
        if(sizes[i] != 0)
            munit_assert_string_equal(buffer.data()+offset, docs[i]);
        offset += sizes[i];
    }

    /* erroneous cases */

    /* tries to load with nullptr as ids */
    ret = yk_doc_load_packed(dbh, "abcd", 0, 6, nullptr, 60, buffer.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr for document */
    ret = yk_doc_load_packed(dbh, "abcd", 0, 6, ids.data(), 60, nullptr, sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_load_packed(dbh, "abcd", 0, 6, ids.data(), 60, buffer.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_load_packed(dbh, "efgh", 0, 6, ids.data(),
            60, buffer.data(), sizes.data());
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
    { (char*) "/coll/load", test_coll_load,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/load_multi", test_coll_load_multi,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/load_packed", test_coll_load_packed,
        test_common_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

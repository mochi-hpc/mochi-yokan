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

static void* test_coll_size_context_setup(const MunitParameter params[], void* user_data)
{
    auto context = static_cast<doc_test_context*>(
        doc_test_common_context_setup(params, user_data));

    auto count = context->reference.size();
    std::vector<const void*> ptrs;
    std::vector<size_t>      sizes;

    ptrs.reserve(count);
    sizes.reserve(count);

    for(auto& doc : context->reference) {
        ptrs.push_back(doc.data());
        sizes.push_back(doc.size());
    }

    yk_collection_create(context->dbh, "abcd", 0);

    std::vector<yk_id_t> ids(count);
    yk_doc_store_multi(context->dbh, "abcd", 0, count, ptrs.data(), sizes.data(), ids.data());

    return context;
}

static MunitResult test_coll_doc_size(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    for(unsigned i=0; i < g_num_items; i++) {
        yk_id_t id = (yk_id_t)i;
        size_t size;
        ret = yk_doc_size(dbh, "abcd", 0, id, &size);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        auto& ref = context->reference[i];
        munit_assert_long(size, ==, ref.size());
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
    ret = yk_doc_size(dbh, "abcd", 0, g_num_items+1, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_doc_size_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<size_t> buf_sizes(g_num_items+1);
    std::vector<yk_id_t> ids;
    for(unsigned i=0; i < g_num_items+1; i++) /* id g_num_items does not exist */
        ids.push_back(i);

    ret = yk_doc_size_multi(dbh, "abcd", 0, g_num_items+1, ids.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(unsigned i=0; i < g_num_items+1; i++) {
        if(i == g_num_items) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_KEY_NOT_FOUND);
            continue;
        }
        auto& ref = context->reference[i];
        munit_assert_long(buf_sizes[i], ==, ref.size());
    }

    /* erroneous cases */

    /* tries to load with nullptr as ids */
    ret = yk_doc_size_multi(dbh, "abcd", 0, g_num_items+1, nullptr, buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to get size with nullptr as size */
    ret = yk_doc_size_multi(dbh, "abcd", 0, g_num_items+1, ids.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to get size of doc from a collection that does not exist */
    ret = yk_doc_size_multi(dbh, "efgh", 0, g_num_items+1, ids.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    for(unsigned i=0; i < g_num_items+1; i++)
        munit_assert_long(buf_sizes[i], ==, YOKAN_KEY_NOT_FOUND);

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
    { (char*) "/coll/doc_size", test_coll_doc_size,
        test_coll_size_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/doc_size_multi", test_coll_doc_size_multi,
        test_coll_size_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

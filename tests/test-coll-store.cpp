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

static void* test_coll_store_context_setup(const MunitParameter params[], void* user_data)
{
    auto context = static_cast<doc_test_context*>(
        doc_test_common_context_setup(params, user_data));

    yk_collection_create(context->dbh, "abcd", 0);

    return context;
}

static MunitResult test_coll_store(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id;
        auto& doc = context->reference[i];
        ret = yk_doc_store(dbh, "abcd", 0, doc.data(), doc.size(), &id);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(id, ==, (long)i);
    }

    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        std::vector<char> buffer(g_max_val_size);
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, ref.size());
        munit_assert_memory_equal(ref.size(), buffer.data(), ref.data());
    }

    /* erroneous cases */

    /* tries to store nullptr with a non-zero size */
    yk_id_t id;
    ret = yk_doc_store(dbh, "abcd", 0, nullptr, 10, &id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to store in a collection that does not exist */
    ret = yk_doc_store(dbh, "efgh", 0, "somedoc", 7, &id);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_store_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = context->reference.size();
    std::vector<yk_id_t> ids(count);

    std::vector<const void*> ptrs(count);
    std::vector<size_t> sizes(count);
    for(unsigned i=0; i < count; i++) {
        ptrs[i] = context->reference[i].data();
        sizes[i] = context->reference[i].size();
    }

    ret = yk_doc_store_multi(dbh, "abcd", 0, g_num_items,
            (const void* const*)ptrs.data(), sizes.data(), ids.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    for(unsigned i=0; i < count; i++) {
        munit_assert_int(i, ==, ids[i]);
    }

    for(unsigned i=0; i < count; i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        std::vector<char> buffer(g_max_val_size);
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, ref.size());
        munit_assert_memory_equal(ref.size(), buffer.data(), ref.data());
    }

    /* erroneous cases */

    /* tries to store nullptr with a non-zero size */
    ret = yk_doc_store_multi(dbh, "abcd", 0, count, nullptr, sizes.data(), ids.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to store in a collection that does not exist */
    ret = yk_doc_store_multi(dbh, "efgh", 0, count, ptrs.data(), sizes.data(), ids.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_store_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = context->reference.size();

    std::string packed;
    std::vector<size_t> sizes;
    packed.reserve(g_num_items*g_max_val_size);
    sizes.reserve(g_num_items);
    for(auto& s : context->reference) {
        packed += s;
        sizes.push_back(s.size());
    }
    std::vector<yk_id_t> ids(count);

    ret = yk_doc_store_packed(dbh, "abcd", 0, g_num_items, packed.data(), sizes.data(), ids.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    for(unsigned i=0; i < g_num_items; i++) {
        munit_assert_long(i, ==, ids[i]);
    }

    for(unsigned i=0; i < count; i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        std::vector<char> buffer(g_max_val_size);
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, ref.size());
        munit_assert_memory_equal(ref.size(), buffer.data(), ref.data());
    }

    /* erroneous cases */

    /* tries to store nullptr with a non-zero size */
    ret = yk_doc_store_packed(dbh, "abcd", 0, g_num_items,
            nullptr, sizes.data(), ids.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to store in a collection that does not exist */
    ret = yk_doc_store_packed(dbh, "efgh", 0, g_num_items,
            packed.data(), sizes.data(), ids.data());
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
    { (char*) "/coll/store", test_coll_store,
        test_coll_store_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/store_multi", test_coll_store_multi,
        test_coll_store_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/store_packed", test_coll_store_packed,
        test_coll_store_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

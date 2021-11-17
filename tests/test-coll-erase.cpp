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

static void* test_coll_erase_context_setup(const MunitParameter params[], void* user_data)
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

static MunitResult test_coll_erase(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    for(yk_id_t i = 0; i < context->reference.size(); i++) {
        if(i % 3 != 0) continue;
        ret = yk_doc_erase(dbh, "abcd", 0, i);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
    }

    std::vector<char> buffer(g_max_val_size);
    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        if(i % 3 == 0) {
            munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);
        } else {
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_long(bufsize, ==, ref.size());
            munit_assert_memory_equal(bufsize, buffer.data(), ref.data());
        }
    }

    /* tries to erase an id outside of the collection */
    ret = yk_doc_erase(dbh, "abcd", 0, g_num_items+10);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    /* tries to erase from an invalid collection */
    ret = yk_doc_erase(dbh, "efgh", 0, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_erase_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<yk_id_t> ids_to_erase;
    for(yk_id_t i = 0; i < context->reference.size(); i++) {
        if(i % 3 != 0) continue;
        ids_to_erase.push_back(i);
    }

    ret = yk_doc_erase_multi(dbh, "abcd", 0, ids_to_erase.size(), ids_to_erase.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    std::vector<char> buffer(g_max_val_size);
    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        if(i % 3 == 0) {
            munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);
        } else {
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_long(bufsize, ==, ref.size());
            munit_assert_memory_equal(bufsize, buffer.data(), ref.data());
        }
    }

    /* tries to pass null pointer */
    ret = yk_doc_erase_multi(dbh, "abcd", 0, ids_to_erase.size(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tried to erase from an invalid collection */
    ret = yk_doc_erase_multi(dbh, "efgh", 0, ids_to_erase.size(), ids_to_erase.data());
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
    { (char*) "/coll/erase", test_coll_erase,
        test_coll_erase_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/erase_multi", test_coll_erase_multi,
        test_coll_erase_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

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

static void* test_coll_update_context_setup(const MunitParameter params[], void* user_data)
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

static MunitResult test_coll_update(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    for(yk_id_t i = 0; i < context->reference.size(); i++) {
        if(i % 3 != 0) continue;
        std::string doc;
        size_t size;
        if(g_min_val_size == 0 && g_max_val_size == 0) {
            size = 0;
        } else {
            size = (i+1)%7 == 0 ? 0 : munit_rand_int_range(g_min_val_size, g_max_val_size);
        }
        doc.resize(size);
        for(int j = 0; j < (int)size; j++) {
            char c = (char)munit_rand_int_range(33, 126);
            doc[j] = c;
        }
        ret = yk_doc_update(dbh, "abcd", 0, i, doc.data(), size);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        context->reference[i] = std::move(doc);
    }

    std::vector<char> buffer(g_max_val_size);
    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, ref.size());
        munit_assert_memory_equal(bufsize, buffer.data(), ref.data());
    }

    /* tries to update an id outside of the collection */
    ret = yk_doc_update(dbh, "abcd", 0, g_num_items+10, "something", 9);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ID);

    /* tries to update from an invalid collection */
    ret = yk_doc_update(dbh, "efgh", 0, 0, "something", 9);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    /* tries to update using nullptr for the document content */
    ret = yk_doc_update(dbh, "efgh", 0, 0, nullptr, 9);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_coll_update_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<void*> ptrs;
    std::vector<size_t> sizes;
    std::vector<yk_id_t> ids;

    for(yk_id_t i = 0; i < context->reference.size(); i++) {
        if(i % 3 != 0) continue;
        std::string doc;
        size_t size;
        if(g_min_val_size == 0 && g_max_val_size == 0) {
            size = 0;
        } else {
            size = (i+1)%7 == 0 ? 0 : munit_rand_int_range(g_min_val_size, g_max_val_size);
        }
        doc.resize(size);
        for(int j = 0; j < (int)size; j++) {
            char c = (char)munit_rand_int_range(33, 126);
            doc[j] = c;
        }
        context->reference[i] = std::move(doc);
        ptrs.push_back(const_cast<char*>(context->reference[i].data()));
        sizes.push_back(context->reference[i].size());
        ids.push_back(i);
    }

    ret = yk_doc_update_multi(dbh, "abcd", 0, ids.size(), ids.data(),
            ptrs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    std::vector<char> buffer(g_max_val_size);
    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, ref.size());
        munit_assert_memory_equal(bufsize, buffer.data(), ref.data());
    }

    /* tries to update from an invalid collection */
    ret = yk_doc_update_multi(dbh, "efgh", 0, ids.size(), ids.data(), ptrs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    /* tries to update using nullptr for the ids */
    ret = yk_doc_update_multi(dbh, "abcd", 0, ids.size(), nullptr, ptrs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to update using nullptr for the documents content */
    ret = yk_doc_update_multi(dbh, "abcd", 0, ids.size(), ids.data(), nullptr, sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to update using nullptr for the sizes */
    ret = yk_doc_update_multi(dbh, "abcd", 0, ids.size(), ids.data(), ptrs.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to update an id outside of the collection */
    ids[0] = g_num_items+10;
    ret = yk_doc_update_multi(dbh, "abcd", 0, ids.size(), ids.data(),
            ptrs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ID);

    return MUNIT_OK;
}

static MunitResult test_coll_update_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::string packed_docs;
    std::vector<size_t> sizes;
    std::vector<yk_id_t> ids;

    for(yk_id_t i = 0; i < context->reference.size(); i++) {
        if(i % 3 != 0) continue;
        std::string doc;
        size_t size;
        if(g_min_val_size == 0 && g_max_val_size == 0) {
            size = 0;
        } else {
            size = (i+1)%7 == 0 ? 0 : munit_rand_int_range(g_min_val_size, g_max_val_size);
        }
        doc.resize(size);
        for(int j = 0; j < (int)size; j++) {
            char c = (char)munit_rand_int_range(33, 126);
            doc[j] = c;
        }
        packed_docs += doc;
        sizes.push_back(doc.size());
        context->reference[i] = std::move(doc);
        ids.push_back(i);
    }

    ret = yk_doc_update_packed(dbh, "abcd", 0, ids.size(), ids.data(),
            packed_docs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    std::vector<char> buffer(g_max_val_size);
    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", 0, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, ref.size());
        munit_assert_memory_equal(bufsize, buffer.data(), ref.data());
    }

    /* tries to update from an invalid collection */
    ret = yk_doc_update_packed(dbh, "efgh", 0, ids.size(), ids.data(), packed_docs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    /* tries to update using nullptr for the ids */
    ret = yk_doc_update_packed(dbh, "abcd", 0, ids.size(), nullptr, packed_docs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to update using nullptr for the documents content */
    ret = yk_doc_update_packed(dbh, "abcd", 0, ids.size(), ids.data(), nullptr, sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to update using nullptr for the sizes */
    ret = yk_doc_update_packed(dbh, "abcd", 0, ids.size(), ids.data(), packed_docs.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to update an id outside of the collection */
    ids[0] = g_num_items+10;
    ret = yk_doc_update_packed(dbh, "abcd", 0, ids.size(), ids.data(),
            packed_docs.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ID);

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
    { (char*) "/coll/update", test_coll_update,
        test_coll_update_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/update_multi", test_coll_update_multi,
        test_coll_update_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/update_packed", test_coll_update_packed,
        test_coll_update_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

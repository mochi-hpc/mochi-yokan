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

static size_t g_items_per_op = 6;

static void* test_coll_iter_context_setup(const MunitParameter params[], void* user_data)
{
    auto context = static_cast<doc_test_context*>(
        doc_test_common_context_setup(params, user_data));

    auto count = context->reference.size();
    std::vector<const void*> ptrs;
    std::vector<size_t>      sizes;

    const char* items_per_op_str = munit_parameters_get(params, "items-per-op");
    g_items_per_op = items_per_op_str ? atol(items_per_op_str) : 6;

    ptrs.reserve(count);
    sizes.reserve(count);

    for(auto& doc : context->reference) {
        ptrs.push_back(doc.data());
        sizes.push_back(doc.size());
    }

    yk_collection_create(context->dbh, "abcd", 0);

    std::vector<yk_id_t> ids(count);
    yk_doc_store_multi(context->dbh, "abcd", context->mode,
                       count, ptrs.data(), sizes.data(), ids.data());

    return context;
}

static MunitResult test_coll_iter(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    yk_id_t start_id = 0;

    struct doc_iter_context {
        std::vector<yk_id_t>     recv_ids;
        std::vector<std::string> recv_docs;
    };

    doc_iter_context result;

    auto func = [](void* u, size_t i, yk_id_t id, const void* doc, size_t docsize) -> yk_return_t {
        (void)i;
        auto result = static_cast<doc_iter_context*>(u);
        result->recv_ids.push_back(id);
        result->recv_docs.emplace_back((const char*)doc, docsize);
        return YOKAN_SUCCESS;
    };

    yk_doc_iter_options_t options;
    options.batch_size = 0;
    options.pool = ABT_POOL_NULL;
    size_t i = 0;
    while(i < g_num_items) {
        ret = yk_doc_iter(dbh, "abcd", context->mode,
                          start_id, nullptr, 0, g_items_per_op,
                          func, (void*)&result, &options);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        i += g_items_per_op;
        start_id += g_items_per_op;
    }

    munit_assert_size(result.recv_ids.size(), ==, context->reference.size());

    for(uint64_t i=0; i < result.recv_ids.size(); i++) {
        munit_assert_uint64(result.recv_ids[i], ==, i);
        munit_assert_size(result.recv_docs[i].size(), ==, context->reference[i].size());
        munit_assert_memory_equal(result.recv_docs[i].size(), result.recv_docs[i].data(), context->reference[i].data());
    }

    /* erroneous cases */

    /* tries to load with nullptr as callback */
    ret = yk_doc_iter(dbh, "abcd", context->mode, 0, nullptr, 0, g_items_per_op,
            nullptr, nullptr, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_iter(dbh, "efgh", context->mode, 0, nullptr, 0, g_items_per_op,
            func, (void*)&result, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_iter_lua(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    yk_id_t start_id = 0;

    const char* lua_code =
        "return (__id__ % 3 == 0) or ((string.len(__doc__) > 0) and (__doc__:byte(1) < 100))";
    size_t code_size = strlen(lua_code);

    struct doc_iter_context {
        std::vector<yk_id_t>     recv_ids;
        std::vector<std::string> recv_docs;
    };

    doc_iter_context result;

    auto func = [](void* u, size_t i, yk_id_t id, const void* doc, size_t docsize) -> yk_return_t {
        (void)i;
        auto result = static_cast<doc_iter_context*>(u);
        result->recv_ids.push_back(id);
        result->recv_docs.emplace_back((const char*)doc, docsize);
        return YOKAN_SUCCESS;
    };

    yk_doc_iter_options_t options;
    options.batch_size = 0;
    options.pool = ABT_POOL_NULL;
    size_t i = 0;
    while(i < g_num_items) {
        ret = yk_doc_iter(dbh, "abcd", context->mode|YOKAN_MODE_LUA_FILTER,
                          start_id, lua_code, code_size, g_items_per_op,
                          func, (void*)&result, &options);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        i += g_items_per_op;
        start_id += g_items_per_op;
    }

    for(uint64_t i=0; i < result.recv_ids.size(); i++) {
        auto id = result.recv_ids[i];
        munit_assert_uint64(id, < , context->reference.size());
        munit_assert_true((id % 3 == 0) || (result.recv_docs[i].size() > 0 && result.recv_docs[i][0] < 100));
        munit_assert_size(result.recv_docs[i].size(), ==, context->reference[id].size());
        munit_assert_memory_equal(result.recv_docs[i].size(), result.recv_docs[i].data(), context->reference[id].data());
    }

    return MUNIT_OK;
}

static MunitResult test_coll_iter_custom_filter(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    yk_id_t start_id = 0;

    std::string filter = "libcustom-filters.so:custom_doc:";

    struct doc_iter_context {
        std::vector<yk_id_t>     recv_ids;
        std::vector<std::string> recv_docs;
    };

    doc_iter_context result;

    auto func = [](void* u, size_t i, yk_id_t id, const void* doc, size_t docsize) -> yk_return_t {
        (void)i;
        auto result = static_cast<doc_iter_context*>(u);
        result->recv_ids.push_back(id);
        result->recv_docs.emplace_back((const char*)doc, docsize);
        return YOKAN_SUCCESS;
    };

    yk_doc_iter_options_t options;
    options.batch_size = 0;
    options.pool = ABT_POOL_NULL;
    while(true) {
        size_t i = result.recv_ids.size();
        ret = yk_doc_iter(dbh, "abcd", context->mode|YOKAN_MODE_LIB_FILTER,
                          start_id, filter.data(), filter.size(), g_items_per_op,
                          func, (void*)&result, &options);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        start_id = result.recv_ids.back()+1;
        if(result.recv_ids.size() == i) break; // no change since last call
    }

    for(uint64_t i=0; i < result.recv_ids.size(); i++) {
        auto id = result.recv_ids[i];
        munit_assert_uint64(id, < , context->reference.size());
        munit_assert_long(id, ==, 2*i);
        auto& ref = context->reference[id];
        munit_assert_size(result.recv_docs[i].size(), ==, ref.size());
        munit_assert_memory_equal(result.recv_docs[i].size(), result.recv_docs[i].data(), ref.data());
    }

    return MUNIT_OK;
}

static char* no_rdma_params[] = {
    (char*)"true", (char*)"false", (char*)NULL };

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"no-rdma", (char**)no_rdma_params },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-items", NULL },
  { (char*)"items-per-op", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    /* coll_create */
    { (char*) "/coll/iter", test_coll_iter,
        test_coll_iter_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/iter/lua", test_coll_iter_lua,
        test_coll_iter_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/iter/custom_filter", test_coll_iter_custom_filter,
        test_coll_iter_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

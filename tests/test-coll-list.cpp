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

static void* test_coll_list_context_setup(const MunitParameter params[], void* user_data)
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

static MunitResult test_coll_list(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = g_num_items + g_items_per_op - (g_num_items % g_items_per_op);

    std::vector<std::vector<char>> buffers(count);
    for(auto& v : buffers) v.resize(g_max_val_size);

    std::vector<void*> buf_ptrs;
    std::vector<size_t> buf_sizes;
    buf_ptrs.reserve(g_num_items);
    buf_sizes.reserve(g_num_items);
    unsigned i=0;
    for(auto& v : buffers) {
        buf_ptrs.push_back(v.data());
        if(i % 8 == 0) {
            buf_sizes.push_back(0);
        } else {
            buf_sizes.push_back(g_max_val_size);
        }
        i += 1;
    }
    std::vector<yk_id_t> ids(count);

    yk_id_t start_id = 0;

    i = 0;
    while(i < g_num_items) {
        ret = yk_doc_list(dbh, "abcd", YOKAN_MODE_INCLUSIVE|context->mode,
                          start_id, nullptr, 0,
                          g_items_per_op, ids.data()+i,
                          buf_ptrs.data()+i, buf_sizes.data()+i);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        i += g_items_per_op;
        start_id += g_items_per_op;
    }

    for(unsigned i=0; i < count; i++) {
        if(i >= g_num_items) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_NO_MORE_DOCS);
            continue;
        }
        auto& ref = context->reference[i];
        if(i % 8 == 0 && ref.size() != 0) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_SIZE_TOO_SMALL);
            continue;
        }
        munit_assert_long(buf_sizes[i], ==, ref.size());
        if(buf_sizes[i] != 0)
            munit_assert_memory_equal(ref.size(), buffers[i].data(), ref.data());
    }

    /* erroneous cases */

    /* tries to load with nullptr as ids */
    ret = yk_doc_list(dbh, "abcd", context->mode, 0, nullptr, 0, g_items_per_op,
            nullptr, buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr for document */
    ret = yk_doc_list(dbh, "abcd", context->mode, 0, nullptr, 0, g_items_per_op,
            ids.data(), nullptr, buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_list(dbh, "abcd", context->mode, 0, nullptr, 0, g_items_per_op,
            ids.data(), buf_ptrs.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_list(dbh, "efgh", context->mode, 0, nullptr, 0, g_items_per_op,
            ids.data(), buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_list_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = g_num_items + g_items_per_op - (g_num_items % g_items_per_op);

    std::vector<char> buffer(count*g_max_val_size);
    std::vector<size_t> buf_sizes(count);
    std::vector<yk_id_t> ids(count);

    yk_id_t start_id = 0;

    unsigned i = 0;
    size_t doc_offset = 0;
    while(i < g_num_items) {
        ret = yk_doc_list_packed(dbh, "abcd", YOKAN_MODE_INCLUSIVE|context->mode,
                                 start_id, nullptr, 0,
                                 g_items_per_op, ids.data()+i,
                                 buffer.size()-doc_offset,
                                 buffer.data()+doc_offset, buf_sizes.data()+i);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        for(unsigned j = 0; j < g_items_per_op; j++) {
            if(buf_sizes[i+j] == YOKAN_NO_MORE_DOCS) {
                break;
            }
            else {
                doc_offset += buf_sizes[i+j];
            }
        }
        i += g_items_per_op;
        start_id += g_items_per_op;
    }

    doc_offset = 0;
    for(unsigned i=0; i < count; i++) {
        if(i >= g_num_items) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_NO_MORE_KEYS);
            continue;
        }
        auto& ref = context->reference[i];
        munit_assert_long(buf_sizes[i], ==, ref.size());
        munit_assert_memory_equal(ref.size(), buffer.data()+doc_offset, ref.data());
        doc_offset += buf_sizes[i];
    }

    /* erroneous cases */

    /* tries to load with nullptr as ids */
    ret = yk_doc_list_packed(dbh, "abcd", context->mode, 0, nullptr, 0, g_items_per_op,
            nullptr, buffer.size(), buffer.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr for document */
    ret = yk_doc_list_packed(dbh, "abcd", context->mode, 0, nullptr, 0, g_items_per_op,
            ids.data(), buffer.size(), nullptr, buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_list_packed(dbh, "abcd", context->mode, 0, nullptr, 0, g_items_per_op,
            ids.data(), buffer.size(), buffer.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_list_packed(dbh, "efgh", context->mode, 0, nullptr, 0, g_items_per_op,
            ids.data(), buffer.size(), buffer.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_list_lua(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<std::vector<char>> buffers(g_items_per_op);
    for(auto& v : buffers) v.resize(g_max_val_size);

    std::vector<void*> buf_ptrs;
    std::vector<size_t> buf_sizes;
    buf_ptrs.reserve(g_items_per_op);
    buf_sizes.reserve(g_items_per_op);
    for(auto& v : buffers) {
        buf_ptrs.push_back(v.data());
        buf_sizes.push_back(g_max_val_size);
    }
    std::vector<yk_id_t> ids(g_items_per_op);

    yk_id_t start_id = 0;

    const char* lua_code =
        "return (__id__ % 3 == 0) or ((string.len(__doc__) > 0) and (__doc__:byte(1) < 100))";
    size_t code_size = strlen(lua_code);

    int32_t mode = YOKAN_MODE_INCLUSIVE|YOKAN_MODE_LUA_FILTER|context->mode;
    while(start_id != YOKAN_NO_MORE_DOCS) {
        ret = yk_doc_list(dbh, "abcd", mode, start_id,
                          lua_code, code_size,
                          g_items_per_op, ids.data(),
                          buf_ptrs.data(), buf_sizes.data());
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        for(unsigned i=0; i < g_items_per_op; i++) {
            if(ids[i] == YOKAN_NO_MORE_DOCS) {
                start_id = YOKAN_NO_MORE_DOCS;
                break;
            }
            auto& ref = context->reference[ids[i]];
            munit_assert_long(buf_sizes[i], ==, ref.size());
            munit_assert_memory_equal(ref.size(), buffers[i].data(), ref.data());
            munit_assert_true((ids[i] % 3 == 0) || (buf_sizes[i] > 0 && ref[0] < 100));
            start_id = ids[i]+1;
            buf_sizes[i] = g_max_val_size;
        }
    }

    return MUNIT_OK;
}

static MunitResult test_coll_list_packed_lua(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<char> buffer(g_items_per_op*g_max_val_size);
    std::vector<size_t> buf_sizes(g_items_per_op);
    std::vector<yk_id_t> ids(g_items_per_op);

    yk_id_t start_id = 0;

    const char* lua_code =
        "return (__id__ % 3 == 0) or ((string.len(__doc__) > 0) and (__doc__:byte(1) < 100))";
    size_t code_size = strlen(lua_code);

    int32_t mode = YOKAN_MODE_INCLUSIVE|YOKAN_MODE_LUA_FILTER|YOKAN_MODE_FILTER_VALUE;
    mode |= context->mode;
    while(start_id != YOKAN_NO_MORE_DOCS) {
        ret = yk_doc_list_packed(dbh, "abcd", mode, start_id,
                          lua_code, code_size,
                          g_items_per_op, ids.data(),
                          buffer.size(), buffer.data(), buf_sizes.data());
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        size_t offset = 0;
        for(unsigned i=0; i < g_items_per_op; i++) {
            if(ids[i] == YOKAN_NO_MORE_DOCS) {
                start_id = YOKAN_NO_MORE_DOCS;
                break;
            }
            auto& ref = context->reference[ids[i]];
            munit_assert_long(buf_sizes[i], ==, ref.size());
            munit_assert_memory_equal(ref.size(), buffer.data()+offset, ref.data());
            munit_assert_true((ids[i] % 3 == 0) || (buf_sizes[i] > 0 && ref[0] < 100));
            start_id = ids[i]+1;
            offset += buf_sizes[i];
            buf_sizes[i] = g_max_val_size;
        }
    }

    return MUNIT_OK;
}

static MunitResult test_coll_list_custom_filter(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = g_num_items;

    std::vector<std::vector<char>> buffers(count);
    for(auto& v : buffers) v.resize(g_max_val_size);

    std::vector<void*> buf_ptrs;
    std::vector<size_t> buf_sizes;
    buf_ptrs.reserve(count);
    buf_sizes.reserve(count);
    for(auto& v : buffers) {
        buf_ptrs.push_back(v.data());
        buf_sizes.push_back(g_max_val_size);
    }
    std::vector<yk_id_t> ids(count);

    yk_id_t start_id = 0;
    std::string filter = "libcustom-filters.so:custom_doc:";

    ret = yk_doc_list(dbh, "abcd", YOKAN_MODE_INCLUSIVE|YOKAN_MODE_LIB_FILTER|context->mode, start_id,
            filter.data(), filter.size(), count, ids.data(), buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(unsigned i=0; i < count; i++) {
        if(2*i >= g_num_items) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_NO_MORE_DOCS);
            continue;;
        }
        munit_assert_long(ids[i], ==, 2*i);
        auto& ref = context->reference[2*i];
        munit_assert_long(buf_sizes[i], ==, ref.size());
        munit_assert_memory_equal(ref.size(), buffers[i].data(), ref.data());
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
    { (char*) "/coll/list", test_coll_list,
        test_coll_list_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/list/lua", test_coll_list_lua,
        test_coll_list_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/list_packed", test_coll_list_packed,
        test_coll_list_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/list_packed/lua", test_coll_list_packed_lua,
        test_coll_list_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/list/custom_filter", test_coll_list_custom_filter,
        test_coll_list_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

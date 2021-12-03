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

static void* test_coll_load_context_setup(const MunitParameter params[], void* user_data)
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
    yk_doc_store_multi(context->dbh, "abcd", context->mode,
                       count, ptrs.data(), sizes.data(), ids.data());

    return context;
}

static MunitResult test_coll_load(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<char> buffer(g_max_val_size);
    for(unsigned i=0; i < context->reference.size(); i++) {
        yk_id_t id = (yk_id_t)i;
        auto& ref = context->reference[i];
        size_t bufsize = g_max_val_size;
        ret = yk_doc_load(dbh, "abcd", context->mode, id, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_long(bufsize, ==, ref.size());
        if(bufsize != 0)
            munit_assert_memory_equal(bufsize, buffer.data(), ref.data());
    }

    /* erroneous cases */

    size_t bufsize = g_max_val_size;
    /* tries to load with nullptr for document */
    ret = yk_doc_load(dbh, "abcd", context->mode, 0, nullptr, &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_load(dbh, "abcd", context->mode, 0, buffer.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_load(dbh, "efgh",  context->mode, 0, buffer.data(), &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    /* tries to load with not enough memory */
    for(size_t i = 0; i < context->reference.size(); i++) {
        auto& ref = context->reference[i];
        if(ref.size() == 0) continue;
        bufsize = ref.size() - 1;
        ret = yk_doc_load(dbh, "abcd",  context->mode, i, buffer.data(), &bufsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_ERR_BUFFER_SIZE);
        break;
    }

    /* tries to load with an invalid id */
    bufsize = g_max_val_size;
    ret = yk_doc_load(dbh, "abcd",  context->mode, g_num_items+10, buffer.data(), &bufsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_load_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<std::vector<char>> buffers(g_num_items+1);
    for(auto& v : buffers) v.resize(g_max_val_size);

    std::vector<void*> buf_ptrs;
    std::vector<size_t> buf_sizes;
    buf_ptrs.reserve(g_num_items+1);
    buf_sizes.reserve(g_num_items+1);
    unsigned i=0;
    for(auto& v : buffers) {
        buf_ptrs.push_back(v.data());
        if(i % 8 == 0 && i < g_num_items) {
            buf_sizes.push_back(0);
        } else {
            buf_sizes.push_back(g_max_val_size);
        }
        i += 1;
    }
    std::vector<yk_id_t> ids;
    ids.reserve(g_num_items+1);
    for(unsigned i=0; i < g_num_items+1; i++) /* last id won't exist */
        ids.push_back(i);

    ret = yk_doc_load_multi(dbh, "abcd",  context->mode, g_num_items+1, ids.data(),
            buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(unsigned i=0; i < g_num_items+1; i++) {
        if(i == g_num_items) {
            munit_assert_long(buf_sizes[i], ==, YOKAN_KEY_NOT_FOUND);
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
    ret = yk_doc_load_multi(dbh, "abcd",  context->mode, 6, nullptr, buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr for document */
    ret = yk_doc_load_multi(dbh, "abcd",  context->mode, 6, ids.data(), nullptr, buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_load_multi(dbh, "abcd",  context->mode, 6, ids.data(), buf_ptrs.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_load_multi(dbh, "efgh",  context->mode, 6, ids.data(),
            buf_ptrs.data(), buf_sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}

static MunitResult test_coll_load_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    std::vector<char> buffer(((g_num_items+1)*g_max_val_size)/3); /* buffer may be too small */
    std::vector<size_t> sizes(g_num_items+1);
    std::vector<yk_id_t> ids;
    ids.push_back(g_num_items); /* id that does not exist */
    for(unsigned i=0; i < g_num_items; i++)
        ids.push_back(i);

    ret = yk_doc_load_packed(dbh, "abcd",  context->mode, g_num_items+1, ids.data(),
            buffer.size(), buffer.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    size_t offset = 0;
    for(unsigned i=0; i < g_num_items+1; i++) {
        if(i == 0) {
            munit_assert_long(sizes[i], ==, YOKAN_KEY_NOT_FOUND);
            continue;
        }
        auto& ref = context->reference[ids[i]];
        if(ref.size() != 0 && ref.size() + offset > buffer.size()) {
            munit_assert_long(sizes[i], ==, YOKAN_SIZE_TOO_SMALL);
            for(; i < g_num_items+1; i++) {
                munit_assert_long(sizes[i], ==, YOKAN_SIZE_TOO_SMALL);
            }
            break;
        }
        munit_assert_long(sizes[i], ==, ref.size());
        if(sizes[i] != 0)
            munit_assert_memory_equal(ref.size(), buffer.data()+offset, ref.data());
        offset += sizes[i];
    }

    /* erroneous cases */

    /* tries to load with nullptr as ids */
    ret = yk_doc_load_packed(dbh, "abcd",  context->mode, g_num_items+1, nullptr,
            buffer.size(), buffer.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr for document */
    ret = yk_doc_load_packed(dbh, "abcd",  context->mode, g_num_items+1, ids.data(),
            buffer.size(), nullptr, sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load with nullptr as size */
    ret = yk_doc_load_packed(dbh, "abcd",  context->mode, g_num_items+1, ids.data(),
            buffer.size(), buffer.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* tries to load from a collection that does not exist */
    ret = yk_doc_load_packed(dbh, "efgh",  context->mode, 6, ids.data(),
            buffer.size(), buffer.data(), sizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);

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
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    /* coll_create */
    { (char*) "/coll/load", test_coll_load,
        test_coll_load_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/load_multi", test_coll_load_multi,
        test_coll_load_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/coll/load_packed", test_coll_load_packed,
        test_coll_load_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

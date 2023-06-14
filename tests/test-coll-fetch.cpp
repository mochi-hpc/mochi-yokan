/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-coll-common-setup.hpp"
#include <yokan/collection.h>
#include <functional>
#include <numeric>
#include <vector>
#include <iostream>
#include <array>

yk_return_t dummy(void* uargs, size_t i, yk_id_t id,
                  const void* doc, size_t doc_size) {
        (void)uargs;
        (void)i;
        (void)id;
        (void)doc;
        (void)doc_size;
        return YOKAN_SUCCESS;
}

static void* test_doc_fetch_context_setup(const MunitParameter params[], void* user_data)
{
    auto context = static_cast<doc_test_context*>(
        doc_test_common_context_setup(params, user_data));

    auto count = context->reference.size();
    std::vector<const void*> ptrs;
    std::vector<size_t>      sizes;

    ptrs.reserve(count);
    sizes.reserve(count);

    for(auto& p : context->reference) {
        auto doc      = p.data();
        auto doc_size = p.size();
        ptrs.push_back(doc);
        sizes.push_back(doc_size);
    }

    yk_collection_create(context->dbh, "abcd", 0);

    std::vector<yk_id_t> ids(count);

    yk_doc_store_multi(context->dbh, "abcd", context->mode,
                       count, ptrs.data(), sizes.data(), ids.data());
    return context;
}

/**
 * @brief Check that we can fetch documents from the reference vector.
 */
static MunitResult test_doc_fetch(const MunitParameter params[], void* data)
{
    (void)data;
    (void)params;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    yk_id_t id = 0;
    for(auto& p : context->reference) {
        auto func = [](void* uargs, size_t i, yk_id_t id,
                       const void* data, size_t size) {
            (void)i;
            (void)id;
            auto ref = (std::string*)uargs;
            munit_assert_int(size, ==, ref->size());
            munit_assert_memory_equal(size, ref->data(), data);
            return YOKAN_SUCCESS;
        };
        ret = yk_doc_fetch(dbh, "abcd", context->mode, id, func, (void*)&p);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);

        id += 1;
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we correctely detect that an id does not exists
 */
static MunitResult test_doc_fetch_id_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    yk_id_t invalid_id = context->reference.size() + 123;

    auto func = [](void* uargs, size_t i, yk_id_t id,
                   const void* data, size_t size) {
        (void)i;
        (void)id;
        (void)data;
        auto size_ptr = (size_t*)uargs;
        *size_ptr = size;
        return YOKAN_SUCCESS;
    };

    size_t size = 0;
    ret = yk_doc_fetch(dbh, "abcd", context->mode, invalid_id, func, &size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(size, ==, YOKAN_KEY_NOT_FOUND);

    return MUNIT_OK;
}

/**
 * @brief Check that we can fetch the documents from the
 * reference vector using doc_fetch_multi, and that doc_fetch_multi also accepts
 * a count of 0.
 */
static MunitResult test_doc_fetch_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_doc_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();

    struct func_args {
        std::vector<yk_id_t>     recv_ids;
        std::vector<std::string> recv_values;
    };

    std::vector<yk_id_t> ids;

    unsigned i = 0;
    for(yk_id_t i = 0; i < context->reference.size(); i++)
        ids.push_back(i);

    auto func = [](void* uargs, size_t i, yk_id_t id,
                   const void* data, size_t size) {
        auto args = (func_args*)uargs;
        munit_assert_int(i, ==, args->recv_ids.size());
        args->recv_ids.emplace_back(id);
        args->recv_values.emplace_back((const char*)data, size);
        return YOKAN_SUCCESS;
    };

    func_args args;
    ret = yk_doc_fetch_multi(dbh, "abcd", context->mode, count,
                             ids.data(), func, &args, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    munit_assert_size(args.recv_values.size(), ==, context->reference.size());
    i = 0;
    for(auto& p : context->reference) {
        auto& val   = args.recv_values[i];
        munit_assert_long(val.size(), ==, p.size());
        munit_assert_memory_equal(val.size(), val.data(), p.data());
        i += 1;
    }

    // check with all NULL

    ret = yk_doc_fetch_multi(dbh, "abcd", context->mode, 0, nullptr, func, nullptr, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can fetch the documents from the
 * reference vector using doc_fetch_multi, and that if an id is not found
 * the document size is properly set to YOKAN_KEY_NOT_FOUND.
 */
static MunitResult test_doc_fetch_multi_id_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct doc_test_context* context = (struct doc_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_doc_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();
    std::vector<yk_id_t>     ids(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 3 == 0)
            ids[i] = context->reference.size() + 123 + i;
        else
            ids[i] = i;
        i += 1;
        (void)p;
    }

    struct func_args {
        std::vector<yk_id_t>     recv_ids;
        std::vector<std::string> recv_vals;
        std::vector<size_t>      recv_valsizes;
    };

    auto func = [](void* uargs, size_t i, yk_id_t id,
                   const void* vdata, size_t vsize) {
        auto args = (func_args*)uargs;
        munit_assert_int(i, ==, args->recv_ids.size());
        args->recv_ids.emplace_back(id);
        args->recv_valsizes.push_back(vsize);
        if(vsize <= YOKAN_LAST_VALID_SIZE)
            args->recv_vals.emplace_back((const char*)vdata, vsize);
        else
            args->recv_vals.emplace_back();
        return YOKAN_SUCCESS;
    };

    func_args args;

    ret = yk_doc_fetch_multi(dbh, "abcd", context->mode, count,
                             ids.data(), func, &args, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    munit_assert_size(args.recv_vals.size(), ==, context->reference.size());
    i = 0;
    for(auto& p : context->reference) {
        auto& val  = args.recv_vals[i];
        auto vsize = args.recv_valsizes[i];
        if(i % 3 == 0) {
            munit_assert_long(vsize, ==, YOKAN_KEY_NOT_FOUND);
        } else {
            munit_assert_long(vsize, ==, p.size());
            munit_assert_memory_equal(vsize, val.data(), p.data());
        }
        i += 1;
    }

    return MUNIT_OK;
}

static char* true_false_params[] = {
    (char*)"true", (char*)"false", (char*)NULL };

static char* batch_size_params[] = {
    (char*)"0", (char*)"5", (char*)NULL };

static MunitParameterEnum test_multi_params[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"no-rdma", (char**)true_false_params },
  { (char*)"batch-size", (char**)batch_size_params },
  { (char*)"use-pool", (char**)true_false_params },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-items", NULL },
  { NULL, NULL }
};

static MunitParameterEnum test_default_params[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"no-rdma", (char**)true_false_params },
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-items", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/doc_fetch", test_doc_fetch,
        test_doc_fetch_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_default_params },
    { (char*) "/doc_fetch/id-not-found", test_doc_fetch_id_not_found,
        test_doc_fetch_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_default_params },
    { (char*) "/doc_fetch_multi", test_doc_fetch_multi,
        test_doc_fetch_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { (char*) "/doc_fetch_multi/id-not-found", test_doc_fetch_multi_id_not_found,
        test_doc_fetch_context_setup, doc_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

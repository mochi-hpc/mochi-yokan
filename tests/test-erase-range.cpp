/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <vector>
#include <string>
#include <cstring>

static const char* k_prefix_a = "AAA-";
static const char* k_prefix_b = "BBB-";

static void* test_erase_range_context_setup(const MunitParameter params[], void* user_data)
{
    auto context = static_cast<kv_test_context*>(
        kv_test_common_context_setup(params, user_data));

    /* Rewrite the reference map so half the keys start with k_prefix_a, the
     * other half with k_prefix_b. We rebuild the DB from scratch to make sure
     * the on-disk state matches reference. */
    std::unordered_map<std::string, std::string> new_ref;
    unsigned i = 0;
    for(auto& p : context->reference) {
        std::string prefixed = (i % 2 == 0 ? k_prefix_a : k_prefix_b) + p.first;
        new_ref.emplace(std::move(prefixed), p.second);
        i += 1;
    }
    context->reference = std::move(new_ref);

    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<const void*> vptrs;
    std::vector<size_t>      vsizes;
    kptrs.reserve(context->reference.size());
    ksizes.reserve(context->reference.size());
    vptrs.reserve(context->reference.size());
    vsizes.reserve(context->reference.size());
    for(auto& p : context->reference) {
        kptrs.push_back(p.first.data());
        ksizes.push_back(p.first.size());
        vptrs.push_back(p.second.data());
        vsizes.push_back(p.second.size());
    }
    yk_put_multi(context->dbh, context->mode, kptrs.size(), kptrs.data(), ksizes.data(),
                 vptrs.data(), vsizes.data());

    return context;
}

static MunitResult test_erase_range_prefix(const MunitParameter params[], void* data)
{
    (void)params;
    auto* context = (kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;

    yk_return_t ret = yk_erase_range(dbh, context->mode, k_prefix_a, std::strlen(k_prefix_a));
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(auto& p : context->reference) {
        uint8_t flag = 0;
        ret = yk_exists(dbh, context->mode, p.first.data(), p.first.size(), &flag);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        bool should_exist = (p.first.compare(0, std::strlen(k_prefix_a), k_prefix_a) != 0);
        munit_assert_int(flag, ==, should_exist ? 1 : 0);
    }

    return MUNIT_OK;
}

static MunitResult test_erase_range_empty_prefix(const MunitParameter params[], void* data)
{
    (void)params;
    auto* context = (kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;

    yk_return_t ret = yk_erase_range(dbh, context->mode, nullptr, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(auto& p : context->reference) {
        uint8_t flag = 1;
        ret = yk_exists(dbh, context->mode, p.first.data(), p.first.size(), &flag);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(flag, ==, 0);
    }

    return MUNIT_OK;
}

static MunitResult test_erase_range_no_match(const MunitParameter params[], void* data)
{
    (void)params;
    auto* context = (kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;

    /* A prefix that no key starts with: nothing should be erased. */
    const char* unmatched = "ZZZ-no-such-prefix-";
    yk_return_t ret = yk_erase_range(dbh, context->mode, unmatched, std::strlen(unmatched));
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(auto& p : context->reference) {
        uint8_t flag = 0;
        ret = yk_exists(dbh, context->mode, p.first.data(), p.first.size(), &flag);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(flag, ==, 1);
    }

    return MUNIT_OK;
}

static MunitResult test_erase_range_invalid_args(const MunitParameter params[], void* data)
{
    (void)params;
    auto* context = (kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;

    yk_return_t ret = yk_erase_range(dbh, context->mode, nullptr, 4);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
    { (char*)"backend", (char**)available_backends },
    { (char*)"min-key-size", NULL },
    { (char*)"max-key-size", NULL },
    { (char*)"min-val-size", NULL },
    { (char*)"max-val-size", NULL },
    { (char*)"num-items", NULL },
    { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/erase_range/prefix", test_erase_range_prefix,
        test_erase_range_context_setup, kv_test_common_context_tear_down,
        MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_range/empty-prefix", test_erase_range_empty_prefix,
        test_erase_range_context_setup, kv_test_common_context_tear_down,
        MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_range/no-match", test_erase_range_no_match,
        test_erase_range_context_setup, kv_test_common_context_tear_down,
        MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_range/invalid-args", test_erase_range_invalid_args,
        test_erase_range_context_setup, kv_test_common_context_tear_down,
        MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

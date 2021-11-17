/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <numeric>
#include <vector>
#include <array>

static void* test_get_context_setup(const MunitParameter params[], void* user_data)
{
    auto context = static_cast<kv_test_context*>(
        kv_test_common_context_setup(params, user_data));

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<const void*> vptrs;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vptrs.reserve(count);
    vsizes.reserve(count);

    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = p.second.data();
        auto vsize = p.second.size();
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vptrs.push_back(val);
        vsizes.push_back(vsize);
    }

    yk_put_multi(context->dbh, 0,count, kptrs.data(), ksizes.data(),
                                vptrs.data(), vsizes.data());

    return context;
}

static MunitResult test_count(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    size_t count = 0;
    ret = yk_count(dbh, 0, &count);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_long(count, ==, context->reference.size());

    return MUNIT_OK;
}

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-itemss", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/count", test_count,
        test_get_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

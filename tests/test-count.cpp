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
    auto context = static_cast<test_context*>(
        test_common_context_setup(params, user_data));

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

    rkv_put_multi(context->dbh, 0,count, kptrs.data(), ksizes.data(),
                                vptrs.data(), vsizes.data());

    return context;
}

static MunitResult test_count(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    size_t count = 0;
    ret = rkv_count(dbh, 0, &count);
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
  { (char*)"num-keyvals", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/count", test_count,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/rkv/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "rkv", argc, argv);
}

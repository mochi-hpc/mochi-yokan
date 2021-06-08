/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <vector>

/**
 * @brief Check that we can put key/value pairs from the reference map.
 */
static MunitResult test_put(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = p.second.data();
        auto vsize = p.second.size();
        ret = rkv_put(dbh, key, ksize, val, vsize);
        munit_assert_int(ret, ==, RKV_SUCCESS);
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we can put key/value pairs where the value is empty.
 */
static MunitResult test_put_empty_values(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        ret = rkv_put(dbh, key, ksize, nullptr, 0);
        munit_assert_int(ret, ==, RKV_SUCCESS);
    }

    return MUNIT_OK;
}

/**
 * @brief Check that putting an empty key leads to an error.
 */
static MunitResult test_put_empty_keys(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    ret = rkv_put(dbh, "abc", 0, "def", 3);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    ret = rkv_put(dbh, nullptr, 0, "def", 3);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    ret = rkv_put(dbh, nullptr, 0, nullptr, 0);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can put the key/value pairs from the
 * reference map using put_multi, and that put_multi also accepts
 * a count of 0.
 */
static MunitResult test_put_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

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

    ret = rkv_put_multi(dbh, count, kptrs.data(), ksizes.data(),
                                    vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_put_multi(dbh, 0, NULL, NULL, NULL, NULL);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use put_multi to put all empty values.
 */
static MunitResult test_put_multi_all_empty_values(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

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
        auto val   = nullptr;
        auto vsize = 0;
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vptrs.push_back(val);
        vsizes.push_back(vsize);
    }

    ret = rkv_put_multi(dbh, count, kptrs.data(), ksizes.data(),
                                    vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use put_multi to put some empty values.
 */
static MunitResult test_put_multi_some_empty_values(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<const void*> vptrs;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vptrs.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;

    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = p.second.data();
        auto vsize = p.second.size();
        if(i % 3 == 0)
            vsize = 0;
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vptrs.push_back(val);
        vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_put_multi(dbh, count, kptrs.data(), ksizes.data(),
                                    vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if one key is empty, the function correctly fails.
 */
static MunitResult test_put_multi_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

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
        auto val   = nullptr;
        auto vsize = 0;
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vptrs.push_back(val);
        vsizes.push_back(vsize);
    }

    ksizes[count/2] = 0;

    ret = rkv_put_multi(dbh, count, kptrs.data(), ksizes.data(),
                                    vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use put_packed to store the key/value
 * pairs from the reference map, and that a count of 0 is also
 * valid.
 */
static MunitResult test_put_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         pkeys;
    std::vector<size_t> ksizes;
    std::string         pvals;
    std::vector<size_t> vsizes;

    ksizes.reserve(count);
    vsizes.reserve(count);

    for(auto& p : context->reference) {
        auto& key  = p.first;
        auto ksize = p.first.size();
        auto& val  = p.second;
        auto vsize = p.second.size();
        pkeys += key;
        ksizes.push_back(ksize);
        pvals += val;
        vsizes.push_back(vsize);
    }

    ret = rkv_put_packed(dbh, count, pkeys.data(), ksizes.data(),
                                     pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_put_packed(dbh, 0, pkeys.data(), ksizes.data(),
                                 pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_put_packed(dbh, 0, NULL, NULL, NULL, NULL);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use put_packed to store the key/value
 * pairs from the reference map, including some values of size 0.
 */
static MunitResult test_put_packed_some_empty_values(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         pkeys;
    std::vector<size_t> ksizes;
    std::string         pvals;
    std::vector<size_t> vsizes;

    ksizes.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;

    for(auto& p : context->reference) {
        auto& key  = p.first;
        auto ksize = p.first.size();
        auto& val  = p.second;
        auto vsize = p.second.size();
        pkeys += key;
        ksizes.push_back(ksize);
        if(i % 3 == 0) {
            vsizes.push_back(0);
        } else {
            pvals += val;
            vsizes.push_back(vsize);
        }
        i += 1;
    }

    ret = rkv_put_packed(dbh, count, pkeys.data(), ksizes.data(),
                                     pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use put_packed to store the key/value
 * pairs from the reference map, with all values of size 0.
 */
static MunitResult test_put_packed_all_empty_values(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         pkeys;
    std::vector<size_t> ksizes;
    std::string         pvals;
    std::vector<size_t> vsizes;

    ksizes.reserve(count);
    vsizes.reserve(count);

    for(auto& p : context->reference) {
        auto& key  = p.first;
        auto ksize = p.first.size();
        pkeys += key;
        ksizes.push_back(ksize);
        vsizes.push_back(0);
    }

    ret = rkv_put_packed(dbh, count, pkeys.data(), ksizes.data(),
                                     pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that put_packed correctly detects that a key is empty
 * and returns an error.
 */
static MunitResult test_put_packed_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         pkeys;
    std::vector<size_t> ksizes;
    std::string         pvals;
    std::vector<size_t> vsizes;

    ksizes.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;

    for(auto& p : context->reference) {
        auto& key  = p.first;
        auto ksize = p.first.size();
        auto& val  = p.second;
        auto vsize = p.second.size();
        if(i == count/2) {
            ksizes.push_back(0);
        } else {
            pkeys += key;
            ksizes.push_back(ksize);
        }
        pvals += val;
        vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_put_packed(dbh, count, pkeys.data(), ksizes.data(),
                                     pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}
static MunitTest test_suite_tests[] = {
    { (char*) "/put", test_put,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put/empty-values", test_put_empty_values,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put/empty-keys", test_put_empty_keys,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_multi", test_put_multi,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_multi/all-empty-values", test_put_multi_all_empty_values,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_multi/some-empty-values", test_put_multi_some_empty_values,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_multi/empty-key", test_put_multi_empty_key,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_packed", test_put_packed,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_packed/all-empty-values", test_put_packed_all_empty_values,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_packed/some-empty-values", test_put_packed_some_empty_values,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { (char*) "/put_packed/empty-key", test_put_packed_empty_key,
        test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/rkv/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "rkv", argc, argv);
}

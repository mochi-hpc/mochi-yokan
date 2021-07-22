/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <numeric>
#include <vector>
#include <cmath>
#include <array>

static void* test_erase_context_setup(const MunitParameter params[], void* user_data)
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

    rkv_put_multi(context->dbh, kptrs.size(), kptrs.data(), ksizes.data(),
                  vptrs.data(), vsizes.data());

    return context;
}

static MunitResult test_erase(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    // erase half of the keys
    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 0) {
            auto key = p.first.data();
            auto ksize = p.first.size();
            ret = rkv_erase(dbh, key, ksize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, RKV_SUCCESS);
        }
        i += 1;
    }

    // check which keys now exist
    i = 0;
    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        uint8_t flag = 0;
        ret = rkv_exists(dbh, key, ksize, &flag);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, RKV_SUCCESS);
        munit_assert_int(flag, ==, (i % 2 != 0));
        i += 1;
    }

    return MUNIT_OK;
}

static MunitResult test_erase_empty_keys(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    ret = rkv_erase(dbh, "abc", 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    ret = rkv_erase(dbh, nullptr, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_erase_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;

    kptrs.reserve(count);
    ksizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 0) {
            auto key   = p.first.data();
            auto ksize = p.first.size();
            kptrs.push_back(key);
            ksizes.push_back(ksize);
        }
        i += 1;
    }

    ret = rkv_erase_multi(dbh, ksizes.size(),
                          kptrs.data(),
                          ksizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    // check which keys now exist
    i = 0;
    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        uint8_t flag = 0;
        ret = rkv_exists(dbh, key, ksize, &flag);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, RKV_SUCCESS);
        munit_assert_int(flag, ==, (i % 2 != 0));
        i += 1;
    }

    // check with all NULL
    ret = rkv_erase_multi(dbh, 0, NULL, NULL);
    munit_assert_int(ret, ==, RKV_SUCCESS);
    SKIP_IF_NOT_IMPLEMENTED(ret);

    return MUNIT_OK;
}

static MunitResult test_erase_multi_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;

    kptrs.reserve(count);
    ksizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        if(i == context->reference.size()/2) {
            kptrs.push_back(key);
            ksizes.push_back(0);
        } else if(i % 2 == 0) {
            kptrs.push_back(key);
            ksizes.push_back(ksize);
        }
        i += 1;
    }

    ret = rkv_erase_multi(dbh, kptrs.size(), kptrs.data(), ksizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    // check with invalid keys or ksizes
    ret = rkv_erase_multi(dbh, kptrs.size(), nullptr, ksizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    ret = rkv_erase_multi(dbh, kptrs.size(), kptrs.data(), nullptr);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_erase_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    std::string          packed_keys;
    std::vector<size_t>  packed_ksizes;

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 0) {
            packed_keys += p.first;
            packed_ksizes.push_back(p.first.size());
        }
        i += 1;
    }

    ret = rkv_erase_packed(dbh, packed_ksizes.size(),
                           packed_keys.data(),
                           packed_ksizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    // check which keys now exist
    i = 0;
    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        uint8_t flag = 0;
        ret = rkv_exists(dbh, key, ksize, &flag);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, RKV_SUCCESS);
        munit_assert_int(flag, ==, (i % 2 != 0));
        i += 1;
    }

    // check with all NULL

    ret = rkv_erase_packed(dbh, 0, NULL, NULL);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

static MunitResult test_erase_packed_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i == context->reference.size()/2) {
            packed_ksizes[i] = 0;
        } else if (i % 2 == 0) {
            packed_keys += p.first;
            packed_ksizes[i] = p.first.size();
        }
        i += 1;
    }

    ret = rkv_erase_packed(dbh, packed_ksizes.size(),
                           packed_keys.data(),
                           packed_ksizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    // other invalid args test cases
    ret = rkv_erase_packed(dbh, packed_ksizes.size(),
                           nullptr,
                           packed_ksizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);
    ret = rkv_erase_packed(dbh, packed_ksizes.size(),
                           packed_keys.data(),
                           nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    // erase packed with all the keys of size 0
    for(auto& s : packed_ksizes) s = 0;
    ret = rkv_erase_packed(dbh, packed_ksizes.size(),
                           packed_keys.data(),
                           packed_ksizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_erase_bulk(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;
    hg_return_t hret;
    hg_bulk_t bulk;

    auto count = context->reference.size();
    std::string          pkeys;
    std::vector<size_t>  ksizes;

    ksizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 0) {
            auto& key  = p.first;
            auto ksize = p.first.size();
            pkeys += key;
            ksizes.push_back(ksize);
        }
        i += 1;
    }

    size_t garbage_size = 42;
    std::string garbage('x', garbage_size);

    std::array<void*, 3> seg_ptrs = {
        const_cast<char*>(garbage.data()),
        static_cast<void*>(ksizes.data()),
        const_cast<char*>(pkeys.data())
    };
    std::array<hg_size_t, 3> seg_sizes = {
        garbage_size,
        ksizes.size()*sizeof(size_t),
        pkeys.size()
    };
    auto useful_size = std::accumulate(
            seg_sizes.begin()+1, seg_sizes.end(), 0);

    hret = margo_bulk_create(context->mid,
            3, seg_ptrs.data(), seg_sizes.data(),
            HG_BULK_READ_ONLY, &bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->mid,
            addr_str, &addr_str_size, context->addr);

    ret = rkv_erase_bulk(dbh, ksizes.size(), addr_str, bulk,
                         garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_erase_bulk(dbh, ksizes.size(), nullptr, bulk,
                         garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_erase_bulk(dbh, ksizes.size(), "invalid-address", bulk,
                         garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_FROM_MERCURY);

    /* first invalid size (covers key sizes,
     * but not all of the keys) */
    auto invalid_size = seg_sizes[1] + 1;
    ret = rkv_erase_bulk(dbh, ksizes.size(), nullptr, bulk,
                         garbage_size, invalid_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    // check with a size of 0
    ret = rkv_erase_bulk(dbh, ksizes.size(), nullptr, bulk,
                         garbage_size, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);

    hret = margo_bulk_free(bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

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
    { (char*) "/erase", test_erase,
        test_erase_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase/empty-keys", test_erase_empty_keys,
        test_erase_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_multi", test_erase_multi,
        test_erase_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_multi/empty-key", test_erase_multi_empty_key,
        test_erase_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_packed", test_erase_packed,
        test_erase_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_packed/empty-key", test_erase_packed_empty_key,
        test_erase_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/erase_bulk", test_erase_bulk,
        test_erase_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/rkv/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "rkv", argc, argv);
}

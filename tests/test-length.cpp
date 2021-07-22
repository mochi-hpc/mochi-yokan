/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <numeric>
#include <vector>
#include <array>

static void* test_length_context_setup(const MunitParameter params[], void* user_data)
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

    rkv_put_multi(context->dbh, count, kptrs.data(), ksizes.data(),
                                vptrs.data(), vsizes.data());

    return context;
}

/**
 * @brief Check that we can get the size of values from the reference map.
 */
static MunitResult test_length(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        size_t vsize = 0;
        ret = rkv_length(dbh, key, ksize, &vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, RKV_SUCCESS);
        munit_assert_int(vsize, ==, p.second.size());
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we correctely detect that a key does not exists,
 * and that the resulting size is set fo RKV_KEY_NOT_FOUND.
 */
static MunitResult test_length_key_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto key = std::string("XXXXXXXXXXXX");
    auto ksize = key.size();
    size_t vsize = 0;

    ret = rkv_length(dbh, key.data(), ksize, &vsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_KEY_NOT_FOUND);
    munit_assert_long(vsize, ==, RKV_KEY_NOT_FOUND);

    return MUNIT_OK;
}

/**
 * @brief Check that using an empty key leads to an error.
 */
static MunitResult test_length_empty_keys(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    size_t val_size = 0;
    ret = rkv_length(dbh, "abc", 0, &val_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    ret = rkv_length(dbh, nullptr, 0, &val_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can get the value sizes from the
 * reference map using length_multi, and that length_multi also accepts
 * a count of 0.
 */
static MunitResult test_length_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto vsize = g_max_val_size;
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_length_multi(dbh, count,
                           kptrs.data(), ksizes.data(),
                           vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    i = 0;
    for(auto& p : context->reference) {
        auto vsize = vsizes[i];
        munit_assert_long(vsize, ==, p.second.size());
        i += 1;
    }

    // check with all NULL

    ret = rkv_length_multi(dbh, 0, NULL, NULL, NULL);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if one key is empty, the function correctly fails.
 */
static MunitResult test_length_multi_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto vsize = g_max_val_size;
        kptrs.push_back(key);
        if(i == context->reference.size()/2) {
            ksizes.push_back(0);
        } else {
            ksizes.push_back(ksize);
        }
        vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_length_multi(dbh, count, kptrs.data(), ksizes.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    // other invalid args tests
    ret = rkv_length_multi(dbh, count, nullptr, ksizes.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);
    ret = rkv_length_multi(dbh, count, kptrs.data(), nullptr, vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);
    ret = rkv_length_multi(dbh, count, kptrs.data(), ksizes.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can get the length of values from the
 * reference map using length_multi, and that if a key is not found
 * the value size is properly set to RKV_KEY_NOT_FOUND.
 */
static MunitResult test_length_multi_key_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<std::string> keys(count);
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 3 == 0)
            keys[i] = "XXXXXXXXXXXX";
        else
            keys[i]    = p.first;
        auto key   = keys[i].data();
        auto ksize = keys[i].size();
        auto vsize = 0;
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_length_multi(dbh, count,
                           kptrs.data(), ksizes.data(),
                           vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    i = 0;
    for(auto& p : context->reference) {
        auto vsize = vsizes[i];
        if(i % 3 == 0) {
            munit_assert_long(vsize, ==, RKV_KEY_NOT_FOUND);
        } else {
            munit_assert_long(vsize, ==, p.second.size());
        }
        i += 1;
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we can get the key/value pairs from the
 * reference map using get_packed, and that put_multi also accepts
 * a count of 0.
 */
static MunitResult test_length_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);
    std::vector<size_t> packed_vsizes(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        packed_keys += p.first;
        packed_ksizes[i] = p.first.size();
        i += 1;
    }

    ret = rkv_length_packed(dbh, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            packed_vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    i = 0;
    for(auto& p : context->reference) {
        auto vsize = packed_vsizes[i];
        munit_assert_long(vsize, ==, p.second.size());
        i += 1;
    }

    // check with all NULL

    ret = rkv_length_packed(dbh, 0, NULL, NULL, NULL);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if a key has a size of 0, we get an error.
 */
static MunitResult test_length_packed_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);
    std::vector<size_t> packed_vsizes(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i == context->reference.size()/2) {
            packed_ksizes[i] = 0;
        } else {
            packed_keys += p.first;
            packed_ksizes[i] = p.first.size();
        }
        i += 1;
    }

    ret = rkv_length_packed(dbh, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            packed_vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    // other invalid args tests
    ret = rkv_length_packed(dbh, count,
                            nullptr,
                            packed_ksizes.data(),
                            packed_vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);
    ret = rkv_length_packed(dbh, count,
                            packed_keys.data(),
                            nullptr,
                            packed_vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);
    ret = rkv_length_packed(dbh, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    for(auto& s : packed_ksizes) s = 0;

    ret = rkv_length_packed(dbh, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            packed_vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_length_packed_key_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);
    std::vector<size_t> packed_vsizes(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 3 == 0) {
            packed_keys += "XXXXXXXXXXXX";
            packed_ksizes[i] = 12;
        } else {
            packed_keys += p.first;
            packed_ksizes[i] = p.first.size();
        }
        i += 1;
    }

    ret = rkv_length_packed(dbh, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            packed_vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    i = 0;
    for(auto& p : context->reference) {
        auto vsize = packed_vsizes[i];
        if(i % 3 == 0) {
            munit_assert_long(vsize, ==, RKV_KEY_NOT_FOUND);
        } else {
            munit_assert_long(vsize, ==, p.second.size());
        }
        i += 1;
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we can use length_bulk to get the value sizes.
 * We use either null as the origin address, or this process' address,
 * to exercise both code paths.
 */
static MunitResult test_length_bulk(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;
    hg_return_t hret;
    hg_bulk_t bulk;

    auto count = context->reference.size();
    std::string         pkeys;
    std::vector<size_t> ksizes;
    std::vector<size_t> vsizes(count, g_max_val_size);

    ksizes.reserve(count);

    for(auto& p : context->reference) {
        auto& key  = p.first;
        auto ksize = p.first.size();
        pkeys += key;
        ksizes.push_back(ksize);
    }

    size_t garbage_size = 42;
    std::string garbage('x', garbage_size);

    std::array<void*, 4> seg_ptrs = {
        const_cast<char*>(garbage.data()),
        static_cast<void*>(ksizes.data()),
        const_cast<char*>(pkeys.data()),
        static_cast<void*>(vsizes.data())
    };
    std::array<hg_size_t, 4> seg_sizes = {
        garbage_size,
        ksizes.size()*sizeof(size_t),
        pkeys.size(),
        vsizes.size()*sizeof(size_t)
    };
    auto useful_size = std::accumulate(
            seg_sizes.begin()+1, seg_sizes.end(), 0);

    hret = margo_bulk_create(context->mid,
            4, seg_ptrs.data(), seg_sizes.data(),
            HG_BULK_READWRITE, &bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->mid,
            addr_str, &addr_str_size, context->addr);

    ret = rkv_length_bulk(dbh, count, addr_str, bulk,
                          garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_length_bulk(dbh, count, nullptr, bulk,
                          garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_length_bulk(dbh, count, "invalid-address", bulk,
                          garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_FROM_MERCURY);

    /* first invalid size (covers key sizes,
     * but not all of the keys) */
    auto invalid_size = seg_sizes[1] + 1;
    ret = rkv_length_bulk(dbh, count, nullptr, bulk,
                          garbage_size, invalid_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    /* second invalid size (covers key sizes, keys,
     * but not enough space for value sizes). */
    invalid_size = seg_sizes[1] + seg_sizes[2] + 1;
    ret = rkv_length_bulk(dbh, count, nullptr, bulk,
                          garbage_size, invalid_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    /* third invalid size (0) */
    ret = rkv_length_bulk(dbh, count, nullptr, bulk,
                          garbage_size, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

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
    { (char*) "/length", test_length,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length/empty-keys", test_length_empty_keys,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length/key-not-found", test_length_key_not_found,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length_multi", test_length_multi,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length_multi/empty-key", test_length_multi_empty_key,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length_multi/key-not-found", test_length_multi_key_not_found,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length_packed", test_length_packed,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length_packed/empty-key", test_length_packed_empty_key,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length_packed/key-not-found", test_length_packed_key_not_found,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/length_bulk", test_length_bulk,
        test_length_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/rkv/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "rkv", argc, argv);
}

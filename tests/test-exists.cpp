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

static void* test_exists_context_setup(const MunitParameter params[], void* user_data)
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

    // we only store every other key
    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 0) {
            auto key   = p.first.data();
            auto ksize = p.first.size();
            auto val   = p.second.data();
            auto vsize = p.second.size();
            kptrs.push_back(key);
            ksizes.push_back(ksize);
            vptrs.push_back(val);
            vsizes.push_back(vsize);
        }
        i += 1;
    }

    yk_put_multi(context->dbh, context->mode, kptrs.size(), kptrs.data(), ksizes.data(),
                                vptrs.data(), vsizes.data());
    return context;
}

/**
 * @brief Check that we can get the size of values from the reference map.
 */
static MunitResult test_exists(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    unsigned i = 0;

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        uint8_t flag = 0;
        ret = yk_exists(dbh, context->mode, key, ksize, &flag);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(flag, ==, (i % 2 == 0));
        i += 1;
    }

    return MUNIT_OK;
}

/**
 * @brief Check that using an empty key leads to an error.
 */
static MunitResult test_exists_empty_keys(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    uint8_t flag = 0;
    ret = yk_exists(dbh, context->mode, "abc", 0, &flag);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    ret = yk_exists(dbh, context->mode, nullptr, 0, &flag);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_exists_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<uint8_t>     flags;

    kptrs.reserve(count);
    ksizes.reserve(count);
    flags.reserve(std::ceil(count/8.0));

    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        kptrs.push_back(key);
        ksizes.push_back(ksize);
    }

    ret = yk_exists_multi(dbh, context->mode, count,
                          kptrs.data(),
                          ksizes.data(),
                          flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(unsigned i = 0; i < count; i++) {
        auto exists = yk_unpack_exists_flag(flags.data(), i);
        munit_assert_int(exists, ==, (i % 2 == 0));
    }

    // check with all NULL

    ret = yk_exists_multi(dbh, context->mode, 0, NULL, NULL, NULL);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if one key is empty, the function correctly fails.
 */
static MunitResult test_exists_multi_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = context->reference.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<uint8_t>      flags;

    kptrs.reserve(count);
    ksizes.reserve(count);
    flags.reserve(std::ceil(count/8.0));

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        kptrs.push_back(key);
        if(i == context->reference.size()/2) {
            ksizes.push_back(0);
        } else {
            ksizes.push_back(ksize);
        }
        i += 1;
    }

    ret = yk_exists_multi(dbh, context->mode, count, kptrs.data(), ksizes.data(), flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // test with other invalid args
    ret = yk_exists_multi(dbh, context->mode, count, nullptr, ksizes.data(), flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_exists_multi(dbh, context->mode, count, kptrs.data(), nullptr, flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_exists_multi(dbh, context->mode, count, kptrs.data(), ksizes.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);


    return MUNIT_OK;
}

static MunitResult test_exists_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = context->reference.size();
    std::string          packed_keys;
    std::vector<size_t>  packed_ksizes(count);
    std::vector<uint8_t> flags(std::ceil(count/8.0));

    unsigned i = 0;
    for(auto& p : context->reference) {
        packed_keys += p.first;
        packed_ksizes[i] = p.first.size();
        i += 1;
    }

    ret = yk_exists_packed(dbh, context->mode, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    for(i = 0; i < count; i++) {
        auto exists = yk_unpack_exists_flag(flags.data(), i);
        munit_assert_int(exists, ==, (i % 2 == 0));
        i += 1;
    }

    // check with all NULL

    ret = yk_exists_packed(dbh, context->mode, 0, NULL, NULL, NULL);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if a key has a size of 0, we get an error.
 */
static MunitResult test_exists_packed_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);
    std::vector<uint8_t> flags(std::ceil(count/8.0));

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

    ret = yk_exists_packed(dbh, context->mode, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // other invalid args tests
    ret = yk_exists_packed(dbh, context->mode, count,
                            nullptr,
                            packed_ksizes.data(),
                            flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_exists_packed(dbh, context->mode, count,
                            packed_keys.data(),
                            nullptr,
                            flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_exists_packed(dbh, context->mode, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // test with only 0s in the ksizes
    for(auto& s : packed_ksizes) s = 0;

    ret = yk_exists_packed(dbh, context->mode, count,
                            packed_keys.data(),
                            packed_ksizes.data(),
                            flags.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_exists_bulk(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;
    hg_return_t hret;
    hg_bulk_t bulk;

    auto count = context->reference.size();
    std::string          pkeys;
    std::vector<size_t>  ksizes;
    std::vector<uint8_t> flags(std::ceil(count/8.0));

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
        static_cast<void*>(flags.data())
    };
    std::array<hg_size_t, 4> seg_sizes = {
        garbage_size,
        ksizes.size()*sizeof(size_t),
        pkeys.size(),
        flags.size()
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

    ret = yk_exists_bulk(dbh, context->mode, count, addr_str, bulk,
                          garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_exists_bulk(dbh, context->mode, count, nullptr, bulk,
                          garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_exists_bulk(dbh, context->mode, count, "invalid-address", bulk,
                          garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_FROM_MERCURY);

    /* first invalid size (covers key sizes,
     * but not all of the keys) */
    auto invalid_size = seg_sizes[1] + 1;
    ret = yk_exists_bulk(dbh, context->mode, count, nullptr, bulk,
                          garbage_size, invalid_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* second invalid size (covers key sizes, keys,
     * but not enough space for value sizes). */
    invalid_size = seg_sizes[1] + seg_sizes[2] + 1;
    ret = yk_exists_bulk(dbh, context->mode, count, nullptr, bulk,
                          garbage_size, invalid_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    /* third invalid size (0) */
    ret = yk_exists_bulk(dbh, context->mode, count, nullptr, bulk,
                          garbage_size, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);


    hret = margo_bulk_free(bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    return MUNIT_OK;
}

static char* no_rdma_params[] = {
    (char*)"true", (char*)"false", (char*)NULL };

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"no-rdma", (char**)no_rdma_params },
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-items", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/exists", test_exists,
        test_exists_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/exists/empty-keys", test_exists_empty_keys,
        test_exists_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/exists_multi", test_exists_multi,
        test_exists_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/exists_multi/empty-key", test_exists_multi_empty_key,
        test_exists_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/exists_packed", test_exists_packed,
        test_exists_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/exists_packed/empty-key", test_exists_packed_empty_key,
        test_exists_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/exists_bulk", test_exists_bulk,
        test_exists_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

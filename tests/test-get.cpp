/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <numeric>
#include <vector>

static void* test_get_context_setup(const MunitParameter params[], void* user_data)
{
    auto context = static_cast<test_context*>(
        test_common_context_setup(params, user_data));

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

    ret = rkv_put_multi(context->dbh, count, kptrs.data(), ksizes.data(),
                                      vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return context;
}

/**
 * @brief Check that we can get key/value pairs from the reference map.
 */
static MunitResult test_get(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = rkv_get(dbh, key, ksize, val.data(), &vsize);
        munit_assert_int(ret, ==, RKV_SUCCESS);
        munit_assert_int(vsize, ==, p.second.size());
        munit_assert_memory_equal(vsize, val.data(), p.second.data());
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we correctely detect a buffer too small.
 */
static MunitResult test_get_too_small(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto it = context->reference.begin();
    while(it->second.size() == 0 && it != context->reference.end()) it++;

    if(it == context->reference.end())
        return MUNIT_SKIP;

    auto& p = *it;

    auto key = p.first.data();
    auto ksize = p.first.size();
    std::vector<char> val(p.second.size()/2);
    size_t vsize = val.size();
    ret = rkv_get(dbh, key, ksize, val.data(), &vsize);
    munit_assert_int(ret, ==, RKV_ERR_BUFFER_SIZE);

    return MUNIT_OK;
}

/**
 * @brief Check that we correctely detect that a key does not exists
 */
static MunitResult test_get_key_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto key = std::string("XXXXXXXXXXXX");
    auto ksize = key.size();
    std::vector<char> val(g_max_val_size);
    size_t vsize = g_max_val_size;

    ret = rkv_get(dbh, key.data(), ksize, val.data(), &vsize);
    munit_assert_int(ret, ==, RKV_ERR_KEY_NOT_FOUND);

    return MUNIT_OK;
}



/**
 * @brief Check that getting an empty key leads to an error.
 */
static MunitResult test_get_empty_keys(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    std::vector<char> val(g_max_val_size);
    size_t val_size = g_max_val_size;
    ret = rkv_get(dbh, "abc", 0, val.data(), &val_size);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    val_size = g_max_val_size;
    ret = rkv_get(dbh, nullptr, 0, val.data(), &val_size);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    val_size = 0;
    ret = rkv_get(dbh, nullptr, 0, nullptr, &val_size);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can get the key/value pairs from the
 * reference map using get_multi, and that put_multi also accepts
 * a count of 0.
 */
static MunitResult test_get_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<std::vector<char>> values(count);
    for(auto& v : values) v.resize(g_max_val_size);
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<void*>       vptrs;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vptrs.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = values[i].data();
        auto vsize = g_max_val_size;
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vptrs.push_back(val);
        vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_get_multi(dbh, count, kptrs.data(), ksizes.data(),
                                    vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    i = 0;
    for(auto& p : context->reference) {
        auto val   = vptrs[i];
        auto vsize = vsizes[i];
        munit_assert_long(vsize, ==, p.second.size());
        munit_assert_memory_equal(vsize, val, p.second.data());
        i += 1;
    }

    // check with all NULL

    ret = rkv_get_multi(dbh, 0, NULL, NULL, NULL, NULL);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if one key is empty, the function correctly fails.
 */
static MunitResult test_get_multi_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<std::vector<char>> values(count);
    for(auto& v : values) v.resize(g_max_val_size);
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<void*>       vptrs;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vptrs.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = values[i].data();
        auto vsize = g_max_val_size;
        kptrs.push_back(key);
        if(i == context->reference.size()/2) {
            ksizes.push_back(0);
        } else {
            ksizes.push_back(ksize);
        }
        vptrs.push_back(val);
        vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_get_multi(dbh, count, kptrs.data(), ksizes.data(),
                                    vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);
    return MUNIT_OK;
}

/**
 * @brief Check that we can get the key/value pairs from the
 * reference map using get_multi, and that if a value buffer is too
 * small, its size is properly set to RKV_SIZE_TOO_SMALL.
 */
static MunitResult test_get_multi_too_small(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct test_context* context = (struct test_context*)data;
    rkv_database_handle_t dbh = context->dbh;
    rkv_return_t ret;

    auto count = context->reference.size();
    std::vector<std::vector<char>> values(count);
    for(auto& v : values) v.resize(g_max_val_size);
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<void*>       vptrs;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vptrs.reserve(count);
    vsizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = values[i].data();
        auto vsize = values[i].size();
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vptrs.push_back(val);
        if(i % 3 == 0)
            vsizes.push_back(p.second.size()/2);
        else
            vsizes.push_back(vsize);
        i += 1;
    }

    ret = rkv_get_multi(dbh, count, kptrs.data(), ksizes.data(),
                                    vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    i = 0;
    for(auto& p : context->reference) {
        auto val   = vptrs[i];
        auto vsize = vsizes[i];
        if(i % 3 == 0 && p.second.size() > 0) {
            munit_assert_long(vsize, ==, RKV_SIZE_TOO_SMALL);
        } else {
            munit_assert_long(vsize, ==, p.second.size());
            munit_assert_memory_equal(vsize, val, p.second.data());
        }
        i += 1;
    }

    return MUNIT_OK;
}

#if 0
/**
 * @brief Check that we can use get_packed to store the key/value
 * pairs from the reference map, and that a count of 0 is also
 * valid.
 */
static MunitResult test_get_packed(const MunitParameter params[], void* data)
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

    ret = rkv_get_packed(dbh, count, pkeys.data(), ksizes.data(),
                                     pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    // check that the key/values were correctly stored

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = rkv_get(dbh, key, ksize, val.data(), &vsize);
        munit_assert_int(ret, ==, RKV_SUCCESS);
        munit_assert_int(vsize, ==, p.second.size());
        munit_assert_memory_equal(vsize, val.data(), p.second.data());
    }

    // check with 0 keys

    ret = rkv_get_packed(dbh, 0, pkeys.data(), ksizes.data(),
                                 pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    // check with all NULL

    ret = rkv_get_packed(dbh, 0, NULL, NULL, NULL, NULL);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use get_packed to store the key/value
 * pairs from the reference map, with all values of size 0.
 */
static MunitResult test_get_packed_all_empty_values(const MunitParameter params[], void* data)
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

    ret = rkv_get_packed(dbh, count, pkeys.data(), ksizes.data(),
                                     pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    // check that the key/values were correctly stored

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = rkv_get(dbh, key, ksize, val.data(), &vsize);
        munit_assert_int(ret, ==, RKV_SUCCESS);
        munit_assert_int(vsize, ==, 0);
    }

    return MUNIT_OK;
}

/**
 * @brief Check that get_packed correctly detects that a key is empty
 * and returns an error.
 */
static MunitResult test_get_packed_empty_key(const MunitParameter params[], void* data)
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

    ret = rkv_get_packed(dbh, count, pkeys.data(), ksizes.data(),
                                     pvals.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use get_bulk to store the key/value
 * pairs from the reference map. We use either null as the origin
 * address, or this process' address, to exercise both code paths.
 */
static MunitResult test_get_bulk(const MunitParameter params[], void* data)
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

    size_t garbage_size = 42;
    std::string garbage('x', garbage_size);

    std::array<void*, 5> seg_ptrs = {
        const_cast<char*>(garbage.data()),
        static_cast<void*>(ksizes.data()),
        static_cast<void*>(vsizes.data()),
        const_cast<char*>(pkeys.data()),
        const_cast<char*>(pvals.data())
    };
    std::array<hg_size_t, 5> seg_sizes = {
        garbage_size,
        ksizes.size()*sizeof(size_t),
        vsizes.size()*sizeof(size_t),
        pkeys.size(),
        pvals.size()
    };
    auto useful_size = std::accumulate(
            seg_sizes.begin()+1, seg_sizes.end(), 0);

    hret = margo_bulk_create(context->mid,
            5, seg_ptrs.data(), seg_sizes.data(),
            HG_BULK_READ_ONLY, &bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->mid,
            addr_str, &addr_str_size, context->addr);

    ret = rkv_get_bulk(dbh, count, addr_str, bulk,
                       garbage_size, useful_size);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_get_bulk(dbh, count, nullptr, bulk,
                       garbage_size, useful_size);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    hret = margo_bulk_free(bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    return MUNIT_OK;
}

/**
 * Same as above but with empty values.
 */
static MunitResult test_get_bulk_all_empty_values(const MunitParameter params[], void* data)
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

    size_t garbage_size = 42;
    std::string garbage('x', garbage_size);

    std::array<void*, 4> seg_ptrs = {
        const_cast<char*>(garbage.data()),
        static_cast<void*>(ksizes.data()),
        static_cast<void*>(vsizes.data()),
        const_cast<char*>(pkeys.data())
    };
    std::array<hg_size_t, 4> seg_sizes = {
        garbage_size,
        ksizes.size()*sizeof(size_t),
        vsizes.size()*sizeof(size_t),
        pkeys.size()
    };
    auto useful_size = std::accumulate(
            seg_sizes.begin()+1, seg_sizes.end(), 0);

    hret = margo_bulk_create(context->mid,
            4, seg_ptrs.data(), seg_sizes.data(),
            HG_BULK_READ_ONLY, &bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->mid,
            addr_str, &addr_str_size, context->addr);

    ret = rkv_get_bulk(dbh, count, addr_str, bulk,
                       garbage_size, useful_size);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    ret = rkv_get_bulk(dbh, count, nullptr, bulk,
                       garbage_size, useful_size);
    munit_assert_int(ret, ==, RKV_SUCCESS);

    hret = margo_bulk_free(bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Same as test_get_bulk but introduces an empty key and checks
 * for correct error reporting.
 */
static MunitResult test_get_bulk_empty_key(const MunitParameter params[], void* data)
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

    size_t garbage_size = 42;
    std::string garbage('x', garbage_size);

    std::array<void*, 5> seg_ptrs = {
        const_cast<char*>(garbage.data()),
        static_cast<void*>(ksizes.data()),
        static_cast<void*>(vsizes.data()),
        const_cast<char*>(pkeys.data()),
        const_cast<char*>(pvals.data())
    };
    std::array<hg_size_t, 5> seg_sizes = {
        garbage_size,
        ksizes.size()*sizeof(size_t),
        vsizes.size()*sizeof(size_t),
        pkeys.size(),
        pvals.size()
    };
    auto useful_size = std::accumulate(
            seg_sizes.begin()+1, seg_sizes.end(), 0);

    hret = margo_bulk_create(context->mid,
            5, seg_ptrs.data(), seg_sizes.data(),
            HG_BULK_READ_ONLY, &bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->mid,
            addr_str, &addr_str_size, context->addr);

    ret = rkv_get_bulk(dbh, count, addr_str, bulk,
                       garbage_size, useful_size);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    ret = rkv_get_bulk(dbh, count, nullptr, bulk,
                       garbage_size, useful_size);
    munit_assert_int(ret, ==, RKV_ERR_INVALID_ARGS);

    hret = margo_bulk_free(bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    return MUNIT_OK;
}
#endif

static MunitParameterEnum test_params[] = {
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-keyvals", NULL },
  { NULL, NULL }
};


static MunitTest test_suite_tests[] = {
    { (char*) "/get", test_get,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get/empty-keys", test_get_empty_keys,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get/too-small", test_get_too_small,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get/key-not-found", test_get_key_not_found,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_multi", test_get_multi,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_multi/empty-key", test_get_multi_empty_key,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_multi/too-small", test_get_multi_too_small,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
#if 0
    { (char*) "/get_multi/key-not-found", test_get_multi_key_not_found,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_packed", test_get_packed,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_packed/empty-key", test_get_packed_empty_key,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_bulk", test_get_bulk,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_bulk/all-empty-values", test_get_bulk_all_empty_values,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/get_bulk/empty-key", test_get_bulk_empty_key,
        test_get_context_setup, test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
#endif
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/rkv/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "rkv", argc, argv);
}

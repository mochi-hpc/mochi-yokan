/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <algorithm>
#include <numeric>
#include <vector>
#include <array>
#include <iostream>

/**
 * @brief Check that we can put key/value pairs from the reference map.
 */
static MunitResult test_put(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = p.second.data();
        auto vsize = p.second.size();
        ret = yk_put(dbh, context->mode, key, ksize, val, vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
    }

    // check that the key/values were correctly stored

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = yk_get(dbh, context->mode, key, ksize, val.data(), &vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(vsize, ==, p.second.size());
        munit_assert_memory_equal(vsize, val.data(), p.second.data());
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
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    ret = yk_put(dbh, context->mode, "abc", 0, "def", 3);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    ret = yk_put(dbh, context->mode, nullptr, 0, "def", 3);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    ret = yk_put(dbh, context->mode, nullptr, 0, nullptr, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

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
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

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

    ret = yk_put_multi(dbh, context->mode,
                       count, kptrs.data(), ksizes.data(),
                       vptrs.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that the key/values were correctly stored

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = yk_get(dbh, context->mode,
                     key, ksize, val.data(), &vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(vsize, ==, p.second.size());
        munit_assert_memory_equal(vsize, val.data(), p.second.data());
    }

    // check with some nullptr
    ret = yk_put_multi(dbh, context->mode,
                       count, nullptr, ksizes.data(),
                       vptrs.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_put_multi(dbh, context->mode,
                       count, kptrs.data(), nullptr,
                       vptrs.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_put_multi(dbh, context->mode,
                       count, kptrs.data(), ksizes.data(),
                       nullptr, vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_put_multi(dbh, context->mode,
                       count, kptrs.data(), ksizes.data(),
                       vptrs.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // check with all NULL

    ret = yk_put_multi(dbh, context->mode,
                       0, NULL, NULL, NULL, NULL);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use put_multi to put all empty values.
 */
static MunitResult test_put_multi_all_empty_values(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

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

    ret = yk_put_multi(dbh, context->mode,
                       count, kptrs.data(), ksizes.data(),
                       vptrs.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that the key/values were correctly stored

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = yk_get(dbh, context->mode, key, ksize, val.data(), &vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(vsize, ==, 0);
    }


    return MUNIT_OK;
}

/**
 * @brief Check that if one key is empty, the function correctly fails.
 */
static MunitResult test_put_multi_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

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

    ret = yk_put_multi(dbh, context->mode, count,
                       kptrs.data(), ksizes.data(),
                       vptrs.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

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
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

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

    ret = yk_put_packed(dbh, context->mode,
                        count, pkeys.data(), ksizes.data(),
                        pvals.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that the key/values were correctly stored

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = yk_get(dbh, context->mode, key, ksize, val.data(), &vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(vsize, ==, p.second.size());
        munit_assert_memory_equal(vsize, val.data(), p.second.data());
    }

    // check with 0 keys

    ret = yk_put_packed(dbh, context->mode, 0, pkeys.data(), ksizes.data(),
                        pvals.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    // check with some nullptrs
    ret = yk_put_packed(dbh, context->mode, count, nullptr, ksizes.data(),
                                     pvals.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_put_packed(dbh, context->mode, count, pkeys.data(), nullptr,
                        pvals.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    if(!context->empty_values) {
        ret = yk_put_packed(dbh, context->mode, count,
                            pkeys.data(), ksizes.data(),
                            nullptr, vsizes.data());
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    }
    ret = yk_put_packed(dbh, context->mode, count,
                        pkeys.data(), ksizes.data(),
                        pvals.data(), nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // check with all ksizes[*] = 0
    for(auto& s : ksizes) s = 0;
    ret = yk_put_packed(dbh, context->mode, count,
                        pkeys.data(), ksizes.data(),
                        pvals.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // check with all NULL

    ret = yk_put_packed(dbh, context->mode, 0, NULL, NULL, NULL, NULL);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

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
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

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

    ret = yk_put_packed(dbh, context->mode, count,
                        pkeys.data(), ksizes.data(),
                        pvals.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // check that the key/values were correctly stored

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        std::vector<char> val(g_max_val_size);
        size_t vsize = g_max_val_size;
        ret = yk_get(dbh, context->mode, key,
                     ksize, val.data(), &vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(vsize, ==, 0);
    }

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
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

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

    ret = yk_put_packed(dbh, context->mode, count,
                        pkeys.data(), ksizes.data(),
                        pvals.data(), vsizes.data());
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can use put_bulk to store the key/value
 * pairs from the reference map. We use either null as the origin
 * address, or this process' address, to exercise both code paths.
 */
static MunitResult test_put_bulk(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;
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

    if(pvals.size() != 0) {
        hret = margo_bulk_create(context->mid,
                5, seg_ptrs.data(), seg_sizes.data(),
                HG_BULK_READ_ONLY, &bulk);
    } else {
        hret = margo_bulk_create(context->mid,
                4, seg_ptrs.data(), seg_sizes.data(),
                HG_BULK_READ_ONLY, &bulk);
    }
    munit_assert_int(hret, ==, HG_SUCCESS);

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->mid,
            addr_str, &addr_str_size, context->addr);

    ret = yk_put_bulk(dbh, context->mode,
                      count, addr_str, bulk,
                      garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_put_bulk(dbh, context->mode,
                      count, nullptr, bulk,
                      garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    // with useful size = 0
    ret = yk_put_bulk(dbh, context->mode, count,
                      addr_str, bulk,
                      garbage_size, 0);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    hret = margo_bulk_free(bulk);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(hret, ==, HG_SUCCESS);

    return MUNIT_OK;
}

/**
 * Same as above but with empty values.
 */
static MunitResult test_put_bulk_all_empty_values(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;
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

    ret = yk_put_bulk(dbh, context->mode, count,
                      addr_str, bulk,
                      garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_put_bulk(dbh, context->mode, count, nullptr, bulk,
                      garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    /* invalid address */
    ret = yk_put_bulk(dbh, context->mode, count,
                      "invalid-address", bulk,
                      garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_FROM_MERCURY);

    /* incorrect bulk size */
    ret = yk_put_bulk(dbh, context->mode, count, nullptr, bulk,
                      garbage_size, useful_size/2);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    hret = margo_bulk_free(bulk);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(hret, ==, HG_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Same as test_put_bulk but introduces an empty key and checks
 * for correct error reporting.
 */
static MunitResult test_put_bulk_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;
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

    if(pvals.size() != 0) {
        hret = margo_bulk_create(context->mid,
                5, seg_ptrs.data(), seg_sizes.data(),
                HG_BULK_READ_ONLY, &bulk);
        munit_assert_int(hret, ==, HG_SUCCESS);
    } else {
        hret = margo_bulk_create(context->mid,
                4, seg_ptrs.data(), seg_sizes.data(),
                HG_BULK_READ_ONLY, &bulk);
        munit_assert_int(hret, ==, HG_SUCCESS);
    }

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->mid,
            addr_str, &addr_str_size, context->addr);

    ret = yk_put_bulk(dbh, context->mode, count, addr_str, bulk,
                      garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    ret = yk_put_bulk(dbh, context->mode, count, nullptr, bulk,
                       garbage_size, useful_size);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    hret = margo_bulk_free(bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

    return MUNIT_OK;
}

static MunitResult test_put_append(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    // start by putting with YOKAN_MODE_APPEND keys that don't exist

    size_t i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 1) {
            auto key   = p.first.data();
            auto ksize = p.first.size();
            auto val   = p.second.data();
            auto vsize = p.second.size();
            ret = yk_put(dbh, YOKAN_MODE_APPEND, key, ksize, val, vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }
        i += 1;
    }

    // check that the key/values were correctly stored

    i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 1) {
            auto key = p.first.data();
            auto ksize = p.first.size();
            std::vector<char> val(g_max_val_size);
            size_t vsize = g_max_val_size;
            ret = yk_get(dbh, context->mode, key, ksize, val.data(), &vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_int(vsize, ==, p.second.size());
            munit_assert_memory_equal(vsize, val.data(), p.second.data());
        }
        i += 1;
    }

    // use values at i % 2 == 0 to append to existing values
    const char* val = nullptr;
    size_t vsize = 0;
    i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 0) {
            val   = p.second.data();
            vsize = p.second.size();
        } else {
            auto key   = p.first.data();
            auto ksize = p.first.size();
            ret = yk_put(dbh, YOKAN_MODE_APPEND|context->mode, key, ksize, val, vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }
        i += 1;
    }

    // check that the values were correctly appended
    i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 0) {
            val   = p.second.data();
            vsize = p.second.size();
        } else {
            auto key = p.first.data();
            auto ksize = p.first.size();
            auto exp_val = p.second + std::string(val, vsize);
            auto exp_vsize = p.second.size() + vsize;
            std::vector<char> out_val(g_max_val_size*2);
            size_t out_vsize = 2*g_max_val_size;
            ret = yk_get(dbh, context->mode, key, ksize, out_val.data(), &out_vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_int(out_vsize, ==, exp_vsize);
            munit_assert_memory_equal(out_vsize, exp_val.data(), out_val.data());
        }
        i += 1;
    }

    return MUNIT_OK;
}

static MunitResult test_put_exist_only(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    // start by putting half of the keys
    size_t i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 1) {
            auto key   = p.first.data();
            auto ksize = p.first.size();
            auto val   = p.second.data();
            auto vsize = p.second.size();
            ret = yk_put(dbh, context->mode, key, ksize, val, vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }
        i += 1;
    }

    // check that the key/values were correctly stored
    i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 1) {
            auto key = p.first.data();
            auto ksize = p.first.size();
            std::vector<char> val(g_max_val_size);
            size_t vsize = g_max_val_size;
            ret = yk_get(dbh, context->mode, key, ksize, val.data(), &vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_int(vsize, ==, p.second.size());
            munit_assert_memory_equal(vsize, val.data(), p.second.data());
        }
        i += 1;
    }

    // replace values with the their reverse
    for(auto& p : context->reference) {
        std::reverse(p.second.begin(), p.second.end());
    }

    // put all the value this time, with YOKAN_MODE_EXIST_ONLY
    for(auto& p : context->reference)
    {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = p.second.data();
        auto vsize = p.second.size();
        ret = yk_put(dbh, YOKAN_MODE_EXIST_ONLY|context->mode,
                     key, ksize, val, vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
    }

    // check that only the keys that previously existed were modified
    i = 0;
    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        auto val = p.second.data();
        auto vsize = p.second.size();
        std::vector<char> out_val(g_max_val_size);
        size_t out_vsize = g_max_val_size;
        ret = yk_get(dbh, context->mode, key, ksize, out_val.data(), &out_vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        if(i % 2 == 0) {
            munit_assert_int(ret, ==, YOKAN_ERR_KEY_NOT_FOUND);
        } else {
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_int(out_vsize, ==, vsize);
            munit_assert_memory_equal(out_vsize, val, out_val.data());
        }
        i += 1;
    }

    return MUNIT_OK;
}

static MunitResult test_put_new_only(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    // start by putting half of the keys
    size_t i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 1) {
            auto key   = p.first.data();
            auto ksize = p.first.size();
            auto val   = p.second.data();
            auto vsize = p.second.size();
            ret = yk_put(dbh, context->mode, key, ksize, val, vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }
        i += 1;
    }

    // check that the key/values were correctly stored
    i = 0;
    for(auto& p : context->reference) {
        if(i % 2 == 1) {
            auto key = p.first.data();
            auto ksize = p.first.size();
            std::vector<char> val(g_max_val_size);
            size_t vsize = g_max_val_size;
            ret = yk_get(dbh, context->mode,key, ksize, val.data(), &vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
            munit_assert_int(vsize, ==, p.second.size());
            munit_assert_memory_equal(vsize, val.data(), p.second.data());
        }
        i += 1;
    }

    // put all the value this time, with YOKAN_MODE_NEW_ONLY, and reverse
    // the ones we initially put
    i = 0;
    for(auto& p : context->reference)
    {
        if(i % 2 == 0) {
            auto key   = p.first.data();
            auto ksize = p.first.size();
            auto val   = p.second.data();
            auto vsize = p.second.size();
            ret = yk_put(dbh, YOKAN_MODE_NEW_ONLY|context->mode, key, ksize, val, vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        } else {
            auto rev_val = p.second;
            std::reverse(rev_val.begin(), rev_val.end());
            auto key   = p.first.data();
            auto ksize = p.first.size();
            auto val   = rev_val.data();
            auto vsize = rev_val.size();
            ret = yk_put(dbh, YOKAN_MODE_NEW_ONLY|context->mode, key, ksize, val, vsize);
            SKIP_IF_NOT_IMPLEMENTED(ret);
            munit_assert_int(ret, ==, YOKAN_SUCCESS);
        }
    }

    // check that only the keys that previously did not exist were added
    // and the ones that did exist were not modified
    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        auto val = p.second.data();
        auto vsize = p.second.size();
        std::vector<char> out_val(g_max_val_size);
        size_t out_vsize = g_max_val_size;
        ret = yk_get(dbh, context->mode, key, ksize, out_val.data(), &out_vsize);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_int(out_vsize, ==, vsize);
        munit_assert_memory_equal(out_vsize, val, out_val.data());
    }

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
  { (char*)"num-keyvals", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    /* put tests */
    { (char*) "/put", test_put,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put/empty-keys", test_put_empty_keys,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    /* put_multi tests */
    { (char*) "/put_multi", test_put_multi,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put_multi/all-empty-values", test_put_multi_all_empty_values,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put_multi/empty-key", test_put_multi_empty_key,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    /* put_packed tests */
    { (char*) "/put_packed", test_put_packed,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put_packed/all-empty-values", test_put_packed_all_empty_values,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put_packed/empty-key", test_put_packed_empty_key,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    /* put_bulk tests */
    { (char*) "/put_bulk", test_put_bulk,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put_bulk/all-empty-values", test_put_bulk_all_empty_values,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put_bulk/empty-key", test_put_bulk_empty_key,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    /* mode tests */
    { (char*) "/put/append", test_put_append,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put/exist_only", test_put_exist_only,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/put/new_only", test_put_new_only,
        kv_test_common_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

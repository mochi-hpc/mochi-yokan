/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <functional>
#include <numeric>
#include <vector>
#include <iostream>
#include <array>

yk_return_t dummy(void* uargs, size_t i,
                  const void* kdata, size_t ksize,
                  const void* vdata, size_t vsize) {
        (void)uargs;
        (void)i;
        (void)kdata;
        (void)ksize;
        (void)vdata;
        (void)vsize;
        return YOKAN_SUCCESS;
}

static void* test_fetch_context_setup(const MunitParameter params[], void* user_data)
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

    yk_put_multi(context->dbh, context->mode, count,
                 kptrs.data(), ksizes.data(),
                 vptrs.data(), vsizes.data());

    return context;
}

/**
 * @brief Check that we can fetch key/value pairs from the reference map.
 */
static MunitResult test_fetch(const MunitParameter params[], void* data)
{
    (void)data;
    (void)params;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    for(auto& p : context->reference) {
        auto key = p.first.data();
        auto ksize = p.first.size();
        using pair_type = decltype(context->reference)::value_type;
        auto func = [](void* uargs, size_t i,
                       const void* kdata, size_t ksize,
                       const void* vdata, size_t vsize) {
            (void)i;
            pair_type* p = (pair_type*)uargs;
            auto& key = p->first;
            auto& val = p->second;
            munit_assert_int(ksize, ==, key.size());
            munit_assert_int(vsize, ==, val.size());
            munit_assert_memory_equal(ksize, key.data(), kdata);
            munit_assert_memory_equal(vsize, val.data(), vdata);
            return YOKAN_SUCCESS;
        };
        ret = yk_fetch(dbh, context->mode, key, ksize, func, (void*)&p);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we correctely detect that a key does not exists
 */
static MunitResult test_fetch_key_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    auto key = std::string("XXXXXXXXXXXX");
    auto ksize = key.size();

    size_t vsize = 0;
    auto func = [](void* uargs, size_t i,
                   const void* kdata, size_t ksize,
                   const void* vdata, size_t vsize) {
        (void)i;
        (void)kdata;
        (void)ksize;
        (void)vdata;
        auto vsize_ptr = (size_t*)uargs;
        *vsize_ptr = vsize;
        return YOKAN_SUCCESS;
    };

    ret = yk_fetch(dbh, context->mode, key.data(), ksize, func, &vsize);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);
    munit_assert_long(vsize, ==, YOKAN_KEY_NOT_FOUND);

    return MUNIT_OK;
}

/**
 * @brief Check that fetching an empty key leads to an error.
 */
static MunitResult test_fetch_empty_keys(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    ret = yk_fetch(dbh, context->mode, "abc", 0, dummy, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    ret = yk_fetch(dbh, context->mode, nullptr, 0, dummy, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    ret = yk_fetch(dbh, context->mode, "abc", 3, nullptr, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can fetch the key/value pairs from the
 * reference map using fetch_multi, and that fetch_multi also accepts
 * a count of 0.
 */
static MunitResult test_fetch_multi(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();

    struct func_args {
        std::vector<std::string> recv_keys;
        std::vector<std::string> recv_values;
    };

    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;

    kptrs.reserve(count);
    ksizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        i += 1;
    }

    auto func = [](void* uargs, size_t i,
                   const void* kdata, size_t ksize,
                   const void* vdata, size_t vsize) {
        (void)i;
        auto args = (func_args*)uargs;
        munit_assert_int(i, ==, args->recv_keys.size());
        args->recv_keys.emplace_back((const char*)kdata, ksize);
        args->recv_values.emplace_back((const char*)vdata, vsize);
        return YOKAN_SUCCESS;
    };

    func_args args;
    ret = yk_fetch_multi(dbh, context->mode, count,
                         kptrs.data(), ksizes.data(),
                         func, &args, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    munit_assert_size(args.recv_values.size(), ==, context->reference.size());
    i = 0;
    for(auto& p : context->reference) {
        auto& val   = args.recv_values[i];
        munit_assert_long(val.size(), ==, p.second.size());
        munit_assert_memory_equal(val.size(), val.data(), p.second.data());
        i += 1;
    }

    // check with all NULL

    ret = yk_fetch_multi(dbh, context->mode, 0, nullptr, nullptr, func, nullptr, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if one key is empty, the function correctly fails.
 */
static MunitResult test_fetch_multi_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();
    std::vector<std::vector<char>> values(count);
    for(auto& v : values) v.resize(g_max_val_size);
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;

    kptrs.reserve(count);
    ksizes.reserve(count);

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

    ret = yk_fetch_multi(dbh, context->mode, count,
                         kptrs.data(), ksizes.data(),
                         dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // other invalid args tests
    ret = yk_fetch_multi(dbh, context->mode, count,
                         nullptr, ksizes.data(),
                         dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_fetch_multi(dbh, context->mode, count,
                         kptrs.data(), nullptr,
                         dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_fetch_multi(dbh, context->mode, count,
                         kptrs.data(), ksizes.data(),
                         nullptr, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

/**
 * @brief Check that we can fetch the key/value pairs from the
 * reference map using fetch_multi, and that if a key is not found
 * the value size is properly set to YOKAN_KEY_NOT_FOUND.
 */
static MunitResult test_fetch_multi_key_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();
    std::vector<std::string> keys(count);
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;

    kptrs.reserve(count);
    ksizes.reserve(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        if(i % 3 == 0)
            keys[i] = "XXXXXXXXXXXX";
        else
            keys[i]    = p.first;
        auto key   = keys[i].data();
        auto ksize = keys[i].size();
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        i += 1;
    }

    struct func_args {
        std::vector<std::string> recv_keys;
        std::vector<std::string> recv_vals;
        std::vector<size_t> recv_valsizes;
    };

    auto func = [](void* uargs, size_t i,
                   const void* kdata, size_t ksize,
                   const void* vdata, size_t vsize) {
        (void)i;
        auto args = (func_args*)uargs;
        munit_assert_int(i, ==, args->recv_keys.size());
        args->recv_keys.emplace_back((const char*)kdata, ksize);
        args->recv_valsizes.push_back(vsize);
        if(vsize <= YOKAN_LAST_VALID_SIZE)
            args->recv_vals.emplace_back((const char*)vdata, vsize);
        else
            args->recv_vals.emplace_back();
        return YOKAN_SUCCESS;
    };

    func_args args;

    ret = yk_fetch_multi(dbh, context->mode, count,
                         kptrs.data(), ksizes.data(),
                         func, &args, &options);
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
            munit_assert_long(vsize, ==, p.second.size());
            munit_assert_memory_equal(vsize, val.data(), p.second.data());
        }
        i += 1;
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we can fetch the key/value pairs from the
 * reference map using fetch_packed, and that fetch_multi also accepts
 * a count of 0.
 */
static MunitResult test_fetch_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);

    unsigned i = 0;
    for(auto& p : context->reference) {
        packed_keys += p.first;
        packed_ksizes[i] = p.first.size();
        i += 1;
    }
    struct func_args {
        std::vector<std::string> recv_keys;
        std::vector<std::string> recv_values;
    };

    auto func = [](void* uargs, size_t i,
                   const void* kdata, size_t ksize,
                   const void* vdata, size_t vsize) {
        (void)i;
        auto args = (func_args*)uargs;
        munit_assert_int(i, ==, args->recv_keys.size());
        munit_assert_size(vsize, <=, YOKAN_LAST_VALID_SIZE);
        args->recv_keys.emplace_back((const char*)kdata, ksize);
        args->recv_values.emplace_back((const char*)vdata, vsize);
        return YOKAN_SUCCESS;
    };

    func_args args;
    ret = yk_fetch_packed(dbh, context->mode, count,
                          packed_keys.data(), packed_ksizes.data(),
                          func, &args, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    munit_assert_size(args.recv_values.size(), ==, context->reference.size());
    i = 0;
    for(auto& p : context->reference) {
        auto& val   = args.recv_values[i];
        munit_assert_long(val.size(), ==, p.second.size());
        munit_assert_memory_equal(val.size(), val.data(), p.second.data());
        i += 1;
    }

    // check with all NULL

    ret = yk_fetch_packed(dbh, context->mode, 0, nullptr, nullptr, func, nullptr, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    return MUNIT_OK;
}

/**
 * @brief Check that if a key has a size of 0, we get an error.
 */
static MunitResult test_fetch_packed_empty_key(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);

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

    ret = yk_fetch_packed(dbh, context->mode, count,
                          packed_keys.data(), packed_ksizes.data(),
                          dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    // other invalid args tests
    ret = yk_fetch_packed(dbh, context->mode, count,
                          nullptr, packed_ksizes.data(),
                          dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_fetch_packed(dbh, context->mode, count,
                          packed_keys.data(), nullptr,
                          dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);
    ret = yk_fetch_packed(dbh, context->mode, count,
                          packed_keys.data(), packed_ksizes.data(),
                          nullptr, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    for(auto& s : packed_ksizes) s = 0;

    ret = yk_fetch_packed(dbh, context->mode, count,
                          packed_keys.data(), packed_ksizes.data(),
                          dummy, nullptr, nullptr);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    return MUNIT_OK;
}

static MunitResult test_fetch_packed_key_not_found(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();
    std::string         packed_keys;
    std::vector<size_t> packed_ksizes(count);

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

    struct func_args {
        std::vector<std::string> recv_keys;
        std::vector<std::string> recv_vals;
        std::vector<size_t> recv_valsizes;
    };

    auto func = [](void* uargs, size_t i,
                   const void* kdata, size_t ksize,
                   const void* vdata, size_t vsize) {
        (void)i;
        auto args = (func_args*)uargs;
        munit_assert_int(i, ==, args->recv_keys.size());
        args->recv_keys.emplace_back((const char*)kdata, ksize);
        args->recv_valsizes.push_back(vsize);
        if(vsize <= YOKAN_LAST_VALID_SIZE) {
            args->recv_vals.emplace_back((const char*)vdata, vsize);
        } else {
            args->recv_vals.emplace_back();
        }
        return YOKAN_SUCCESS;
    };

    func_args args;

    ret = yk_fetch_packed(dbh, context->mode, count,
                          packed_keys.data(), packed_ksizes.data(),
                          func, &args, &options);
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
            munit_assert_long(vsize, ==, p.second.size());
            munit_assert_memory_equal(vsize, val.data(), p.second.data());
        }
        i += 1;
    }

    return MUNIT_OK;
}

/**
 * @brief Check that we can use fetch_bulk to fetch the key/value
 * pairs from the reference map. We use either null as the origin
 * address, or this process' address, to exercise both code paths.
 */
static MunitResult test_fetch_bulk(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    struct kv_test_context* context = (struct kv_test_context*)data;
    yk_database_handle_t dbh = context->dbh;
    yk_return_t ret;
    hg_return_t hret;
    hg_bulk_t bulk;

    const char* use_pool_str   = munit_parameters_get(params, "use-pool");
    const char* batch_size_str = munit_parameters_get(params, "batch-size");

    yk_fetch_options_t options;
    if(strcmp(use_pool_str, "true") == 0)
        margo_get_progress_pool(context->mid, &options.pool);
    else
        options.pool = ABT_POOL_NULL;
    options.batch_size = atol(batch_size_str);

    auto count = context->reference.size();
    std::string         pkeys;
    std::vector<size_t> ksizes;

    ksizes.reserve(count);

    for(auto& p : context->reference) {
        auto& key  = p.first;
        auto ksize = p.first.size();
        pkeys += key;
        ksizes.push_back(ksize);
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

    ret = yk_fetch_bulk(dbh, context->mode, count, addr_str, bulk,
                        garbage_size, useful_size, dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_fetch_bulk(dbh, context->mode, count, nullptr, bulk,
                        garbage_size, useful_size, dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_SUCCESS);

    ret = yk_fetch_bulk(dbh, context->mode, count, "invalid-address", bulk,
                        garbage_size, useful_size, dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_FROM_MERCURY);

    /* third invalid size of 0 */
    ret = yk_fetch_bulk(dbh, context->mode, count, nullptr, bulk,
                        garbage_size, 0, dummy, nullptr, &options);
    SKIP_IF_NOT_IMPLEMENTED(ret);
    munit_assert_int(ret, ==, YOKAN_ERR_INVALID_ARGS);

    hret = margo_bulk_free(bulk);
    munit_assert_int(hret, ==, HG_SUCCESS);

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
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
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
    { (char*) "/fetch", test_fetch,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_default_params },
    { (char*) "/fetch/empty-keys", test_fetch_empty_keys,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_default_params },
    { (char*) "/fetch/key-not-found", test_fetch_key_not_found,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_default_params },
    { (char*) "/fetch_multi", test_fetch_multi,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { (char*) "/fetch_multi/empty-key", test_fetch_multi_empty_key,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { (char*) "/fetch_multi/key-not-found", test_fetch_multi_key_not_found,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { (char*) "/fetch_packed", test_fetch_packed,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { (char*) "/fetch_packed/empty-key", test_fetch_packed_empty_key,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { (char*) "/fetch_packed/key-not-found", test_fetch_packed_key_not_found,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { (char*) "/fetch_bulk", test_fetch_bulk,
        test_fetch_context_setup, kv_test_common_context_tear_down, MUNIT_TEST_OPTION_NONE, test_multi_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

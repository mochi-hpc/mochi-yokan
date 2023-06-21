/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <numeric>
#include <vector>
#include <cstring>
#include <iostream>
#include <map>

inline bool starts_with(const std::string& s, const std::string& prefix) {
    if(s.size() < prefix.size()) return false;
    if(prefix.size() == 0) return true;
    if(std::memcmp(s.data(), prefix.data(), prefix.size()) == 0) return true;
    return false;
}

struct iter_context {
    kv_test_context*                  base;
    std::map<std::string,std::string> ordered_ref;
    std::string                       prefix;
};

static void* test_iter_context_setup(const MunitParameter params[], void* user_data)
{
    auto base_context = static_cast<kv_test_context*>(
        kv_test_common_context_setup(params, user_data));

    auto context = new iter_context;
    context->base = base_context;

    auto prefix = munit_parameters_get(params, "prefix");
    context->prefix = prefix ? prefix : "";
    g_max_key_size += context->prefix.size(); // important!
    context->base->mode |= to_bool(munit_parameters_get(params, "inclusive")) ? YOKAN_MODE_INCLUSIVE : 0;

    // modify the key/value pairs in the reference to add a prefix in half of the keys
    unsigned i = 0;
    for(auto& p : base_context->reference) {
        if(i % 2 == 0) {
            context->ordered_ref[context->prefix + p.first] = p.second;
        } else {
            context->ordered_ref[p.first] = p.second;
        }
        i += 1;
    }
    base_context->reference.clear();

    auto count = context->ordered_ref.size();
    std::vector<const void*> kptrs;
    std::vector<size_t>      ksizes;
    std::vector<const void*> vptrs;
    std::vector<size_t>      vsizes;

    kptrs.reserve(count);
    ksizes.reserve(count);
    vptrs.reserve(count);
    vsizes.reserve(count);

    for(auto& p : context->ordered_ref) {
        auto key   = p.first.data();
        auto ksize = p.first.size();
        auto val   = p.second.data();
        auto vsize = p.second.size();
        kptrs.push_back(key);
        ksizes.push_back(ksize);
        vptrs.push_back(val);
        vsizes.push_back(vsize);
    }

    yk_put_multi(base_context->dbh, context->base->mode, count,
                  kptrs.data(), ksizes.data(),
                  vptrs.data(), vsizes.data());
    return context;
}

static void test_iter_context_tear_down(void* user_data)
{
    auto context = static_cast<iter_context*>(user_data);
    kv_test_common_context_tear_down(context->base);
    delete context;
}

static MunitResult test_iter(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<iter_context*>(data);
    yk_database_handle_t dbh = context->base->dbh;
    yk_return_t ret;

    std::vector<std::string> expected_keys;
    std::vector<std::string> expected_vals;

    for(auto& p : context->ordered_ref) {
        auto& key = p.first;
        auto& val = p.second;
        if(starts_with(key, context->prefix)) {
            expected_keys.push_back(key);
            expected_vals.push_back(val);
        }
    }

    auto count = atol(munit_parameters_get(params, "keys-per-op"));
    std::string from_key;
    std::string prefix = context->prefix;

    auto no_values = to_bool(munit_parameters_get(params, "no-values"));

    struct iter_context {
        std::vector<std::string> recv_key;
        std::vector<std::string> recv_val;
    };

    auto func = [](void* u, size_t i, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
        (void)i;
        auto ctx = (iter_context*)u;
        /* ignore if it's the same as the previous one (happens when using inclusive) */
        if(!ctx->recv_key.empty() && ctx->recv_key.back() == std::string{(const char*)key, ksize})
            return YOKAN_SUCCESS;
        ctx->recv_key.emplace_back((const char*)key, ksize);
        if(val) ctx->recv_val.emplace_back((const char*)val, vsize);
        else ctx->recv_val.emplace_back();
        return YOKAN_SUCCESS;
    };

    iter_context result;

    yk_iter_options_t options;
    options.batch_size = atol(munit_parameters_get(params, "batch-size"));
    if(to_bool(munit_parameters_get(params, "use-pool"))) {
        margo_get_progress_pool(context->base->mid, &options.pool);
    } else {
        options.pool = ABT_POOL_NULL;
    }
    options.ignore_values = no_values;

    while(result.recv_key.size() != expected_keys.size()) {
        ret = yk_iter(dbh, context->base->mode,
                      from_key.data(), from_key.size(),
                      prefix.data(), prefix.size(),
                      count, func, &result, &options);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_size(result.recv_key.size(), >, 0);
        from_key = result.recv_key.back();
    }

    for(unsigned i=0; i < expected_keys.size(); ++i) {
        auto& k_ref = expected_keys[i];
        auto& v_ref = expected_vals[i];
        auto& k = result.recv_key[i];
        auto& v = result.recv_val[i];
        munit_assert_long(k.size(), ==, k_ref.size());
        munit_assert_memory_equal(k.size(), k.data(), k_ref.data());
        if(no_values) {
            munit_assert(v.empty());
        } else {
            munit_assert_long(v.size(), ==, v_ref.size());
            munit_assert_memory_equal(v.size(), v.data(), v_ref.data());
        }
    }

    return MUNIT_OK;
}

static MunitResult test_iter_custom_filter(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<iter_context*>(data);
    yk_database_handle_t dbh = context->base->dbh;
    yk_return_t ret;

    std::vector<std::string> expected_keys;
    std::vector<std::string> expected_vals;

    for(auto& p : context->ordered_ref) {
        auto key = p.first;
        auto& val = p.second;
        // see extra/custom-filters.cpp
        if(val.size() % 2 == ((key.size() % 2 == 0) ? 1 : 0)) {
            for(unsigned i=0; i < key.size()/2; i++) std::swap(key[i], key[key.size()-i-1]);
            expected_keys.push_back(key);
            expected_vals.push_back(val+"I am groot");
        }
    }

    std::string filter = "libcustom-filters.so:custom_kv:I am groot";

    auto count = atol(munit_parameters_get(params, "keys-per-op"));
    std::string from_key;

    auto no_values = to_bool(munit_parameters_get(params, "no-values"));

    struct iter_context {
        std::vector<std::string> recv_key;
        std::vector<std::string> recv_val;
    };

    auto func = [](void* u, size_t i, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
        (void)i;
        auto ctx = (iter_context*)u;
        /* ignore if it's the same as the previous one (happens when using inclusive) */
        if(!ctx->recv_key.empty() && ctx->recv_key.back() == std::string{(const char*)key, ksize})
            return YOKAN_SUCCESS;
        ctx->recv_key.emplace_back((const char*)key, ksize);
        if(val) ctx->recv_val.emplace_back((const char*)val, vsize);
        else ctx->recv_val.emplace_back();
        return YOKAN_SUCCESS;
    };

    iter_context result;

    yk_iter_options_t options;
    options.batch_size = atol(munit_parameters_get(params, "batch-size"));
    if(to_bool(munit_parameters_get(params, "use-pool"))) {
        margo_get_progress_pool(context->base->mid, &options.pool);
    } else {
        options.pool = ABT_POOL_NULL;
    }
    options.ignore_values = no_values;

    while(result.recv_key.size() != expected_keys.size()) {
        ret = yk_iter(dbh, context->base->mode|YOKAN_MODE_LIB_FILTER,
                      from_key.data(), from_key.size(),
                      filter.data(), filter.size(),
                      count, func, &result, &options);
        SKIP_IF_NOT_IMPLEMENTED(ret);
        munit_assert_int(ret, ==, YOKAN_SUCCESS);
        munit_assert_size(result.recv_key.size(), >, 0);
        from_key = result.recv_key.back();
        // the filter reverses the keys so we have to reverse it back
        for(unsigned i=0; i < from_key.size()/2; i++) std::swap(from_key[i], from_key[from_key.size()-i-1]);
    }

    for(unsigned i=0; i < expected_keys.size(); ++i) {
        auto& k_ref = expected_keys[i];
        auto& v_ref = expected_vals[i];
        auto& k = result.recv_key[i];
        auto& v = result.recv_val[i];
        munit_assert_long(k.size(), ==, k_ref.size());
        munit_assert_memory_equal(k.size(), k.data(), k_ref.data());
        if(no_values) {
            munit_assert(v.empty());
        } else {
            munit_assert_long(v.size(), ==, v_ref.size());
            munit_assert_memory_equal(v.size(), v.data(), v_ref.data());
        }
    }

    return MUNIT_OK;
}

static char* true_false_params[] = {
    (char*)"true", (char*)"false", NULL
};

static char* prefix_params[] = {
    (char*)"", (char*)"matt", NULL
};

static char* batch_size_params[] = {
    (char*)"0", (char*)"5", (char*)NULL };

static char* keys_per_op_params[] = {
    (char*)"0", (char*)"12", (char*)NULL };

static MunitParameterEnum test_params[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"no-rdma", (char**)true_false_params },
  { (char*)"no-values", (char**)true_false_params },
  { (char*)"inclusive", (char**)true_false_params },
  { (char*)"batch-size", (char**)batch_size_params },
  { (char*)"use-pool", (char**)true_false_params },
  { (char*)"keys-per-op", (char**)keys_per_op_params },
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-items", NULL },
  { NULL, NULL }
};

static MunitParameterEnum test_params_with_prefix[] = {
  { (char*)"backend", (char**)available_backends },
  { (char*)"no-rdma", (char**)true_false_params },
  { (char*)"no-values", (char**)true_false_params },
  { (char*)"inclusive", (char**)true_false_params },
  { (char*)"batch-size", (char**)batch_size_params },
  { (char*)"prefix", (char**)prefix_params },
  { (char*)"keys-per-op", (char**)keys_per_op_params },
  { (char*)"use-pool", (char**)true_false_params },
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-items", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/iter", test_iter,
        test_iter_context_setup, test_iter_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params_with_prefix },
    { (char*) "/iter/custom_filter", test_iter_custom_filter,
        test_iter_context_setup, test_iter_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/yk/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "yk", argc, argv);
}

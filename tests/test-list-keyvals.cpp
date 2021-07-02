/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "test-common-setup.hpp"
#include <numeric>
#include <vector>
#include <cstring>
#include <iostream>
#include <map>

inline bool to_bool(const char* v) {
    if(v == nullptr)
        return false;
    if(strcmp(v, "true") == 0)
        return true;
    if(strcmp(v, "false") == 0)
        return false;
    return false;
}

inline bool starts_with(const std::string& s, const std::string& prefix) {
    if(s.size() < prefix.size()) return false;
    if(prefix.size() == 0) return true;
    if(std::memcmp(s.data(), prefix.data(), prefix.size()) == 0) return true;
    return false;
}

struct list_keyvals_context {
    test_context*                     base;
    std::map<std::string,std::string> ordered_ref;
    std::string                       prefix;
    bool                              inclusive;
    size_t                            keys_per_op; // max keys per operation
};

static void* test_list_keyvals_context_setup(const MunitParameter params[], void* user_data)
{
    auto base_context = static_cast<test_context*>(
        test_common_context_setup(params, user_data));

    auto context = new list_keyvals_context;
    context->base = base_context;

    context->prefix = munit_parameters_get(params, "prefix");
    g_max_key_size += context->prefix.size(); // important!
    context->inclusive = to_bool(munit_parameters_get(params, "inclusive"));
    const char* keys_per_op_str = munit_parameters_get(params, "keys-per-op");
    context->keys_per_op = keys_per_op_str ? atol(keys_per_op_str) : 6;

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

    rkv_return_t ret;

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

    ret = rkv_put_multi(base_context->dbh, count,
                        kptrs.data(), ksizes.data(),
                        vptrs.data(), vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    return context;
}

static void test_list_keyvals_context_tear_down(void* user_data)
{
    auto context = static_cast<list_keyvals_context*>(user_data);
    test_common_context_tear_down(context->base);
    delete context;
}

static MunitResult test_list_keyvals(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<list_keyvals_context*>(data);
    rkv_database_handle_t dbh = context->base->dbh;
    rkv_return_t ret;

    auto count = context->keys_per_op;
    std::vector<size_t> ksizes(count, g_max_key_size);
    std::vector<size_t> vsizes(count, g_max_val_size);
    std::vector<std::string> keys(count, std::string(g_max_key_size, '\0'));
    std::vector<std::string> vals(count, std::string(g_max_val_size, '\0'));
    std::vector<void*> kptrs(count, nullptr);
    std::vector<void*> vptrs(count, nullptr);
    for(unsigned i = 0; i < count; i++) {
        kptrs[i] = const_cast<char*>(keys[i].data());
        vptrs[i] = const_cast<char*>(vals[i].data());
    }
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

    bool done_listing = false;
    unsigned i = 0;
    std::string from_key;
    std::string prefix = context->prefix;

    while(!done_listing) {
        ret = rkv_list_keyvals(dbh,
                context->inclusive,
                from_key.data(),
                from_key.size(),
                prefix.data(),
                prefix.size(),
                count,
                kptrs.data(),
                ksizes.data(),
                vptrs.data(),
                vsizes.data());
        munit_assert_int(ret, ==, RKV_SUCCESS);

        for(unsigned j = 0; j < count; j++) {
            if(i+j < expected_keys.size()) {
                auto& exp_key = expected_keys[i+j];
                auto& exp_val = expected_vals[i+j];
                munit_assert_long(ksizes[j], ==, exp_key.size());
                munit_assert_memory_equal(ksizes[j], kptrs[j], exp_key.data());
                munit_assert_long(vsizes[j], ==, exp_val.size());
                munit_assert_memory_equal(vsizes[j], vptrs[j], exp_val.data());
                from_key = exp_key;
            } else {
                munit_assert_long(ksizes[j], ==, RKV_NO_MORE_KEYS);
                munit_assert_long(vsizes[j], ==, RKV_NO_MORE_KEYS);
                done_listing = true;
            }
        }
        i += count;
        if(context->inclusive)
            i -= 1;

        ksizes.clear();
        vsizes.clear();
        ksizes.resize(count, g_max_key_size);
        vsizes.resize(count, g_max_val_size);
    }

    return MUNIT_OK;
}

static MunitResult test_list_keyvals_too_small(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<list_keyvals_context*>(data);
    rkv_database_handle_t dbh = context->base->dbh;
    rkv_return_t ret;

    auto count = context->keys_per_op;
    std::vector<size_t> ksizes(count, g_max_key_size);
    std::vector<size_t> vsizes(count, g_max_val_size);
    std::vector<std::string> keys(count, std::string(g_max_key_size, '\0'));
    std::vector<std::string> vals(count, std::string(g_max_val_size, '\0'));
    std::vector<void*> kptrs(count, nullptr);
    std::vector<void*> vptrs(count, nullptr);
    for(unsigned i = 0; i < count; i++) {
        kptrs[i] = const_cast<char*>(keys[i].data());
        vptrs[i] = const_cast<char*>(vals[i].data());
    }

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

    // make one key buffer too small for its key
    for(unsigned i = 0; i < count; i++) {
        auto& key = expected_keys[i];
        auto& val = expected_vals[i];
        if(i == count/2) {
            ksizes[i] = key.size()/2;
        }
        if(i == count/3) {
            vsizes[i] = val.size()/2;
        }
    }

    std::string from_key;
    std::string prefix = context->prefix;

    ret = rkv_list_keyvals(dbh,
                context->inclusive,
                from_key.data(),
                from_key.size(),
                prefix.data(),
                prefix.size(),
                count,
                kptrs.data(),
                ksizes.data(),
                vptrs.data(),
                vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    for(unsigned j = 0; j < count; j++) {
        if(j < expected_keys.size()) {
            auto& exp_key = expected_keys[j];
            auto& exp_val = expected_vals[j];
            if(j == count/2) {
                munit_assert_long(ksizes[j], ==, RKV_SIZE_TOO_SMALL);
                munit_assert_long(vsizes[j], ==, exp_val.size());
                munit_assert_memory_equal(vsizes[j], vptrs[j], exp_val.data());
            } else if(j == count/3) {
                munit_assert_long(ksizes[j], ==, exp_key.size());
                munit_assert_memory_equal(ksizes[j], kptrs[j], exp_key.data());
                if(exp_val.size() != 0)
                    munit_assert_long(vsizes[j], ==, RKV_SIZE_TOO_SMALL);
            } else {
                munit_assert_long(ksizes[j], ==, exp_key.size());
                munit_assert_memory_equal(ksizes[j], kptrs[j], exp_key.data());
                munit_assert_long(vsizes[j], ==, exp_val.size());
                munit_assert_memory_equal(vsizes[j], vptrs[j], exp_val.data());
            }
        } else {
            munit_assert_long(ksizes[j], ==, RKV_NO_MORE_KEYS);
            munit_assert_long(vsizes[j], ==, RKV_NO_MORE_KEYS);
        }
    }
    return MUNIT_OK;
}

static MunitResult test_list_keyvals_packed(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<list_keyvals_context*>(data);
    rkv_database_handle_t dbh = context->base->dbh;
    rkv_return_t ret;

    auto count = context->keys_per_op;
    std::vector<size_t> packed_ksizes(count, g_max_key_size);
    std::vector<size_t> packed_vsizes(count, g_max_val_size);
    std::vector<char> packed_keys(count*g_max_key_size);
    std::vector<char> packed_vals(count*g_max_val_size);
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

    bool done_listing = false;
    unsigned i = 0;
    std::string from_key;
    std::string prefix = context->prefix;

    while(!done_listing) {

        ret = rkv_list_keyvals_packed(dbh,
                context->inclusive,
                from_key.data(),
                from_key.size(),
                prefix.data(),
                prefix.size(),
                count,
                packed_keys.data(),
                count*g_max_key_size,
                packed_ksizes.data(),
                packed_vals.data(),
                count*g_max_val_size,
                packed_vsizes.data());
        munit_assert_int(ret, ==, RKV_SUCCESS);

        size_t key_offset = 0;
        size_t val_offset = 0;
        for(unsigned j = 0; j < count; j++) {
            if(i+j < expected_keys.size()) {
                auto& exp_key = expected_keys[i+j];
                auto& exp_val = expected_vals[i+j];
                auto recv_key = packed_keys.data()+key_offset;
                auto recv_val = packed_vals.data()+val_offset;
                munit_assert_long(packed_ksizes[j], ==, exp_key.size());
                munit_assert_memory_equal(packed_ksizes[j], recv_key, exp_key.data());
                munit_assert_long(packed_vsizes[j], ==, exp_val.size());
                munit_assert_memory_equal(packed_vsizes[j], recv_val, exp_val.data());
                key_offset += exp_key.size();
                val_offset += exp_val.size();
                from_key = exp_key;
            } else {
                munit_assert_long(packed_ksizes[j], ==, RKV_NO_MORE_KEYS);
                munit_assert_long(packed_vsizes[j], ==, RKV_NO_MORE_KEYS);
                done_listing = true;
            }
        }
        i += count;
        if(context->inclusive)
            i -= 1;

        packed_ksizes.clear();
        packed_ksizes.resize(count, g_max_key_size);
        packed_vsizes.clear();
        packed_vsizes.resize(count, g_max_val_size);
    }

    return MUNIT_OK;
}

static MunitResult test_list_keyvals_packed_key_too_small(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<list_keyvals_context*>(data);
    rkv_database_handle_t dbh = context->base->dbh;
    rkv_return_t ret;

    auto count = context->keys_per_op;
    std::vector<size_t> packed_ksizes(count, g_max_key_size);
    std::vector<size_t> packed_vsizes(count, g_max_val_size);
    std::vector<char> packed_keys(count*g_max_key_size);
    std::vector<char> packed_vals(count*g_max_val_size);
    std::vector<std::string> expected_keys;
    std::vector<std::string> expected_vals;

    size_t size_needed_for_keys = 0;
    size_t size_needed_for_vals = 0;
    unsigned i = 0;
    for(auto& p : context->ordered_ref) {
        auto& key = p.first;
        auto& val = p.second;
        if(starts_with(key, context->prefix)) {
            expected_keys.push_back(key);
            expected_vals.push_back(val);
            if(i < count) {
                size_needed_for_keys += key.size();
                size_needed_for_vals += val.size();
            } else
                break;
            i += 1;
        }
    }

    size_t key_buf_size = size_needed_for_keys/2;
    size_t val_buf_size = size_needed_for_vals;

    std::string from_key;
    std::string prefix = context->prefix;

    ret = rkv_list_keyvals_packed(dbh,
            context->inclusive,
            from_key.data(),
            from_key.size(),
            prefix.data(),
            prefix.size(),
            count,
            packed_keys.data(),
            key_buf_size,
            packed_ksizes.data(),
            packed_vals.data(),
            val_buf_size,
            packed_vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    size_t key_offset = 0;
    size_t val_offset = 0;
    bool key_buf_size_reached = false;
    for(unsigned j = 0; j < count; j++) {
        if(j < expected_keys.size()) {
            auto& exp_key = expected_keys[j];
            auto& exp_val = expected_vals[j];
            auto recv_key = packed_keys.data()+key_offset;
            auto recv_val = packed_vals.data()+val_offset;
            if(key_offset + exp_key.size() > key_buf_size || key_buf_size_reached) {
                munit_assert_long(packed_ksizes[j], ==, RKV_SIZE_TOO_SMALL);
                key_buf_size_reached = true;
            } else {
                munit_assert_long(packed_ksizes[j], ==, exp_key.size());
                munit_assert_memory_equal(packed_ksizes[j], recv_key, exp_key.data());
            }
            munit_assert_long(packed_vsizes[j], ==, exp_val.size());
            munit_assert_memory_equal(packed_vsizes[j], recv_val, exp_val.data());
            key_offset += exp_key.size();
            val_offset += exp_val.size();
        } else {
            munit_assert_long(packed_ksizes[j], ==, RKV_NO_MORE_KEYS);
            munit_assert_long(packed_vsizes[j], ==, RKV_NO_MORE_KEYS);
        }
    }
    return MUNIT_OK;
}

static MunitResult test_list_keyvals_packed_val_too_small(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<list_keyvals_context*>(data);
    rkv_database_handle_t dbh = context->base->dbh;
    rkv_return_t ret;

    auto count = context->keys_per_op;
    std::vector<size_t> packed_ksizes(count, g_max_key_size);
    std::vector<size_t> packed_vsizes(count, g_max_val_size);
    std::vector<char> packed_keys(count*g_max_key_size);
    std::vector<char> packed_vals(count*g_max_val_size);
    std::vector<std::string> expected_keys;
    std::vector<std::string> expected_vals;

    size_t size_needed_for_keys = 0;
    size_t size_needed_for_vals = 0;
    unsigned i = 0;
    for(auto& p : context->ordered_ref) {
        auto& key = p.first;
        auto& val = p.second;
        if(starts_with(key, context->prefix)) {
            expected_keys.push_back(key);
            expected_vals.push_back(val);
            if(i < count) {
                size_needed_for_keys += key.size();
                size_needed_for_vals += val.size();
            } else
                break;
            i += 1;
        }
    }

    size_t key_buf_size = size_needed_for_keys;
    size_t val_buf_size = size_needed_for_vals/2;

    std::string from_key;
    std::string prefix = context->prefix;

    ret = rkv_list_keyvals_packed(dbh,
            context->inclusive,
            from_key.data(),
            from_key.size(),
            prefix.data(),
            prefix.size(),
            count,
            packed_keys.data(),
            key_buf_size,
            packed_ksizes.data(),
            packed_vals.data(),
            val_buf_size,
            packed_vsizes.data());
    munit_assert_int(ret, ==, RKV_SUCCESS);

    size_t key_offset = 0;
    size_t val_offset = 0;
    bool val_buf_size_reached = false;
    for(unsigned j = 0; j < count; j++) {
        if(j < expected_keys.size()) {
            auto& exp_key = expected_keys[j];
            auto& exp_val = expected_vals[j];
            auto recv_key = packed_keys.data()+key_offset;
            auto recv_val = packed_vals.data()+val_offset;
            munit_assert_long(packed_ksizes[j], ==, exp_key.size());
            munit_assert_memory_equal(packed_ksizes[j], recv_key, exp_key.data());
            if(val_offset + exp_val.size() > val_buf_size || val_buf_size_reached) {
                munit_assert_long(packed_vsizes[j], ==, RKV_SIZE_TOO_SMALL);
                val_buf_size_reached = true;
            } else {
                munit_assert_long(packed_vsizes[j], ==, exp_val.size());
                munit_assert_memory_equal(packed_vsizes[j], recv_val, exp_val.data());
            }
            key_offset += exp_key.size();
            val_offset += exp_val.size();
        } else {
            munit_assert_long(packed_ksizes[j], ==, RKV_NO_MORE_KEYS);
            munit_assert_long(packed_vsizes[j], ==, RKV_NO_MORE_KEYS);
        }
    }
    return MUNIT_OK;
}

static MunitResult test_list_keyvals_bulk(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    auto context = static_cast<list_keyvals_context*>(data);
    rkv_database_handle_t dbh = context->base->dbh;
    rkv_return_t ret;
    hg_return_t hret;

    auto count = context->keys_per_op;
    std::vector<size_t> packed_ksizes(count, g_max_key_size);
    std::vector<size_t> packed_vsizes(count, g_max_val_size);
    std::vector<char> packed_keys(count*g_max_key_size);
    std::vector<char> packed_vals(count*g_max_val_size);

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

    char addr_str[256];
    hg_size_t addr_str_size = 256;
    hret = margo_addr_to_string(context->base->mid,
            addr_str, &addr_str_size, context->base->addr);

    bool done_listing = false;
    unsigned i = 0;
    std::string from_key;
    std::string prefix = context->prefix;

    std::vector<char> garbage(42);
    hg_size_t garbage_size = 42;

    while(!done_listing) {

        hg_bulk_t data = HG_BULK_NULL;
        {
            std::vector<void*> ptrs = { garbage.data() };
            std::vector<hg_size_t> sizes = { garbage_size };
            if(!from_key.empty()) {
                ptrs.push_back(const_cast<char*>(from_key.data()));
                sizes.push_back(from_key.size());
            }
            if(!prefix.empty()) {
                ptrs.push_back(const_cast<char*>(prefix.data()));
                sizes.push_back(prefix.size());
            }
            ptrs.push_back(packed_ksizes.data());
            sizes.push_back(count*sizeof(size_t));
            ptrs.push_back(packed_vsizes.data());
            sizes.push_back(count*sizeof(size_t));
            ptrs.push_back(packed_keys.data());
            sizes.push_back(packed_keys.size());
            ptrs.push_back(packed_vals.data());
            sizes.push_back(packed_vals.size());

            hret = margo_bulk_create(
                    context->base->mid,
                    ptrs.size(),
                    ptrs.data(),
                    sizes.data(),
                    HG_BULK_READWRITE,
                    &data);
            munit_assert_int(hret, ==, HG_SUCCESS);
        }

        ret = rkv_list_keyvals_bulk(dbh,
                context->inclusive,
                from_key.size(),
                prefix.size(),
                addr_str, data,
                garbage_size,
                packed_keys.size(),
                packed_vals.size(),
                true, count);
        munit_assert_int(ret, ==, RKV_SUCCESS);

        hret = margo_bulk_free(data);
        munit_assert_int(hret, ==, HG_SUCCESS);

        size_t key_offset = 0;
        size_t val_offset = 0;
        for(unsigned j = 0; j < count; j++) {
            if(i+j < expected_keys.size()) {
                auto& exp_key = expected_keys[i+j];
                auto& exp_val = expected_vals[i+j];
                auto recv_key = packed_keys.data()+key_offset;
                auto recv_val = packed_vals.data()+val_offset;
                munit_assert_long(packed_ksizes[j], ==, exp_key.size());
                munit_assert_long(packed_vsizes[j], ==, exp_val.size());
                munit_assert_memory_equal(packed_ksizes[j], recv_key, exp_key.data());
                munit_assert_memory_equal(packed_vsizes[j], recv_val, exp_val.data());
                key_offset += exp_key.size();
                val_offset += exp_val.size();
                from_key = exp_key;
            } else {
                munit_assert_long(packed_ksizes[j], ==, RKV_NO_MORE_KEYS);
                munit_assert_long(packed_vsizes[j], ==, RKV_NO_MORE_KEYS);
                done_listing = true;
            }
        }
        i += count;
        if(context->inclusive)
            i -= 1;

        packed_ksizes.clear();
        packed_vsizes.clear();
        packed_ksizes.resize(count, g_max_key_size);
        packed_vsizes.resize(count, g_max_val_size);
    }

    return MUNIT_OK;
}

static char* inclusive_params[] = {
    (char*)"true", (char*)"false", NULL
};

static char* prefix_params[] = {
    (char*)"", (char*)"matt", NULL
};

static MunitParameterEnum test_params[] = {
  { (char*)"inclusive", inclusive_params },
  { (char*)"prefix", prefix_params },
  { (char*)"min-key-size", NULL },
  { (char*)"max-key-size", NULL },
  { (char*)"min-val-size", NULL },
  { (char*)"max-val-size", NULL },
  { (char*)"num-keyvals", NULL },
  { (char*)"keys-per-op", NULL },
  { NULL, NULL }
};

static MunitTest test_suite_tests[] = {
    { (char*) "/list_keyvals", test_list_keyvals,
        test_list_keyvals_context_setup, test_list_keyvals_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/list_keyvals/too_small", test_list_keyvals_too_small,
        test_list_keyvals_context_setup, test_list_keyvals_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/list_keyvals_packed", test_list_keyvals_packed,
        test_list_keyvals_context_setup, test_list_keyvals_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/list_keyvals_packed/keys_too_small", test_list_keyvals_packed_key_too_small,
        test_list_keyvals_context_setup, test_list_keyvals_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/list_keyvals_packed/vals_too_small", test_list_keyvals_packed_val_too_small,
        test_list_keyvals_context_setup, test_list_keyvals_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { (char*) "/list_keyvals_bulk", test_list_keyvals_bulk,
        test_list_keyvals_context_setup, test_list_keyvals_context_tear_down, MUNIT_TEST_OPTION_NONE, test_params },
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    (char*) "/rkv/database", test_suite_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};

int main(int argc, char* argv[MUNIT_ARRAY_PARAM(argc + 1)]) {
    return munit_suite_main(&test_suite, (void*) "rkv", argc, argv);
}

/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/server.h"
#include "provider.hpp"
#include "../common/types.h"
#include "../common/defer.hpp"
#include "../common/logging.h"
#include "../common/checks.h"
#include <numeric>
#include <iostream>

void yk_iter_ult(hg_handle_t h)
{
    hg_return_t hret;
    iter_in_t in;
    iter_out_t out;

    std::memset(&in, 0, sizeof(in));
    out.ret = YOKAN_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    yk_provider_t provider = (yk_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(in.batch_size == 0)
        in.batch_size = in.count;

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    struct previous_op {
        std::vector<size_t> ksizes;
        std::vector<size_t> vsizes;
        std::vector<char>   keyvals;
        hg_handle_t         handle = HG_HANDLE_NULL;
        hg_bulk_t           bulk   = HG_BULK_NULL;
        margo_request       req    = MARGO_REQUEST_NULL;
    };

    previous_op previous;

    uint64_t            num_keyvals_sent = 0;
    std::vector<size_t> ksizes; ksizes.reserve(in.batch_size);
    std::vector<size_t> vsizes; vsizes.reserve(in.batch_size);
    std::vector<char>   keyvals;

    auto wait_for_previous_rpc = [&]() -> yk_return_t {
        hg_return_t hret = HG_SUCCESS;
        DEFER(margo_destroy(previous.handle); previous.handle = HG_HANDLE_NULL;);
        DEFER(margo_bulk_free(previous.bulk); previous.bulk = HG_BULK_NULL;);
        if(previous.handle == HG_HANDLE_NULL) return YOKAN_SUCCESS;
        hret = margo_wait(previous.req);
        CHECK_HRET(hret, margo_wait);
        iter_back_out_t back_out;
        hret = margo_get_output(previous.handle, &back_out);
        CHECK_HRET(hret, margo_get_output);
        DEFER(margo_free_output(previous.handle, &back_out));
        return (yk_return_t)back_out.ret;
    };

    auto send_batch = [&]() -> yk_return_t {

        if(ksizes.size() == 0)
            return YOKAN_SUCCESS;

        std::array<void*, 3>     buffer_ptrs  = {(void*)ksizes.data(), (void*)vsizes.data(), (void*)keyvals.data()};
        std::array<hg_size_t, 3> buffer_sizes = {ksizes.size()*sizeof(size_t), vsizes.size()*sizeof(size_t), keyvals.size()};
        hg_bulk_t local_bulk                  = HG_BULK_NULL;
        hret = margo_bulk_create(mid, 3, buffer_ptrs.data(), buffer_sizes.data(), HG_BULK_READ_ONLY, &local_bulk);
        CHECK_HRET(hret, margo_bulk_create);
        DEFER(margo_bulk_free(local_bulk));

        iter_back_in_t back_in;
        back_in.op_ref = in.op_ref;
        back_in.start  = num_keyvals_sent;
        back_in.count  = ksizes.size();
        back_in.size   = std::accumulate(buffer_sizes.begin(), buffer_sizes.end(), (size_t)0);
        back_in.bulk   = local_bulk;

        auto ret       = wait_for_previous_rpc();
        if(ret != YOKAN_SUCCESS)
            return ret;

        // send a iter_back RPC to the client
        hg_handle_t back_handle = HG_HANDLE_NULL;
        hret = margo_create(mid, info->addr, provider->iter_back_id, &back_handle);
        CHECK_HRET(hret, margo_create);
        DEFER(margo_destroy(back_handle));

        margo_request req = MARGO_REQUEST_NULL;

        hret = margo_iforward(back_handle, &back_in, &req);
        CHECK_HRET(hret, margo_iforward);

        num_keyvals_sent += ksizes.size();

        previous.keyvals = std::move(keyvals);
        previous.ksizes  = std::move(ksizes);
        previous.vsizes  = std::move(vsizes);
        margo_ref_incr(back_handle);
        previous.handle = back_handle;
        margo_bulk_ref_incr(local_bulk);
        previous.bulk   = local_bulk;
        previous.req    = req;

        return YOKAN_SUCCESS;
    };

    auto iter_func = [&](const yokan::UserMem& key, const yokan::UserMem& val) -> yokan::Status {
        auto v = in.no_values ? yokan::UserMem{nullptr, 0} : val;
        ksizes.push_back(key.size);
        vsizes.push_back(v.size);
        auto current_size = keyvals.size();
        keyvals.resize(current_size + key.size + v.size);
        std::memcpy(keyvals.data() + current_size, key.data, key.size);
        std::memcpy(keyvals.data() + current_size + key.size, v.data, v.size);
        if(ksizes.size() == in.batch_size)
            return (yokan::Status)send_batch();
        return yokan::Status::OK;
    };

    auto from_key = yokan::UserMem{in.from_key.data, in.from_key.size};
    auto filter_umem = yokan::UserMem{in.filter.data, in.filter.size };
    auto filter      = yokan::FilterFactory::makeKeyValueFilter(mid, in.mode, filter_umem);

    out.ret = static_cast<yk_return_t>(
            database->iter(in.mode, in.count, from_key, filter, in.no_values, iter_func));

    auto ret = send_batch();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
    ret = wait_for_previous_rpc();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
}
DEFINE_MARGO_RPC_HANDLER(yk_iter_ult)

void yk_iter_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    iter_in_t in;
    iter_out_t out;

    std::memset(&in, 0, sizeof(in));
    out.ret = YOKAN_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    const struct hg_info* info = margo_get_info(h);
    yk_provider_t provider = (yk_provider_t)margo_registered_data(mid, info->id);
    CHECK_PROVIDER(provider);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    if(in.batch_size == 0)
        in.batch_size = in.count;

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    struct previous_op {
        hg_handle_t   handle = HG_HANDLE_NULL;
        margo_request req    = MARGO_REQUEST_NULL;
    };

    previous_op previous;

    uint64_t            num_keyvals_sent = 0;
    std::vector<size_t> ksizes; ksizes.reserve(in.batch_size);
    std::vector<size_t> vsizes; vsizes.reserve(in.batch_size);
    std::vector<char>   keyvals;

    auto wait_for_previous_rpc = [&]() -> yk_return_t {
        hg_return_t hret = HG_SUCCESS;
        DEFER(margo_destroy(previous.handle); previous.handle = HG_HANDLE_NULL;);
        if(previous.handle == HG_HANDLE_NULL) return YOKAN_SUCCESS;
        hret = margo_wait(previous.req);
        CHECK_HRET(hret, margo_wait);
        iter_direct_back_out_t back_out;
        hret = margo_get_output(previous.handle, &back_out);
        CHECK_HRET(hret, margo_get_output);
        DEFER(margo_free_output(previous.handle, &back_out));
        return (yk_return_t)back_out.ret;
    };

    auto send_batch = [&]() -> yk_return_t {

        if(ksizes.size() == 0)
            return YOKAN_SUCCESS;

        iter_direct_back_in_t back_in;
        back_in.op_ref = in.op_ref;
        back_in.start  = num_keyvals_sent;
        back_in.ksizes.count = ksizes.size();
        back_in.ksizes.sizes = ksizes.data();
        back_in.vsizes.count = vsizes.size();
        back_in.vsizes.sizes = vsizes.data();
        back_in.keyvals.data = keyvals.data();
        back_in.keyvals.size = keyvals.size();

        auto ret = wait_for_previous_rpc();
        if(ret != YOKAN_SUCCESS)
            return ret;

        // send a iter_back RPC to the client
        hg_handle_t back_handle = HG_HANDLE_NULL;
        hret = margo_create(mid, info->addr, provider->iter_back_id, &back_handle);
        CHECK_HRET(hret, margo_create);
        DEFER(margo_destroy(back_handle));

        margo_request req = MARGO_REQUEST_NULL;

        hret = margo_iforward(back_handle, &back_in, &req);
        CHECK_HRET(hret, margo_iforward);

        num_keyvals_sent += ksizes.size();

        ksizes.clear();
        vsizes.clear();
        keyvals.clear();

        margo_ref_incr(back_handle);
        previous.handle = back_handle;
        previous.req    = req;

        return YOKAN_SUCCESS;
    };

    auto iter_func = [&](const yokan::UserMem& key, const yokan::UserMem& val) -> yokan::Status {
        auto v = in.no_values ? yokan::UserMem{nullptr, 0} : val;
        ksizes.push_back(key.size);
        vsizes.push_back(v.size);
        auto current_size = keyvals.size();
        keyvals.resize(current_size + key.size + v.size);
        std::memcpy(keyvals.data() + current_size, key.data, key.size);
        std::memcpy(keyvals.data() + current_size + key.size, v.data, v.size);
        if(ksizes.size() == in.batch_size)
            return (yokan::Status)send_batch();
        return yokan::Status::OK;
    };

    auto from_key    = yokan::UserMem{in.from_key.data, in.from_key.size};
    auto filter_umem = yokan::UserMem{in.filter.data, in.filter.size };
    auto filter      = yokan::FilterFactory::makeKeyValueFilter(mid, in.mode, filter_umem);

    out.ret = static_cast<yk_return_t>(
            database->iter(in.mode, in.count, from_key, filter, in.no_values, iter_func));

    auto ret = send_batch();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
    ret = wait_for_previous_rpc();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
}
DEFINE_MARGO_RPC_HANDLER(yk_iter_direct_ult)

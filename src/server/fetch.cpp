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

void yk_fetch_ult(hg_handle_t h)
{
    hg_return_t hret;
    fetch_in_t in;
    std::memset(&in, 0, sizeof(in));
    fetch_out_t out;
    hg_addr_t origin_addr = HG_ADDR_NULL;

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

    size_t num_batches = (size_t)std::ceil((double)in.count/(double)in.batch_size);

    if(in.origin) {
        hret = margo_addr_lookup(mid, in.origin, &origin_addr);
        CHECK_HRET_OUT(hret, margo_addr_lookup);
    } else {
        hret = margo_addr_dup(mid, info->addr, &origin_addr);
        CHECK_HRET_OUT(hret, margo_addr_dup);
    }
    DEFER(margo_addr_free(mid, origin_addr));

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    yk_buffer_t keys_buffer = provider->bulk_cache.get(
        provider->bulk_cache_data, in.size, HG_BULK_WRITE_ONLY);
    CHECK_BUFFER(keys_buffer);
    DEFER(provider->bulk_cache.release(provider->bulk_cache_data, keys_buffer));

    size_t keys_offset = in.count*sizeof(size_t);

    // transfer ksizes
    size_t sizes_to_transfer = in.count*sizeof(size_t);

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
            in.bulk, in.offset, keys_buffer->bulk, 0, sizes_to_transfer);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    // build buffer wrappers for key sizes
    auto ksizes_ptr  = reinterpret_cast<size_t*>(keys_buffer->data);
    auto total_ksize = std::accumulate(ksizes_ptr, ksizes_ptr+in.count, (size_t)0);

    // check that there is no key of size 0
    auto min_key_size = std::accumulate(ksizes_ptr, ksizes_ptr + in.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    // check that the total_ksize found is consistent with in.size
    if(in.size < keys_offset + total_ksize) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    // transfer the actual keys from the client
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
            in.bulk, in.offset + keys_offset,
            keys_buffer->bulk, keys_offset, total_ksize);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    struct previous_op {
        std::vector<char>   values;
        std::vector<size_t> vsizes;
        hg_handle_t         handle = HG_HANDLE_NULL;
        hg_bulk_t           bulk   = HG_BULK_NULL;
        margo_request       req    = MARGO_REQUEST_NULL;
    };

    previous_op previous;

    auto wait_for_previous_rpc = [&previous, &mid]() -> yk_return_t {
        hg_return_t hret = HG_SUCCESS;
        DEFER(margo_destroy(previous.handle); previous.handle = HG_HANDLE_NULL;);
        DEFER(margo_bulk_free(previous.bulk); previous.bulk = HG_BULK_NULL;);
        if(previous.handle == HG_HANDLE_NULL) return YOKAN_SUCCESS;
        hret = margo_wait(previous.req);
        CHECK_HRET(hret, margo_wait);
        fetch_back_out_t back_out;
        hret = margo_get_output(previous.handle, &back_out);
        CHECK_HRET(hret, margo_get_output);
        DEFER(margo_free_output(previous.handle, &back_out));
        return (yk_return_t)back_out.ret;
    };

    for(unsigned batch_index = 0; batch_index < num_batches; ++batch_index) {

        // create UserMem wrapper for ksizes for this batch
        auto ksizes = yokan::BasicUserMem<size_t>{
            ksizes_ptr + in.batch_size*batch_index,
            std::min<size_t>(in.batch_size, in.count - batch_index*in.batch_size)};

        auto total_batch_ksize = std::accumulate(
            ksizes.data, ksizes.data+ksizes.size, (size_t)0);

        // create UserMem wrapper for keys for this batch
        auto keys = yokan::UserMem{ ((char*)ksizes_ptr) + keys_offset, total_batch_ksize };

        // create buffers to hold the values and value sizes
        std::vector<char>   values;
        std::vector<size_t> vsizes;
        vsizes.reserve(in.batch_size);

        auto fetcher = [&values, &vsizes](const yokan::UserMem& key, const yokan::UserMem& val) -> yokan::Status {
            (void)key;
            vsizes.push_back(val.size);
            if(val.size != YOKAN_KEY_NOT_FOUND) {
                size_t current_size = values.size();
                if(values.capacity() < current_size + val.size)
                    values.reserve(values.capacity()*2);
                values.resize(current_size + val.size);
                std::memcpy(values.data() + current_size, val.data, val.size);
            }
            return yokan::Status::OK;
        };

        out.ret = static_cast<yk_return_t>(
                database->fetch(in.mode, keys, ksizes, fetcher));
        if(out.ret != YOKAN_SUCCESS)
            break;

        std::array<void*, 2>     values_ptr   = {(void*)vsizes.data(), (void*)values.data()};
        std::array<hg_size_t, 2> values_sizes = {vsizes.size()*sizeof(size_t), values.size()};
        hg_bulk_t values_bulk  = HG_BULK_NULL;
        if(values_sizes[1])
            hret = margo_bulk_create(mid, 2, values_ptr.data(), values_sizes.data(), HG_BULK_READ_ONLY, &values_bulk);
        else
            hret = margo_bulk_create(mid, 1, values_ptr.data(), values_sizes.data(), HG_BULK_READ_ONLY, &values_bulk);
        CHECK_HRET_OUT_GOTO(hret, margo_bulk_create, finish);
        DEFER(margo_bulk_free(values_bulk));

        fetch_back_in_t back_in;
        back_in.op_ref = in.op_ref;
        back_in.start  = batch_index*in.batch_size;
        back_in.count  = ksizes.size;
        back_in.size   = std::accumulate(values_sizes.begin(), values_sizes.end(), (size_t)0);
        back_in.bulk   = values_bulk;

        out.ret = wait_for_previous_rpc();
        if(out.ret != YOKAN_SUCCESS)
            break;

        // send a fetch_back RPC to the client

        hg_handle_t back_handle = HG_HANDLE_NULL;
        hret = margo_create(mid, info->addr, provider->fetch_back_id, &back_handle);
        CHECK_HRET_OUT_GOTO(hret, margo_create, finish);
        DEFER(margo_destroy(back_handle));

        margo_request req = MARGO_REQUEST_NULL;

        hret = margo_iforward(back_handle, &back_in, &req);
        CHECK_HRET_OUT_GOTO(hret, margo_iforward, finish);

        previous.values = std::move(values);
        previous.vsizes = std::move(vsizes);
        margo_ref_incr(back_handle);
        previous.handle = back_handle;
        margo_bulk_ref_incr(values_bulk);
        previous.bulk   = values_bulk;
        previous.req    = req;

        keys_offset += total_batch_ksize;
    }

finish:
    auto ret = wait_for_previous_rpc();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
    return;
}
DEFINE_MARGO_RPC_HANDLER(yk_fetch_ult)

void yk_fetch_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    fetch_direct_in_t in;
    fetch_direct_out_t out;

    std::memset(&in, 0, sizeof(in));
    std::memset(&out, 0, sizeof(out));

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

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    auto count = in.ksizes.count;
    auto ksizes_umem = yokan::BasicUserMem<size_t>{
        in.ksizes.sizes, count };
    auto keys_umem = yokan::UserMem{
        in.keys.data, in.keys.size };

    // check that there is no key of size 0
    auto min_key_size = std::accumulate(in.ksizes.sizes, in.ksizes.sizes + in.ksizes.count,
                                        std::numeric_limits<size_t>::max(),
                                        [](const size_t& lhs, const size_t& rhs) {
                                            return std::min(lhs, rhs);
                                        });
    if(min_key_size == 0) {
        out.ret = YOKAN_ERR_INVALID_ARGS;
        return;
    }

    fetch_direct_back_in_t back_in;
    fetch_direct_back_out_t back_out;

    std::vector<char>   values;
    std::vector<size_t> vsizes;
    vsizes.reserve(count);

    back_in.op_ref = in.op_ref;
    back_in.start  = 0;

    auto fetcher = [&values, &vsizes](const yokan::UserMem& key, const yokan::UserMem& val) -> yokan::Status {
        (void)key;
        vsizes.push_back(val.size);
        if(val.size != YOKAN_KEY_NOT_FOUND) {
            size_t current_size = values.size();
            if(values.capacity() < current_size + val.size)
                values.reserve(values.capacity()*2);
            values.resize(current_size + val.size);
            std::memcpy(values.data() + current_size, val.data, val.size);
        }
        return yokan::Status::OK;
    };

    out.ret = static_cast<yk_return_t>(
        database->fetch(in.mode, keys_umem, ksizes_umem, fetcher));

    if(out.ret != YOKAN_SUCCESS)
        return;

    back_in.vsizes.count = count;
    back_in.vsizes.sizes = vsizes.data();
    back_in.vals.size    = values.size();
    back_in.vals.data    = values.data();

    // send a fetch_direct_back RPC to the client

    hg_handle_t back_handle = HG_HANDLE_NULL;
    hret = margo_create(mid, info->addr, provider->fetch_direct_back_id, &back_handle);
    CHECK_HRET_OUT(hret, margo_create);
    DEFER(margo_destroy(back_handle));

    hret = margo_forward(back_handle, &back_in);
    CHECK_HRET_OUT(hret, margo_forward);

    hret = margo_get_output(back_handle, &back_out);
    CHECK_HRET_OUT(hret, margo_get_output);
    DEFER(margo_free_output(back_handle, &back_out));

    out.ret = back_out.ret;
}
DEFINE_MARGO_RPC_HANDLER(yk_fetch_direct_ult)

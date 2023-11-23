/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <array>
#include <numeric>
#include <cstring>
#include <iostream>
#include "client.hpp"
#include "../common/defer.hpp"
#include "../common/types.h"
#include "../common/logging.h"
#include "../common/checks.h"

struct iter_context {
    yk_keyvalue_callback_t cb;
    void*                  uargs;
    yk_iter_options_t      options;
};

yk_return_t yk_iter(yk_database_handle_t dbh,
                    int32_t mode,
                    const void* from_key,
                    size_t from_ksize,
                    const void* filter,
                    size_t filter_size,
                    size_t count,
                    yk_keyvalue_callback_t cb,
                    void* uargs,
                    const yk_iter_options_t* options)
{
    if(!cb)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;

    if(!margo_is_listening(mid))
        return YOKAN_ERR_MID_NOT_LISTENING;

    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    iter_in_t in;
    iter_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    iter_context context;
    context.cb      = cb;
    context.uargs   = uargs;
    if(options) {
        context.options.batch_size    = options->batch_size;
        context.options.pool          = options->pool;
        context.options.ignore_values = options->ignore_values;
    } else {
        context.options.batch_size    = 0;
        context.options.pool          = ABT_POOL_NULL;
        context.options.ignore_values = false;
    }

    in.mode          = mode;
    in.no_values     = context.options.ignore_values;
    in.batch_size    = context.options.batch_size;
    in.count         = count;
    in.from_key.data = (char*)from_key;
    in.from_key.size = from_ksize;
    in.filter.data   = (char*)filter;
    in.filter.size   = filter_size;
    in.op_ref        = reinterpret_cast<uint64_t>(&context);

    hret = margo_create(mid, dbh->addr,
        mode & YOKAN_MODE_NO_RDMA ? dbh->client->iter_direct_id : dbh->client->iter_id, &handle);
    CHECK_HRET(hret, margo_create);
    DEFER(margo_destroy(handle));

    hret = margo_provider_forward(dbh->provider_id, handle, &in);
    CHECK_HRET(hret, margo_provider_forward);

    hret = margo_get_output(handle, &out);
    CHECK_HRET(hret, margo_get_output);

    ret = static_cast<yk_return_t>(out.ret);
    hret = margo_free_output(handle, &out);
    CHECK_HRET(hret, margo_free_output);

    return ret;
}

void yk_iter_back_ult(hg_handle_t h)
{

    hg_return_t hret = HG_SUCCESS;
    iter_back_in_t in;
    iter_back_out_t out;

    std::memset(&in, 0, sizeof(in));
    std::memset(&out, 0, sizeof(out));

    out.ret = YOKAN_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    auto info = margo_get_info(h);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    iter_context* context = reinterpret_cast<iter_context*>(in.op_ref);

    // create bulk for the keys and values
    std::vector<size_t>     ksizes(in.count);
    std::vector<size_t>     vsizes(in.count);
    std::vector<char>       buffer(in.size - 2*in.count*sizeof(size_t));

    std::array<void*,3>     buffer_ptrs  = { ksizes.data(), vsizes.data(), buffer.data() };
    std::array<hg_size_t,3> buffer_sizes = { ksizes.size()*sizeof(size_t), vsizes.size()*sizeof(size_t), buffer.size() };
    hg_bulk_t local_bulk  = HG_BULK_NULL;
    hret = margo_bulk_create(mid, 3, buffer_ptrs.data(), buffer_sizes.data(), HG_BULK_WRITE_ONLY, &local_bulk);
    CHECK_HRET_OUT(hret, margo_bulk_create);
    DEFER(margo_bulk_free(local_bulk));

    // pull the data
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.bulk, 0, local_bulk, 0, in.size);
    CHECK_HRET_OUT(hret, margo_bulk_transfer);

    const auto& opt = context->options;
    ABT_pool pool = opt.pool ? opt.pool : ABT_POOL_NULL;

    struct ult_args {
        yk_keyvalue_callback_t cb;
        void*                  uargs;
        unsigned               index;
        const void*            key;
        size_t                 ksize;
        const void*            val;
        size_t                 vsize;
        yk_return_t            ret;
    };

    std::vector<ABT_thread> ults;
    std::vector<ult_args>   args(in.count);

    if(pool != ABT_POOL_NULL)
        ults.resize(in.count);

    auto ult = [](void* a) -> void {
        auto arg = (ult_args*)a;
        arg->ret = (arg->cb)(
            arg->uargs, arg->index,
            arg->key, arg->ksize,
            arg->val, arg->vsize);
    };

    size_t offset = 0;

    for(unsigned i = 0; i < in.count; ++i) {
        args[i].cb    = context->cb;
        args[i].uargs = context->uargs;
        args[i].index = in.start + i;
        args[i].key   = buffer.data() + offset;
        args[i].ksize = ksizes[i];
        args[i].val   = buffer.data() + offset + ksizes[i];
        args[i].vsize = vsizes[i];
        if(pool == ABT_POOL_NULL) {
            ult(&args[i]);
            if(args[i].ret != YOKAN_SUCCESS) {
                out.ret = args[i].ret;
                break;
            }
        } else {
            ABT_thread_create(pool, ult, &args[i], ABT_THREAD_ATTR_NULL, &ults[i]);
        }
        offset += ksizes[i] + vsizes[i];
    }

    if(pool != ABT_POOL_NULL) {
        ABT_thread_join_many(ults.size(), ults.data());
        ABT_thread_free_many(ults.size(), ults.data());
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_iter_back_ult)

void yk_iter_direct_back_ult(hg_handle_t h)
{
    hg_return_t hret = HG_SUCCESS;
    iter_direct_back_in_t in;
    iter_direct_back_out_t out;

    std::memset(&in, 0, sizeof(in));
    std::memset(&out, 0, sizeof(out));

    out.ret = YOKAN_SUCCESS;

    DEFER(margo_destroy(h));
    DEFER(margo_respond(h, &out));

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    CHECK_MID(mid, margo_hg_handle_get_instance);

    hret = margo_get_input(h, &in);
    CHECK_HRET_OUT(hret, margo_get_input);
    DEFER(margo_free_input(h, &in));

    iter_context* context = reinterpret_cast<iter_context*>(in.op_ref);

    const auto& opt = context->options;
    ABT_pool pool = opt.pool ? opt.pool : ABT_POOL_NULL;

    struct ult_args {
        yk_keyvalue_callback_t cb;
        void*                  uargs;
        unsigned               index;
        const void*            key;
        size_t                 ksize;
        const void*            val;
        size_t                 vsize;
        yk_return_t            ret;
    };

    std::vector<ABT_thread> ults;
    std::vector<ult_args>   args(in.vsizes.count);

    if(pool != ABT_POOL_NULL)
        ults.resize(in.vsizes.count);

    auto ult = [](void* a) -> void {
        auto arg = (ult_args*)a;
        arg->ret = (arg->cb)(
            arg->uargs, arg->index,
            arg->key, arg->ksize,
            arg->val, arg->vsize);
    };

    size_t offset = 0;
    for(unsigned i = 0; i < in.vsizes.count; ++i) {
        args[i].cb    = context->cb;
        args[i].uargs = context->uargs;
        args[i].index = in.start + i;
        args[i].key   = ((const char*)in.keyvals.data) + offset;
        args[i].ksize = in.ksizes.sizes[i];
        args[i].val   = ((const char*)in.keyvals.data) + offset + in.ksizes.sizes[i];
        args[i].vsize = in.vsizes.sizes[i];
        if(pool == ABT_POOL_NULL) {
            ult(&args[i]);
            if(args[i].ret != YOKAN_SUCCESS) {
                out.ret = args[i].ret;
                break;
            }
        } else {
            ABT_thread_create(pool, ult, &args[i], ABT_THREAD_ATTR_NULL, &ults[i]);
        }
        offset += args[i].ksize + args[i].vsize;
    }

    if(pool != ABT_POOL_NULL) {
        ABT_thread_join_many(ults.size(), ults.data());
        ABT_thread_free_many(ults.size(), ults.data());
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_iter_direct_back_ult)

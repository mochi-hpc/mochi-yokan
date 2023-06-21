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

struct doc_iter_context {
    yk_document_callback_t cb;
    void*                  uargs;
    yk_doc_iter_options_t  options;
};

yk_return_t yk_doc_iter(yk_database_handle_t dbh,
                        const char* collection,
                        int32_t mode,
                        yk_id_t from_id,
                        const void* filter,
                        size_t filter_size,
                        size_t max,
                        yk_document_callback_t cb,
                        void* uargs,
                        const yk_doc_iter_options_t* options)
{
    if(!cb)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;

    if(!margo_is_listening(mid))
        return YOKAN_ERR_MID_NOT_LISTENING;

    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_iter_in_t in;
    doc_iter_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    doc_iter_context context;
    context.cb      = cb;
    context.uargs   = uargs;
    if(options) {
        context.options.batch_size    = options->batch_size;
        context.options.pool          = options->pool;
    } else {
        context.options.batch_size    = 0;
        context.options.pool          = ABT_POOL_NULL;
    }

    in.db_id        = dbh->database_id;
    in.coll_name    = (char*)collection;
    in.mode         = mode;
    in.batch_size   = context.options.batch_size;
    in.count        = max;
    in.from_id      = from_id;
    in.filter.data  = (char*)filter;
    in.filter.size  = filter_size;
    in.op_ref       = reinterpret_cast<uint64_t>(&context);

    hret = margo_create(mid, dbh->addr,
        mode & YOKAN_MODE_NO_RDMA ? dbh->client->doc_iter_direct_id : dbh->client->doc_iter_id, &handle);
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

void yk_doc_iter_back_ult(hg_handle_t h)
{

    hg_return_t hret = HG_SUCCESS;
    doc_iter_back_in_t in;
    doc_iter_back_out_t out;

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

    doc_iter_context* context = reinterpret_cast<doc_iter_context*>(in.op_ref);

    // create bulk for the ids and values
    std::vector<yk_id_t>  ids(in.count);
    std::vector<size_t>   docsizes(in.count);
    std::vector<char>     docs(in.size - in.count*(sizeof(size_t) + sizeof(yk_id_t)));

    std::array<void*,3>     buffer_ptrs  = { ids.data(), docsizes.data(), docs.data() };
    std::array<hg_size_t,3> buffer_sizes = { ids.size()*sizeof(yk_id_t), docsizes.size()*sizeof(size_t), docs.size() };
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
        yk_document_callback_t cb;
        void*                  uargs;
        unsigned               index;
        yk_id_t                id;
        const void*            doc;
        size_t                 docsize;
        yk_return_t            ret;
    };

    std::vector<ABT_thread> ults;
    std::vector<ult_args>   args(in.count);

    if(pool != ABT_POOL_NULL)
        ults.resize(in.count);

    auto ult = [](void* a) -> void {
        auto arg = (ult_args*)a;
        arg->ret = (arg->cb)(
            arg->uargs, arg->index, arg->id,
            arg->doc, arg->docsize);
    };

    size_t doc_offset = 0;

    for(unsigned i = 0; i < in.count; ++i) {
        args[i].cb      = context->cb;
        args[i].uargs   = context->uargs;
        args[i].index   = in.start + i;
        args[i].id      = ids[i];
        args[i].doc     = docs.data() + doc_offset;
        args[i].docsize = docsizes[i];
        if(pool == ABT_POOL_NULL) {
            ult(&args[i]);
            if(args[i].ret != YOKAN_SUCCESS) {
                out.ret = args[i].ret;
                break;
            }
        } else {
            ABT_thread_create(pool, ult, &args[i], ABT_THREAD_ATTR_NULL, &ults[i]);
        }
        doc_offset += docsizes[i];
    }

    if(pool != ABT_POOL_NULL) {
        ABT_thread_join_many(ults.size(), ults.data());
        ABT_thread_free_many(ults.size(), ults.data());
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_iter_back_ult)

void yk_doc_iter_direct_back_ult(hg_handle_t h)
{
    hg_return_t hret = HG_SUCCESS;
    doc_iter_direct_back_in_t in;
    doc_iter_direct_back_out_t out;

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

    doc_iter_context* context = reinterpret_cast<doc_iter_context*>(in.op_ref);

    const auto& opt = context->options;
    ABT_pool pool = opt.pool ? opt.pool : ABT_POOL_NULL;

    struct ult_args {
        yk_document_callback_t cb;
        void*                  uargs;
        unsigned               index;
        yk_id_t                id;
        const void*            doc;
        size_t                 docsize;
        yk_return_t            ret;
    };

    std::vector<ABT_thread> ults;
    std::vector<ult_args>   args(in.ids.count);

    if(pool != ABT_POOL_NULL)
        ults.resize(in.ids.count);

    auto ult = [](void* a) -> void {
        auto arg = (ult_args*)a;
        arg->ret = (arg->cb)(
            arg->uargs, arg->index,
            arg->id, arg->doc, arg->docsize);
    };

    size_t doc_offset = 0;
    for(unsigned i = 0; i < in.ids.count; ++i) {
        args[i].cb      = context->cb;
        args[i].uargs   = context->uargs;
        args[i].index   = in.start + i;
        args[i].id      = in.ids.ids[i];
        args[i].doc     = ((const char*)in.docs.data) + doc_offset;
        args[i].docsize = in.doc_sizes.sizes[i];
        if(pool == ABT_POOL_NULL) {
            ult(&args[i]);
            if(args[i].ret != YOKAN_SUCCESS) {
                out.ret = args[i].ret;
                break;
            }
        } else {
            ABT_thread_create(pool, ult, &args[i], ABT_THREAD_ATTR_NULL, &ults[i]);
        }
        doc_offset += args[i].docsize;
    }

    if(pool != ABT_POOL_NULL) {
        ABT_thread_join_many(ults.size(), ults.data());
        ABT_thread_free_many(ults.size(), ults.data());
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_iter_direct_back_ult)

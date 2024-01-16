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

struct doc_iter_context_base {
    margo_instance_id            mid;
    void*                        uargs;
    const yk_doc_iter_options_t* options;
};

struct doc_iter_context {
    doc_iter_context_base  base;
    yk_document_callback_t doc_cb;
};

struct doc_iter_bulk_context {
    doc_iter_context_base       base;
    yk_document_bulk_callback_t bulk_cb;
};

static yk_return_t invoke_callback_on_docs(
        ABT_pool pool, size_t count, size_t start, const yk_id_t* ids,
        const size_t* doc_sizes, const char* doc_data, yk_document_callback_t cb,
        void* uargs)
{
    struct ult_args {
        yk_document_callback_t cb;
        void*                  uargs;
        unsigned               index;
        yk_id_t                id;
        const void*            doc;
        size_t                 doc_size;
        yk_return_t            ret;
    };

    std::vector<ABT_thread> ults;
    std::vector<ult_args>   args(count);

    if(pool != ABT_POOL_NULL)
        ults.resize(count);

    auto ult = [](void* a) -> void {
        auto arg = (ult_args*)a;
        arg->ret = (arg->cb)(
                arg->uargs, arg->index,
                arg->id, arg->doc, arg->doc_size);
    };

    yk_return_t ret = YOKAN_SUCCESS;

    size_t doc_offset = 0;
    for(unsigned i = 0; i < count; ++i) {
        args[i].cb       = cb;
        args[i].uargs    = uargs;
        args[i].index    = start + i;
        args[i].id       = ids[i];
        args[i].doc      = doc_data + doc_offset;
        args[i].doc_size = doc_sizes[i];
        if(pool == ABT_POOL_NULL) {
            ult(&args[i]);
            if(args[i].ret != YOKAN_SUCCESS) {
                ret = args[i].ret;
                break;
            }
        } else {
            ABT_thread_create(pool, ult, &args[i], ABT_THREAD_ATTR_NULL, &ults[i]);
        }
        doc_offset += args[i].doc_size <= YOKAN_LAST_VALID_SIZE ? args[i].doc_size : 0;
    }

    if(pool != ABT_POOL_NULL) {
        ABT_thread_join_many(ults.size(), ults.data());
        ABT_thread_free_many(ults.size(), ults.data());
    }

    return ret;
}

static yk_return_t bulk_to_docs(
        doc_iter_context* context, size_t start, size_t count,
        hg_bulk_t bulk, hg_addr_t addr, size_t size)
{
    auto mid     = context->base.mid;
    auto hret    = HG_SUCCESS;

    // create bulk for the documents
    std::vector<yk_id_t>    ids(count);
    std::vector<size_t>     docsizes(count);
    std::vector<char>       docs(size - count*(sizeof(size_t) + sizeof(yk_id_t)));
    std::array<void*,3>     buffer_ptrs  = { ids.data(), docsizes.data(), docs.data() };
    std::array<hg_size_t,3> buffer_sizes = { ids.size()*sizeof(yk_id_t), docsizes.size()*sizeof(size_t), docs.size() };
    hg_bulk_t local_bulk  = HG_BULK_NULL;
    hret = margo_bulk_create(mid, 3, buffer_ptrs.data(), buffer_sizes.data(), HG_BULK_WRITE_ONLY, &local_bulk);
    if(hret != HG_SUCCESS) {
        YOKAN_LOG_ERROR(mid, "margo_bulk_create returned %d", hret);
        return YOKAN_ERR_FROM_MERCURY;
    }
    DEFER(margo_bulk_free(local_bulk));

    // pull the documents
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, addr, bulk, 0, local_bulk, 0, size);
    if(hret != HG_SUCCESS) {
        YOKAN_LOG_ERROR(mid, "margo_bulk_transfer returned %d", hret);
        return YOKAN_ERR_FROM_MERCURY;
    }

    const auto opt = context->base.options;
    ABT_pool pool = opt && opt->pool ? opt->pool : ABT_POOL_NULL;

    return invoke_callback_on_docs(
            pool, count, start, ids.data(), docsizes.data(),
            docs.data(), context->doc_cb, context->base.uargs);
}

static yk_return_t doc_iter_base(yk_database_handle_t dbh,
                                 const char* collection,
                                 int32_t mode,
                                 yk_id_t from_id,
                                 const void* filter,
                                 size_t filter_size,
                                 size_t max,
                                 void* cb,
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

    doc_iter_bulk_context context;
    context.base.mid     = mid;
    context.bulk_cb      = reinterpret_cast<decltype(context.bulk_cb)>(cb);
    context.base.uargs   = uargs;
    context.base.options = options;

    in.coll_name    = (char*)collection;
    in.mode         = mode;
    in.batch_size   = options ? options->batch_size : 0;
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

yk_return_t yk_doc_iter_bulk(yk_database_handle_t dbh,
                             const char* collection,
                             int32_t mode,
                             yk_id_t from_id,
                             const void* filter,
                             size_t filter_size,
                             size_t max,
                             yk_document_bulk_callback_t cb,
                             void* uargs,
                             const yk_doc_iter_options_t* options)
{
    if(mode & YOKAN_MODE_NO_RDMA)
        return YOKAN_ERR_MODE;
    return doc_iter_base(
        dbh, collection, mode, from_id, filter,
        filter_size, max, (void*)cb, uargs, options);
}

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

    if(mode & YOKAN_MODE_NO_RDMA) {

        return doc_iter_base(
                dbh, collection, mode, from_id,
                filter, filter_size, max, (void*)cb,
                uargs, options);

    } else {

        doc_iter_context context;
        context.base.mid     = dbh->client->mid;
        context.base.uargs   = uargs;
        context.base.options = options;
        context.doc_cb       = cb;

        return doc_iter_base(
                dbh, collection, mode, from_id,
                filter, filter_size, max, (void*)bulk_to_docs,
                &context, options);

    }
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

    auto context = reinterpret_cast<doc_iter_bulk_context*>(in.op_ref);

    const auto opt = context->base.options;
    ABT_pool pool = opt && opt->pool ? opt->pool : ABT_POOL_NULL;

    struct ult_args {
        doc_iter_bulk_context* context;
        doc_iter_back_in_t*    in;
        doc_iter_back_out_t*   out;
        hg_addr_t              addr;
    };

    ABT_thread ult;
    ult_args   args{context, &in, &out, info->addr};

    auto ult_fn = [](void* a) -> void {
        auto arg = (ult_args*)a;
        arg->out->ret = (arg->context->bulk_cb)(
            arg->context->base.uargs,
            arg->in->start, arg->in->count,
            arg->in->bulk, arg->addr, arg->in->size);
    };

    if(pool == ABT_POOL_NULL) {
        ult_fn(&args);
    } else {
        ABT_thread_create(pool, ult_fn, &args, ABT_THREAD_ATTR_NULL, &ult);
    }

    if(pool != ABT_POOL_NULL) {
        ABT_thread_join(ult);
        ABT_thread_free(&ult);
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

    const auto& opt = context->base.options;
    ABT_pool pool = opt && opt->pool ? opt->pool : ABT_POOL_NULL;

    out.ret = invoke_callback_on_docs(
        pool, in.ids.count, in.start,
        in.ids.ids, in.doc_sizes.sizes,
        in.docs.data, context->doc_cb, context->base.uargs);
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_iter_direct_back_ult)

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

struct doc_fetch_context_base {
    margo_instance_id             mid;
    size_t                        count;
    const yk_id_t*                ids;
    void*                         uargs;
    const yk_doc_fetch_options_t* options;
};

struct doc_fetch_context {
    doc_fetch_context_base base;
    yk_document_callback_t cb;
};

struct doc_fetch_bulk_context {
    doc_fetch_context_base      base;
    yk_document_bulk_callback_t cb;
};

static yk_return_t doc_fetch_base(yk_database_handle_t dbh,
                                  const char* collection,
                                  int32_t mode,
                                  size_t count,
                                  const yk_id_t* ids,
                                  void* cb,
                                  void* uargs,
                                  const yk_doc_fetch_options_t* options)
{
    if(count == 0)
        return YOKAN_SUCCESS;
    else if(!ids || !cb)
        return YOKAN_ERR_INVALID_ARGS;

    CHECK_MODE_VALID(mode);

    margo_instance_id mid = dbh->client->mid;

    if(!margo_is_listening(mid))
        return YOKAN_ERR_MID_NOT_LISTENING;

    yk_return_t ret = YOKAN_SUCCESS;
    hg_return_t hret = HG_SUCCESS;
    doc_fetch_in_t in;
    doc_fetch_out_t out;
    hg_handle_t handle = HG_HANDLE_NULL;

    doc_fetch_bulk_context context;
    context.base.mid     = dbh->client->mid;
    context.base.count   = count;
    context.base.ids     = ids;
    context.base.uargs   = uargs;
    context.base.options = options;
    context.cb           = reinterpret_cast<decltype(context.cb)>(cb);

    in.mode       = mode;
    in.batch_size = options ? options->batch_size : 0;
    in.coll_name  = (char*)collection;
    in.ids.ids    = (yk_id_t*)ids;
    in.ids.count  = count;
    in.op_ref     = reinterpret_cast<uint64_t>(&context);

    hret = margo_create(mid, dbh->addr, dbh->client->doc_fetch_id, &handle);
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

extern "C" yk_return_t yk_doc_fetch_bulk(yk_database_handle_t dbh,
                                         const char* collection,
                                         int32_t mode,
                                         size_t count,
                                         const yk_id_t* ids,
                                         yk_document_bulk_callback_t cb,
                                         void* uargs,
                                         const yk_doc_fetch_options_t* options)
{
    return doc_fetch_base(dbh, collection, mode, count, ids, (void*)cb, uargs, options);
}

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
    doc_fetch_context* context, size_t start, size_t count,
    hg_bulk_t bulk, hg_addr_t addr, size_t size)
{
    auto mid     = context->base.mid;
    auto hret    = HG_SUCCESS;

    // create bulk for the documents
    std::vector<size_t>     doc_sizes(count);
    std::vector<char>       docs(size - count*sizeof(size_t));
    std::array<void*,2>     docs_ptrs = { doc_sizes.data(), docs.data() };
    std::array<hg_size_t,2> docs_sizes = { doc_sizes.size()*sizeof(size_t), docs.size() };
    hg_bulk_t docs_bulk  = HG_BULK_NULL;
    hret = margo_bulk_create(mid, 2, docs_ptrs.data(), docs_sizes.data(), HG_BULK_WRITE_ONLY, &docs_bulk);
    if(hret != HG_SUCCESS) {
        YOKAN_LOG_ERROR(mid, "margo_bulk_create returned %d", hret);
        return YOKAN_ERR_FROM_MERCURY;
    }
    DEFER(margo_bulk_free(docs_bulk));

    // pull the documents
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, addr, bulk, 0, docs_bulk, 0, size);
    if(hret != HG_SUCCESS) {
        YOKAN_LOG_ERROR(mid, "margo_bulk_transfer returned %d", hret);
        return YOKAN_ERR_FROM_MERCURY;
    }

    auto opt = context->base.options;
    ABT_pool pool = (opt && opt->pool && opt->pool != ABT_POOL_NULL) ?
                    opt->pool : ABT_POOL_NULL;

    return invoke_callback_on_docs(
            pool, count, start, context->base.ids + start, doc_sizes.data(),
            docs.data(), context->cb, context->base.uargs);
}

extern "C" yk_return_t yk_doc_fetch_multi(yk_database_handle_t dbh,
                                          const char* collection,
                                          int32_t mode,
                                          size_t count,
                                          const yk_id_t* ids,
                                          yk_document_callback_t cb,
                                          void* uargs,
                                          const yk_doc_fetch_options_t* options)
{
    if(mode & YOKAN_MODE_NO_RDMA) {

        return doc_fetch_base(
            dbh, collection, mode, count,
            ids, (void*)cb, uargs, options);

    } else {

        doc_fetch_context context;
        context.base.mid     = dbh->client->mid;
        context.base.count   = count;
        context.base.ids     = ids;
        context.base.uargs   = uargs;
        context.base.options = options;
        context.cb           = cb;

        return doc_fetch_base(
            dbh, collection, mode, count,
            ids, (void*)bulk_to_docs, &context, options);
    }
}

extern "C" yk_return_t yk_doc_fetch(yk_database_handle_t dbh,
                                    const char* collection,
                                    int32_t mode,
                                    yk_id_t id,
                                    yk_document_callback_t cb,
                                    void* uargs)
{
    return yk_doc_fetch_multi(dbh, collection, mode, 1, &id, cb, uargs, nullptr);
}

void yk_doc_fetch_back_ult(hg_handle_t h)
{
    hg_return_t hret = HG_SUCCESS;
    doc_fetch_back_in_t in;
    doc_fetch_back_out_t out;

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

    doc_fetch_bulk_context* context = reinterpret_cast<doc_fetch_bulk_context*>(in.op_ref);

    if(context->base.count < in.count) {
        out.ret = YOKAN_ERR_OTHER; // should not be happening
        return;
    }

    struct ult_args {
        doc_fetch_bulk_context* context;
        doc_fetch_back_in_t*    in;
        doc_fetch_back_out_t*   out;
        hg_addr_t               addr;
    };

    auto ult_fn = [](void* a) -> void {
        auto args = reinterpret_cast<ult_args*>(a);
        args->out->ret = args->context->cb(
            args->context->base.uargs,
            args->in->start,
            args->in->count,
            args->in->bulk,
            args->addr,
            args->in->size);
    };

    ult_args ult_args = {
        context, &in, &out, info->addr
    };

    const auto opt = context->base.options;
    ABT_pool pool = (opt && opt->pool && opt->pool != ABT_POOL_NULL) ?
                    opt->pool : ABT_POOL_NULL;

    if(pool == ABT_POOL_NULL) {
        ult_fn(static_cast<void*>(&ult_args));
    } else {
        ABT_thread ult = ABT_THREAD_NULL;
        ABT_thread_create(
            pool, ult_fn,
            static_cast<void*>(&ult_args),
            ABT_THREAD_ATTR_NULL, &ult);
        ABT_thread_join(ult);
        ABT_thread_free(&ult);
    }
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_fetch_back_ult)

void yk_doc_fetch_direct_back_ult(hg_handle_t h)
{
    hg_return_t hret = HG_SUCCESS;
    doc_fetch_direct_back_in_t in;
    doc_fetch_back_out_t out;

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

    doc_fetch_context* context = reinterpret_cast<doc_fetch_context*>(in.op_ref);

    if(context->base.count < in.doc_sizes.count) {
        out.ret = YOKAN_ERR_OTHER; // should not be happening
        return;
    }

    const auto opt = context->base.options;
    ABT_pool pool = (opt && opt->pool && opt->pool != ABT_POOL_NULL) ?
                    opt->pool : ABT_POOL_NULL;

    out.ret = invoke_callback_on_docs(
        pool, in.doc_sizes.count,
        in.start, context->base.ids + in.start,
        in.doc_sizes.sizes, in.docs.data, context->cb,
        context->base.uargs);
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_fetch_direct_back_ult)

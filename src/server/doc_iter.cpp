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

void yk_doc_iter_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_iter_in_t in;
    doc_iter_out_t out;

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

    yk_database* database = provider->db;
    CHECK_DATABASE(database);
    CHECK_MODE_SUPPORTED(database, in.mode);

    struct previous_op {
        std::vector<yk_id_t> ids;
        std::vector<size_t>  docsizes;
        std::vector<char>    docs;
        hg_handle_t          handle = HG_HANDLE_NULL;
        hg_bulk_t            bulk   = HG_BULK_NULL;
        margo_request        req    = MARGO_REQUEST_NULL;
    };

    previous_op previous;

    uint64_t             num_docs_sent = 0;
    std::vector<yk_id_t> ids; ids.reserve(in.batch_size);
    std::vector<size_t>  docsizes; ids.reserve(in.batch_size);
    std::vector<char>    docs;

    auto wait_for_previous_rpc = [&]() -> yk_return_t {
        hg_return_t hret = HG_SUCCESS;
        DEFER(margo_destroy(previous.handle); previous.handle = HG_HANDLE_NULL;);
        DEFER(margo_bulk_free(previous.bulk); previous.bulk = HG_BULK_NULL;);
        if(previous.handle == HG_HANDLE_NULL) return YOKAN_SUCCESS;
        hret = margo_wait(previous.req);
        CHECK_HRET(hret, margo_wait);
        doc_iter_back_out_t back_out;
        hret = margo_get_output(previous.handle, &back_out);
        CHECK_HRET(hret, margo_get_output);
        DEFER(margo_free_output(previous.handle, &back_out));
        return (yk_return_t)back_out.ret;
    };

    auto send_batch = [&]() -> yk_return_t {

        if(ids.size() == 0)
            return YOKAN_SUCCESS;

        std::array<void*, 3>     buffer_ptrs  = {(void*)ids.data(), (void*)docsizes.data(), (void*)docs.data()};
        std::array<hg_size_t, 3> buffer_sizes = {ids.size()*sizeof(yk_id_t), docsizes.size()*sizeof(size_t), docs.size()};
        hg_bulk_t local_bulk                  = HG_BULK_NULL;
        hret = margo_bulk_create(mid, 3, buffer_ptrs.data(), buffer_sizes.data(), HG_BULK_READ_ONLY, &local_bulk);
        CHECK_HRET(hret, margo_bulk_create);
        DEFER(margo_bulk_free(local_bulk));

        doc_iter_back_in_t back_in;
        back_in.op_ref = in.op_ref;
        back_in.start  = num_docs_sent;
        back_in.count  = ids.size();
        back_in.size   = std::accumulate(buffer_sizes.begin(), buffer_sizes.end(), (size_t)0);
        back_in.bulk   = local_bulk;

        auto ret       = wait_for_previous_rpc();
        if(ret != YOKAN_SUCCESS)
            return ret;

        // send a doc_iter_back RPC to the client
        hg_handle_t back_handle = HG_HANDLE_NULL;
        hret = margo_create(mid, info->addr, provider->doc_iter_back_id, &back_handle);
        CHECK_HRET(hret, margo_create);
        DEFER(margo_destroy(back_handle));

        margo_request req = MARGO_REQUEST_NULL;

        hret = margo_iforward(back_handle, &back_in, &req);
        CHECK_HRET(hret, margo_iforward);

        num_docs_sent += ids.size();

        previous.docs     = std::move(docs);
        previous.ids      = std::move(ids);
        previous.docsizes = std::move(docsizes);
        margo_ref_incr(back_handle);
        previous.handle   = back_handle;
        margo_bulk_ref_incr(local_bulk);
        previous.bulk     = local_bulk;
        previous.req      = req;

        return YOKAN_SUCCESS;
    };

    auto filter_umem = yokan::UserMem{in.filter.data, in.filter.size };
    auto filter      = yokan::FilterFactory::makeDocFilter(mid, in.mode, filter_umem);

    if(!filter) {
        out.ret = YOKAN_ERR_INVALID_FILTER;
        return;
    }

    auto doc_iter_func = [&](yk_id_t id, const yokan::UserMem& doc) -> yokan::Status {
        //  filtered_docsize is an upper-bound here
        auto filtered_docsize = filter->docSizeFrom(in.coll_name, doc.data, doc.size);
        auto current_size = docs.size();
        docs.resize(current_size + filtered_docsize);
        filtered_docsize = filter->docCopy(
            in.coll_name,
            docs.data() + current_size,
            filtered_docsize, doc.data, doc.size);
        ids.push_back(id);
        docsizes.push_back(filtered_docsize);
        // filtered_docsize may have changed
        docs.resize(current_size + filtered_docsize);
        if(docsizes.size() == in.batch_size)
            return (yokan::Status)send_batch();
        return yokan::Status::OK;
    };

    out.ret = static_cast<yk_return_t>(
        database->docIter(in.coll_name, in.mode, in.count, in.from_id, filter, doc_iter_func));

    auto ret = send_batch();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
    ret = wait_for_previous_rpc();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_iter_ult)

void yk_doc_iter_direct_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_iter_in_t in;
    doc_iter_out_t out;

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

    yk_database* database = provider->db;
    CHECK_DATABASE(database);
    CHECK_MODE_SUPPORTED(database, in.mode);

    struct previous_op {
        hg_handle_t   handle = HG_HANDLE_NULL;
        margo_request req    = MARGO_REQUEST_NULL;
    };

    previous_op previous;

    uint64_t             num_docs_sent = 0;
    std::vector<yk_id_t> ids; ids.reserve(in.batch_size);
    std::vector<size_t>  docsizes; docsizes.reserve(in.batch_size);
    std::vector<char>    docs;

    auto wait_for_previous_rpc = [&]() -> yk_return_t {
        hg_return_t hret = HG_SUCCESS;
        DEFER(margo_destroy(previous.handle); previous.handle = HG_HANDLE_NULL;);
        if(previous.handle == HG_HANDLE_NULL) return YOKAN_SUCCESS;
        hret = margo_wait(previous.req);
        CHECK_HRET(hret, margo_wait);
        doc_iter_direct_back_out_t back_out;
        hret = margo_get_output(previous.handle, &back_out);
        CHECK_HRET(hret, margo_get_output);
        DEFER(margo_free_output(previous.handle, &back_out));
        return (yk_return_t)back_out.ret;
    };

    auto send_batch = [&]() -> yk_return_t {

        if(ids.size() == 0)
            return YOKAN_SUCCESS;

        doc_iter_direct_back_in_t back_in;
        back_in.op_ref          = in.op_ref;
        back_in.start           = num_docs_sent;
        back_in.ids.count       = ids.size();
        back_in.ids.ids         = ids.data();
        back_in.doc_sizes.count = docsizes.size();
        back_in.doc_sizes.sizes = docsizes.data();
        back_in.docs.data       = docs.data();
        back_in.docs.size       = docs.size();

        auto ret = wait_for_previous_rpc();
        if(ret != YOKAN_SUCCESS)
            return ret;

        // send a doc_iter_back RPC to the client
        hg_handle_t back_handle = HG_HANDLE_NULL;
        hret = margo_create(mid, info->addr, provider->doc_iter_direct_back_id, &back_handle);
        CHECK_HRET(hret, margo_create);
        DEFER(margo_destroy(back_handle));

        margo_request req = MARGO_REQUEST_NULL;

        hret = margo_iforward(back_handle, &back_in, &req);
        CHECK_HRET(hret, margo_iforward);

        num_docs_sent += docs.size();

        ids.clear();
        docsizes.clear();
        docs.clear();

        margo_ref_incr(back_handle);
        previous.handle = back_handle;
        previous.req    = req;

        return YOKAN_SUCCESS;
    };

    auto filter_umem = yokan::UserMem{in.filter.data, in.filter.size };
    auto filter      = yokan::FilterFactory::makeDocFilter(mid, in.mode, filter_umem);

    if(!filter) {
        out.ret = YOKAN_ERR_INVALID_FILTER;
        return;
    }

    auto doc_iter_func = [&](yk_id_t id, const yokan::UserMem& doc) -> yokan::Status {
        // filtered_docsize is an upper-bound here
        auto filtered_docsize = filter->docSizeFrom(in.coll_name, doc.data, doc.size);
        auto current_size = docs.size();
        docs.resize(current_size + filtered_docsize);
        filtered_docsize = filter->docCopy(in.coll_name, docs.data() + current_size, filtered_docsize, doc.data, doc.size);
        ids.push_back(id);
        docsizes.push_back(filtered_docsize);
        // filtered_docsize  may have changed
        docs.resize(current_size + filtered_docsize);
        if(docsizes.size() == in.batch_size)
            return (yokan::Status)send_batch();
        return yokan::Status::OK;
    };

    out.ret = static_cast<yk_return_t>(
        database->docIter(in.coll_name, in.mode, in.count, in.from_id, filter, doc_iter_func));

    auto ret = send_batch();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
    ret = wait_for_previous_rpc();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_iter_direct_ult)

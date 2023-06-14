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

void yk_doc_fetch_ult(hg_handle_t h)
{
    hg_return_t hret;
    doc_fetch_in_t in;
    doc_fetch_out_t out;

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
        in.batch_size = in.ids.count;

    size_t num_batches = (size_t)std::ceil((double)in.ids.count/(double)in.batch_size);

    yk_database* database = find_database(provider, &in.db_id);
    CHECK_DATABASE(database, in.db_id);
    CHECK_MODE_SUPPORTED(database, in.mode);

    bool direct = in.mode & YOKAN_MODE_NO_RDMA;

    struct previous_op {
        std::vector<char>   docs;
        std::vector<size_t> doc_sizes;
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
        doc_fetch_back_out_t back_out;
        hret = margo_get_output(previous.handle, &back_out);
        CHECK_HRET(hret, margo_get_output);
        DEFER(margo_free_output(previous.handle, &back_out));
        return (yk_return_t)back_out.ret;
    };

    for(unsigned batch_index = 0; batch_index < num_batches; ++batch_index) {

        // create UserMem wrapper for ids for this batch
        auto ids = yokan::BasicUserMem<yk_id_t>{
            in.ids.ids + in.batch_size*batch_index,
            std::min<size_t>(in.batch_size, in.ids.count - batch_index*in.batch_size)};

        // create buffers to hold the documents and document sizes
        std::vector<char>   docs;
        std::vector<size_t> doc_sizes;
        doc_sizes.reserve(in.batch_size);

        auto fetcher = [&docs, &doc_sizes](yk_id_t id, const yokan::UserMem& doc) -> yokan::Status {
            (void)id;
            doc_sizes.push_back(doc.size);
            if(doc.size != YOKAN_KEY_NOT_FOUND) {
                size_t current_size = docs.size();
                if(docs.capacity() < current_size + doc.size)
                    docs.reserve(docs.capacity()*2);
                docs.resize(current_size + doc.size);
                std::memcpy(docs.data() + current_size, doc.data, doc.size);
            }
            return yokan::Status::OK;
        };

        out.ret = static_cast<yk_return_t>(
                database->docFetch(in.coll_name, in.mode, ids, fetcher));
        if(out.ret != YOKAN_SUCCESS)
            break;

        if(direct) {

            doc_fetch_direct_back_in_t back_in;
            back_in.op_ref = in.op_ref;
            back_in.start  = batch_index*in.batch_size;
            back_in.doc_sizes.count = doc_sizes.size();
            back_in.doc_sizes.sizes = doc_sizes.data();
            back_in.docs.size = docs.size();
            back_in.docs.data = docs.data();

            out.ret = wait_for_previous_rpc();
            if(out.ret != YOKAN_SUCCESS)
                break;

            // send a doc_fetch_direct_back RPC to the client

            hg_handle_t back_handle = HG_HANDLE_NULL;
            hret = margo_create(mid, info->addr, provider->doc_fetch_direct_back_id, &back_handle);
            CHECK_HRET_OUT_GOTO(hret, margo_create, finish);
            DEFER(margo_destroy(back_handle));

            margo_request req = MARGO_REQUEST_NULL;

            hret = margo_iforward(back_handle, &back_in, &req);
            CHECK_HRET_OUT_GOTO(hret, margo_iforward, finish);

            margo_ref_incr(back_handle);
            previous.handle = back_handle;
            previous.req    = req;

        } else { // use RDMA

            std::array<void*, 2>     ptrs   = {(void*)doc_sizes.data(), (void*)docs.data()};
            std::array<hg_size_t, 2> sizes = {doc_sizes.size()*sizeof(size_t), docs.size()};
            hg_bulk_t                bulk  = HG_BULK_NULL;
            hret = margo_bulk_create(
                    mid, sizes[1] ? 2 : 1, ptrs.data(), sizes.data(), HG_BULK_READ_ONLY, &bulk);
            CHECK_HRET_OUT_GOTO(hret, margo_bulk_create, finish);
            DEFER(margo_bulk_free(bulk));

            doc_fetch_back_in_t back_in;
            back_in.op_ref = in.op_ref;
            back_in.start  = batch_index*in.batch_size;
            back_in.count  = ids.size;
            back_in.size   = std::accumulate(sizes.begin(), sizes.end(), (size_t)0);
            back_in.bulk   = bulk;

            out.ret = wait_for_previous_rpc();
            if(out.ret != YOKAN_SUCCESS)
                break;

            // send a doc_fetch_back RPC to the client

            hg_handle_t back_handle = HG_HANDLE_NULL;
            hret = margo_create(mid, info->addr, provider->doc_fetch_back_id, &back_handle);
            CHECK_HRET_OUT_GOTO(hret, margo_create, finish);
            DEFER(margo_destroy(back_handle));

            margo_request req = MARGO_REQUEST_NULL;

            hret = margo_iforward(back_handle, &back_in, &req);
            CHECK_HRET_OUT_GOTO(hret, margo_iforward, finish);

            previous.docs = std::move(docs);
            previous.doc_sizes = std::move(doc_sizes);
            margo_ref_incr(back_handle);
            previous.handle = back_handle;
            margo_bulk_ref_incr(bulk);
            previous.bulk   = bulk;
            previous.req    = req;
        }
    }

finish:
    auto ret = wait_for_previous_rpc();
    if(out.ret == YOKAN_SUCCESS) out.ret = ret;
    return;
}
DEFINE_MARGO_RPC_HANDLER(yk_doc_fetch_ult)

/*
 * (C) The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/bulk-cache.h"
#include "../common/logging.h"
#include <nlohmann/json.hpp>
#include <atomic>
#include <new>
#include <set>

namespace yokan {

using json = nlohmann::json;

struct bulk_less {
    bool operator()(const yk_buffer_t& a, const yk_buffer_t& b) const {
        if(a->size != b->size)
           return  a->size < b->size;
        else return a->data < b->data; // comparing pointer to prevent std::set
        // from considering uniqueness based only on size
    }
};

struct keep_all_bulk_cache {
    margo_instance_id                mid;
    std::atomic<unsigned long>       num_allocated;
    std::set<yk_buffer_t, bulk_less> buffer_set_readonly;
    std::set<yk_buffer_t, bulk_less> buffer_set_writeonly;
    std::set<yk_buffer_t, bulk_less> buffer_set_readwrite;
    ABT_mutex                        buffer_set_mtx;
    float                            margin;
};

void* keep_all_bulk_cache_init(margo_instance_id mid, const char* config) {
    auto cache = new keep_all_bulk_cache;
    cache->mid = mid;
    cache->num_allocated = 0;
    auto cfg = json::parse(config);
    if(cfg.contains("margin") && cfg["margin"].is_number()) {
        cache->margin = cfg["margin"].get<float>();
        if(cache->margin < 0)
            cache->margin = 0.0;
    } else {
        cache->margin = 0.0;
    }
    ABT_mutex_create(&cache->buffer_set_mtx);
    return cache;
}

void keep_all_bulk_cache_finalize(void* c) {
    auto cache = static_cast<keep_all_bulk_cache*>(c);
    auto num_allocated = cache->num_allocated.load();
    if(num_allocated != 0) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(cache->mid,
            "%ld buffers have not been released to the bulk cache",
            num_allocated);
        // LCOV_EXCL_STOP
    }
    ABT_mutex_free(&cache->buffer_set_mtx);
    for(auto buffer : cache->buffer_set_readonly) {
        margo_bulk_free(buffer->bulk);
        delete[] buffer->data;
        delete buffer;
    }
    for(auto buffer : cache->buffer_set_writeonly) {
        margo_bulk_free(buffer->bulk);
        delete[] buffer->data;
        delete buffer;
    }
    for(auto buffer : cache->buffer_set_readwrite) {
        margo_bulk_free(buffer->bulk);
        delete[] buffer->data;
        delete buffer;
    }
    delete cache;
}

yk_buffer_t keep_all_bulk_cache_get(void* c, size_t size, hg_uint8_t mode) {
    auto cache = static_cast<keep_all_bulk_cache*>(c);
    if(size == 0) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(cache->mid,
            "requesting a buffer of size 0");
        return nullptr;
        // LCOV_EXCL_STOP
    }

    // try to find a buffer that's already allocated with the right size
    std::set<yk_buffer_t, bulk_less>* set = nullptr;
    if(mode == HG_BULK_READ_ONLY)
        set = &cache->buffer_set_readonly;
    else if(mode == HG_BULK_WRITE_ONLY)
        set = &cache->buffer_set_writeonly;
    else if(mode == HG_BULK_READWRITE)
        set = &cache->buffer_set_readwrite;

    yk_buffer lbound{ size, mode, nullptr, HG_BULK_NULL };
    ABT_mutex_spinlock(cache->buffer_set_mtx);
    auto it = set->lower_bound(&lbound);
    if(it != set->end()) { // item found
        auto result = *it;
        set->erase(it);
        ABT_mutex_unlock(cache->buffer_set_mtx);
        return result;
    }
    ABT_mutex_unlock(cache->buffer_set_mtx);

    // item not found in cache, allocate a new one

    size_t buf_size = size*(1.0 + cache->margin);

    auto buffer = new yk_buffer{buf_size, mode, nullptr, HG_BULK_NULL};
    cache->num_allocated += 1;
    buffer->data          = new (std::nothrow) char[buf_size];
    if(!buffer->data) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(cache->mid,
            "Allocation of %lu-byte buffer failed in keep_all_bulk_cache",
            buf_size);
        delete buffer;
        return nullptr;
        // LCOV_EXCL_STOP
    }
    void* buf_ptrs[1]      = { buffer->data };
    hg_size_t buf_sizes[1] = { buf_size };

    hg_return_t hret = margo_bulk_create(cache->mid,
            1, buf_ptrs, buf_sizes, mode, &(buffer->bulk));

    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(cache->mid,
            "margo_bulk_create failed with error code %d when creating bulk handle for %lu bytes", hret, size);
        delete buffer->data;
        delete buffer;
        return nullptr;
        // LCOV_EXCL_STOP
    }
    return buffer;
}

void keep_all_bulk_cache_release(void* c, yk_buffer_t buffer) {
    auto cache = static_cast<keep_all_bulk_cache*>(c);
    cache->num_allocated -= 1;
    ABT_mutex_spinlock(cache->buffer_set_mtx);
    if(buffer->mode == HG_BULK_READ_ONLY) {
        cache->buffer_set_readonly.insert(buffer);
    } else if(buffer->mode == HG_BULK_WRITE_ONLY) {
        cache->buffer_set_writeonly.insert(buffer);
    } else if(buffer->mode == HG_BULK_READWRITE) {
        cache->buffer_set_readwrite.insert(buffer);
    }
    ABT_mutex_unlock(cache->buffer_set_mtx);
}

}

extern "C" {

yk_bulk_cache yk_keep_all_bulk_cache = {
    yokan::keep_all_bulk_cache_init,
    yokan::keep_all_bulk_cache_finalize,
    yokan::keep_all_bulk_cache_get,
    yokan::keep_all_bulk_cache_release
};

}

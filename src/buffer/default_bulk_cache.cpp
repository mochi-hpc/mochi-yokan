/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/bulk-cache.h"
#include "../common/logging.h"
#include <atomic>

namespace rkv {

struct default_bulk_cache {
    margo_instance_id          mid;
    std::atomic<unsigned long> num_allocated;
};

void* default_bulk_cache_init(margo_instance_id mid, const char* config) {
    (void)config;
    auto cache = new default_bulk_cache;
    cache->mid = mid;
    cache->num_allocated = 0;
    return cache;
}

void default_bulk_cache_finalize(void* c) {
    auto cache = static_cast<default_bulk_cache*>(c);
    auto num_allocated = cache->num_allocated.load();
    if(num_allocated != 0) {
        // LCOV_EXCL_START
        RKV_LOG_ERROR(cache->mid,
            "%ld buffers have not been released to the bulk cache",
            num_allocated);
        // LCOV_EXCL_STOP
    }
    delete cache;
}

rkv_buffer_t default_bulk_cache_get(void* c, size_t size, hg_uint8_t mode) {
    auto cache = static_cast<default_bulk_cache*>(c);
    if(size == 0) {
        // LCOV_EXCL_START
        RKV_LOG_ERROR(cache->mid,
            "requesting a buffer of size 0");
        return nullptr;
        // LCOV_EXCL_STOP
    }

    auto buffer = new rkv_buffer{size, mode, nullptr, HG_BULK_NULL};
    cache->num_allocated += 1;
    buffer->data           = new char[size];
    void* buf_ptrs[1]      = { buffer->data };
    hg_size_t buf_sizes[1] = { size };

    hg_return_t hret = margo_bulk_create(cache->mid,
            1, buf_ptrs, buf_sizes, mode, &(buffer->bulk));

    if(hret != HG_SUCCESS) {
        // LCOV_EXCL_START
        RKV_LOG_ERROR(cache->mid,
            "margo_bulk_create failed with error code %d", hret);
        delete buffer;
        return nullptr;
        // LCOV_EXCL_STOP
    }
    return buffer;
}

void default_bulk_cache_release(void* c, rkv_buffer_t buffer) {
    auto cache = static_cast<default_bulk_cache*>(c);
    margo_bulk_free(buffer->bulk);
    delete[] buffer->data;
    cache->num_allocated -= 1;
}

}

extern "C" {

rkv_bulk_cache rkv_default_bulk_cache = {
    rkv::default_bulk_cache_init,
    rkv::default_bulk_cache_finalize,
    rkv::default_bulk_cache_get,
    rkv::default_bulk_cache_release
};

}

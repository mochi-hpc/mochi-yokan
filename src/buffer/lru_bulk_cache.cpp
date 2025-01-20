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
#include <map>
#include <list>

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

using list_t     = std::list<yk_buffer_t>;
using iterator_t = list_t::iterator;
using map_t      = std::map<yk_buffer_t, iterator_t, bulk_less>;

struct lru_bulk_cache {
    margo_instance_id          mid;
    std::atomic<unsigned long> num_in_use;

    list_t buffer_queue_readonly;
    map_t  buffer_set_readonly;

    list_t buffer_queue_writeonly;
    map_t  buffer_set_writeonly;

    list_t buffer_queue_readwrite;
    map_t  buffer_set_readwrite;

    ABT_mutex buffer_set_mtx;
    float     margin;
    size_t    capacity;
};

void* lru_bulk_cache_init(margo_instance_id mid, const char* config) {
    auto cache = new lru_bulk_cache;
    cache->mid = mid;
    cache->num_in_use = 0;
    auto cfg = json::parse(config);
    if(cfg.contains("margin") && cfg["margin"].is_number()) {
        cache->margin = cfg["margin"].get<float>();
        if(cache->margin < 0)
            cache->margin = 0.0;
    } else {
        cache->margin = 0.0;
    }
    if(cfg.contains("capacity") && cfg["capacity"].is_number_unsigned()) {
        cache->capacity = cfg["capacity"].get<size_t>();
    } else {
        cache->capacity = 32;
    }
    ABT_mutex_create(&cache->buffer_set_mtx);
    return cache;
}

void lru_bulk_cache_finalize(void* c) {
    auto cache = static_cast<lru_bulk_cache*>(c);
    auto num_in_use = cache->num_in_use.load();
    if(num_in_use != 0) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(cache->mid,
            "%lu buffers have not been released to the bulk cache",
            num_in_use);
        // LCOV_EXCL_STOP
    }
    ABT_mutex_free(&cache->buffer_set_mtx);
    for(auto& map : { std::ref(cache->buffer_set_readonly),
                      std::ref(cache->buffer_set_writeonly),
                      std::ref(cache->buffer_set_readwrite) }) {
        for(auto it = map.get().cbegin(); it != map.get().cend(); ) {
            auto buffer = it->first;
            it = map.get().erase(it);
            margo_bulk_free(buffer->bulk);
            delete[] buffer->data;
            delete buffer;
        }
    }
    delete cache;
}

yk_buffer_t lru_bulk_cache_get(void* c, size_t size, hg_uint8_t mode) {
    auto cache = static_cast<lru_bulk_cache*>(c);
    if(size == 0) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(cache->mid,
            "requesting a buffer of size 0");
        return nullptr;
        // LCOV_EXCL_STOP
    }

    // try to find a buffer that's already allocated with the right size
    map_t* map   = nullptr;
    list_t* list = nullptr;
    if(mode == HG_BULK_READ_ONLY) {
        map = &cache->buffer_set_readonly;
        list = &cache->buffer_queue_readonly;
    } else if(mode == HG_BULK_WRITE_ONLY) {
        map = &cache->buffer_set_writeonly;
        list = &cache->buffer_queue_writeonly;
    } else if(mode == HG_BULK_READWRITE) {
        map = &cache->buffer_set_readwrite;
        list = &cache->buffer_queue_readwrite;
    }

    yk_buffer lbound{ size, mode, nullptr, HG_BULK_NULL };
    ABT_mutex_spinlock(cache->buffer_set_mtx);
    auto it = map->lower_bound(&lbound);
    if(it != map->end()) { // item found
        auto result = it->first;
        list->erase(it->second);
        map->erase(it);
        cache->num_in_use += 1;
        ABT_mutex_unlock(cache->buffer_set_mtx);
        return result;
    }
    ABT_mutex_unlock(cache->buffer_set_mtx);

    // item not found in cache, allocate a new one

    size_t buf_size = size*(1.0 + cache->margin);

    auto buffer = new yk_buffer{buf_size, mode, nullptr, HG_BULK_NULL};
    buffer->data          = new (std::nothrow) char[buf_size];
    if(!buffer->data) {
        // LCOV_EXCL_START
        YOKAN_LOG_ERROR(cache->mid,
            "Allocation of %lu-byte buffer failed in lru_bulk_cache",
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
    cache->num_in_use += 1;
    return buffer;
}

void lru_bulk_cache_release(void* c, yk_buffer_t buffer) {
    auto cache = static_cast<lru_bulk_cache*>(c);
    cache->num_in_use -= 1;

    static auto release = [](lru_bulk_cache* cache, map_t& map, list_t& list, yk_buffer_t buffer) {
        list.push_front(buffer);
        auto it = list.begin();
        map.emplace(buffer, it);
        if(list.size() > cache->capacity) {
            auto oldest_buffer = list.back();
            list.pop_back();
            auto it = map.find(oldest_buffer);
            map.erase(it);
            margo_bulk_free(oldest_buffer->bulk);
            delete[] oldest_buffer->data;
            delete oldest_buffer;
        }
    };

    ABT_mutex_spinlock(cache->buffer_set_mtx);
    if(buffer->mode == HG_BULK_READ_ONLY) {
        release(cache, cache->buffer_set_readonly, cache->buffer_queue_readonly, buffer);
    } else if(buffer->mode == HG_BULK_WRITE_ONLY) {
        release(cache, cache->buffer_set_writeonly, cache->buffer_queue_writeonly, buffer);
    } else if(buffer->mode == HG_BULK_READWRITE) {
        release(cache, cache->buffer_set_readwrite, cache->buffer_queue_readwrite, buffer);
    }
    ABT_mutex_unlock(cache->buffer_set_mtx);
}

}

extern "C" {

yk_bulk_cache yk_lru_bulk_cache = {
    yokan::lru_bulk_cache_init,
    yokan::lru_bulk_cache_finalize,
    yokan::lru_bulk_cache_get,
    yokan::lru_bulk_cache_release
};

}

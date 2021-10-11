/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_ALLOCATOR_HPP
#define __RKV_ALLOCATOR_HPP

#include "logging.h"
#include "yokan/allocator.h"
#include <memory>
#include <iostream>

namespace rkv {

inline void default_allocator_init(rkv_allocator_t* allocator, const char* config) {
    (void)config;
    allocator->context = new std::allocator<char>();
    allocator->allocate = [](void* ctx, size_t item_size, size_t count) {
        auto a = static_cast<std::allocator<char>*>(ctx);
        return static_cast<void*>(a->allocate(item_size*count));
    };
    allocator->deallocate = [](void* ctx, void* p, size_t item_size, size_t count) {
        auto a = static_cast<std::allocator<char>*>(ctx);
        a->deallocate(static_cast<char*>(p), item_size*count);
    };
    allocator->finalize = [](void* ctx) {
        auto a = static_cast<std::allocator<char>*>(ctx);
        delete a;
    };
}

template <typename T>
struct Allocator {

    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    inline Allocator(rkv_allocator_t& a)
    : m_internal(a) {}

    template<typename U>
    Allocator(const Allocator<U>& other)
    : m_internal(other.m_internal) {}

    template<typename U>
    struct rebind {
        typedef Allocator<U> other;
    };

    inline value_type* allocate(size_type n, const void* hint = 0) {
        (void)hint;
        return static_cast<value_type*>(
                m_internal.allocate(m_internal.context, sizeof(value_type), n));
    }

    inline void deallocate(value_type* p, size_type n) {
        m_internal.deallocate(
            m_internal.context, p, sizeof(value_type), n);
    }

    rkv_allocator_t& m_internal;
};

}

#endif

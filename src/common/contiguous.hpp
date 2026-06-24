/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_COMMON_CONTIGUOUS_HPP
#define __YOKAN_COMMON_CONTIGUOUS_HPP

#include <cstddef>

namespace yokan {

/* Returns a pointer to the start of the data if `count` segments described by
 * (ptrs[i], sizes[i]) are already laid out back-to-back in memory. Zero-sized
 * segments are skipped (their pointer is irrelevant). Returns nullptr if the
 * segments are not contiguous, or if every segment has size 0. */
static inline const void* contiguous_base(const void* const* ptrs,
                                          const size_t* sizes,
                                          size_t count)
{
    const char* base     = nullptr;
    const char* expected = nullptr;
    for(size_t i = 0; i < count; i++) {
        if(sizes[i] == 0) continue;
        const char* p = static_cast<const char*>(ptrs[i]);
        if(base == nullptr) {
            base = p;
        } else if(p != expected) {
            return nullptr;
        }
        expected = p + sizes[i];
    }
    return base;
}

}

#endif

/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_MODES_HPP
#define __YOKAN_MODES_HPP

#include "yokan/common.h"
#include "config.h"
#include <cstring>
#ifdef YOKAN_HAS_LUA
#include <sol/sol.hpp>
#endif

namespace yokan {

/**
 * This function will copy a key into a buffer according to the mode
 * provided, i.e. eliminating prefix or suffix or ignoring the copy
 * as needed.
 */
static inline size_t keyCopy(int32_t mode,
        void* dst, size_t max_dst_size,
        const void* key, size_t ksize) {
    if(mode & YOKAN_MODE_IGNORE_KEYS) return 0;
    if(max_dst_size < ksize) return YOKAN_SIZE_TOO_SMALL;
    std::memcpy(dst, key, ksize);
    return ksize;
}

/**
 * @brief This function is provided for symmetry with keyCopy,
 * and in case we every need value copies to depend on a mode.
 */
static inline size_t valCopy(int32_t mode,
        void* dst, size_t max_dst_size,
        const void* val, size_t vsize) {
    (void)mode;
    if(max_dst_size < vsize) return YOKAN_SIZE_TOO_SMALL;
    std::memcpy(dst, val, vsize);
    return vsize;
}

}
#endif

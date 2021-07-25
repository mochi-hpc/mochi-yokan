/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_MODES_HPP
#define __RKV_MODES_HPP

#include "rkv/rkv-common.h"
#include <cstring>

namespace rkv {

/**
 * This function checks the mode. If the mode does not match
 * RKV_MODE_SUFFIX, it considers "prefix" as a prefix and
 * checks if the key starts with that prefix. Otherwise it
 * considers "prefix" as a suffix and checks if the key ends
 * with that suffix.
 */
static inline bool checkPrefix(int32_t mode,
                               const void* key, size_t ksize,
                               const void* prefix, size_t psize) {
    if(psize > ksize)
        return false;
    if(!(mode & RKV_MODE_SUFFIX)) {
        return std::memcmp(key, prefix, psize) == 0;
    } else {
        return std::memcmp(((const char*)key)+ksize-psize, prefix, psize) == 0;
    }
}

/**
 * This function will copy a key into a buffer according to the mode
 * provided, i.e. eliminating prefix or suffix or ignoring the copy
 * as needed.
 */
static inline size_t keyCopy(int32_t mode,
        void* dst, size_t max_dst_size,
        const void* key, size_t ksize,
        size_t prefix_size,
        bool is_last = false) {
    if(mode & RKV_MODE_IGNORE_KEYS) {
        if(!(is_last && (mode & RKV_MODE_KEEP_LAST)))
            return 0;
    }
    if(!(mode & RKV_MODE_NO_PREFIX)) { // don't eliminate prefix/suffix
        if(max_dst_size < ksize) return RKV_SIZE_TOO_SMALL;
        std::memcpy(dst, key, ksize);
        return ksize;
    } else { // eliminate prefix/suffix
        auto final_ksize = ksize - prefix_size;
        if(max_dst_size < final_ksize)
            return RKV_SIZE_TOO_SMALL;
        if(mode & RKV_MODE_SUFFIX) { // eliminate suffix
            std::memcpy(dst, (const char*)key, final_ksize);
        } else { // eliminate prefix
            std::memcpy(dst, (const char*)key + prefix_size, final_ksize);
        }
        return final_ksize;
    }
}

/**
 * @brief This function is provided for symmetry with keyCopy,
 * and in case we every need value copies to depend on a mode.
 */
static inline size_t valCopy(int32_t mode,
        void* dst, size_t max_dst_size,
        const void* val, size_t vsize) {
    (void)mode;
    if(max_dst_size < vsize) return RKV_SIZE_TOO_SMALL;
    std::memcpy(dst, val, vsize);
    return vsize;
}

}
#endif

/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_MODES_HPP
#define __RKV_MODES_HPP

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

}
#endif

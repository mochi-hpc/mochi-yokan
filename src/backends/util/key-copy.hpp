/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_BACKEND_UTIL_KEY_COPY_HPP
#define __YOKAN_BACKEND_UTIL_KEY_COPY_HPP

namespace yokan {

/**
 * This function is a helper to wrap filter->keyCopy(...) and
 * add support for the YOKAN_MODE_IGNORE_KEYS and YOKAN_MODE_KEEP_LAST
 * modes.
 */
template<typename Filter, typename ... Args>
static inline size_t keyCopy(int32_t mode, bool is_last, const Filter& filter, const Args&... args) {
    if(mode & YOKAN_MODE_IGNORE_KEYS) {
        if(!(is_last && (mode & YOKAN_MODE_KEEP_LAST)))
            return 0;
    }
    return filter->keyCopy(args...);
}

}

#endif

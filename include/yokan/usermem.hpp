/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_USERMEM_H
#define __YOKAN_USERMEM_H

#include <vector>

namespace yokan {

/**
 * @brief Wrapper for user memory (equivalent to
 * some backend's notion of Slice, or to a C++17 std::string_view)
 *
 * @tparam T Type of data held.
 */
template<typename T>
struct BasicUserMem {
    T*     data = nullptr; /*!< Pointer to the data */
    size_t size = 0;       /*!< Number of elements of type T in the buffer */

    template<typename I>
    inline T& operator[](I index) {
        return data[index];
    }

    template<typename I>
    inline const T& operator[](I index) const {
        return data[index];
    }

    BasicUserMem(std::vector<T>& v)
    : data(v.data())
    , size(v.size()) {}

    BasicUserMem(T* d, size_t s)
    : data(d)
    , size(s) {}

    auto from(size_t offset) const {
        if(offset > size)
            throw std::out_of_range("invalid offset passed to BasicUserMem::from()");
        return BasicUserMem<T>{ data + offset, size - offset };
    }
};

/**
 * @brief UserMem is short for BasicUserMem<void>.
 * Its size field represents a number of bytes for
 * a buffer of unspecified type.
 */
using UserMem = BasicUserMem<char>;

}

#endif

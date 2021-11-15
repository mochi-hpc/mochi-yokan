/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_FILTERS_H
#define __YOKAN_FILTERS_H

#include <limits>
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <stdexcept>
#include <memory>
#include <yokan/common.h>
#include <yokan/usermem.hpp>

namespace yokan {

/**
 * @brief Abstract class to represent a filter.
 */
class KeyValueFilter {

    public:

    virtual ~KeyValueFilter() = default;

    virtual bool requiresValue() const = 0;
    virtual bool requiresFullKey() const = 0;
    virtual size_t minRequiredKeySize() const = 0;

    virtual bool check(const void* key, size_t ksize, const void* val, size_t vsize) const = 0;

    virtual size_t keyCopy(
        void* dst, size_t max_dst_size,
        const void* key, size_t ksize,
        bool is_last) const = 0;

    virtual size_t valCopy(
        void* dst, size_t max_dst_size,
        const void* val, size_t vsize) const = 0;

    static std::shared_ptr<KeyValueFilter> makeFilter(
            margo_instance_id mid, int32_t mode, const UserMem& filter_data);
};

class DocFilter {

    public:

    virtual ~DocFilter() = default;

    virtual bool check(yk_id_t id, const void* doc, size_t docsize) const {
        (void)id;
        (void)doc;
        (void)docsize;
        return true;
    }

    static std::shared_ptr<DocFilter> makeFilter(
            margo_instance_id mid, int32_t mode, const UserMem& filter_data);

    static std::shared_ptr<KeyValueFilter> toKeyValueFilter(
            std::shared_ptr<DocFilter> filter,
            const char* collection);
};

}

#endif

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

template <typename T> class __YOKANKeyValueFilterRegistration;
template <typename T> class __YOKANDocFilterRegistration;

namespace yokan {

/**
 * @brief Abstract class to represent a key/value filter.
 */
class KeyValueFilter {

    public:

    virtual ~KeyValueFilter() = default;

    /**
     * @brief Returns whether the filter requires the value to be loaded
     * when calling check. If not, some backends may chose to call check
     * with a nullptr value and vsize of 0 to avoid loading a value
     * unnecessarily.
     */
    virtual bool requiresValue() const = 0;

    /**
     * @brief Checks whether a key (or key/value pair) passes the filter.
     */
    virtual bool check(const void* key, size_t ksize,
                       const void* val, size_t vsize) const = 0;


    /**
     * @brief Compute the new key size from the provided key
     * after the filter is applied, or an upper bound of the key size.
     * This function will only be applied to keys that pass the check
     * function (i.e. you can assume that check has already been called
     * and returned true).
     */
    virtual size_t keySizeFrom(
        const void* key, size_t ksize) const = 0;

    /**
     * @brief Compute the new value size from the provided value
     * after the filter is applied, or an upper bound of the value size.
     * This function will only be applied to keys that pass the check function
     * (i.e. you can assume that check has already been called and returned true).
     */
    virtual size_t valSizeFrom(
        const void* val, size_t vsize) const = 0;

    /**
     * @brief Copy the key to the target destination. This copy may
     * be implemented differently depending on the mode, and may alter
     * the content of the key.
     * This function should return the size actually copied.
     */
    virtual size_t keyCopy(
        void* dst, size_t max_dst_size,
        const void* key, size_t ksize) const = 0;

    /**
     * @brief Copy the value to the target destination. This copy may
     * be implemented differently depending on the mode, and may alter
     * the content of the value.
     * This function should return the size actually copied.
     */
    virtual size_t valCopy(
        void* dst, size_t max_dst_size,
        const void* val, size_t vsize) const = 0;

    /**
     * @brief Some filters may be able to tell Yokan that no more
     * keys will be accepted after a certain key (e.g. the prefix
     * filter). In such a case, the filter can implement the
     * shouldStop function to optimize iterations. Note that this
     * function will be called only if check returns false.
     *
     * @param key Key
     * @param ksize Key size
     * @param val Value
     * @param vsize Value size
     *
     * @return whether iteration can stop after this key.
     */
    virtual bool shouldStop(
        const void* key, size_t ksize,
        const void* val, size_t vsize) const {
        (void)key;
        (void)ksize;
        (void)val;
        (void)vsize;
        return false;
    }
};

/**
 * @brief Abstract class representing a document filter.
 */
class DocFilter {

    public:

    virtual ~DocFilter() = default;

    /**
     * @brief Checks whether the document passes the filter.
     */
    virtual bool check(
        const char* collection,
        yk_id_t id, const void* doc, size_t docsize) const  = 0;

    /**
     * @brief Compute the new document size from the provided document
     * after the filter is applied, or an upper bound of the document size.
     * This function will only be applied to documents that pass the check function
     * (i.e. you can assume that check has already been called and returned true).
     */
    virtual size_t docSizeFrom(
        const char* collection,
        const void* val, size_t vsize) const = 0;

    /**
     * @brief Copy the document to the target destination. This copy may
     * be implemented differently depending on the mode, and may alter
     * the content of the document.
     * This function should return the size actually copied.
     */
    virtual size_t docCopy(
        const char* collection,
        void* dst, size_t max_dst_size,
        const void* val, size_t vsize) const = 0;
};

/**
 * @brief This class is used to register new filters and for the provider
 * to create filters depending on the mode passed.
 */
class FilterFactory {

    template<typename T>
        friend class ::__YOKANKeyValueFilterRegistration;
    template<typename T>
        friend class ::__YOKANDocFilterRegistration;

    public:

    FilterFactory() = delete;

    static std::shared_ptr<KeyValueFilter> makeKeyValueFilter(
        margo_instance_id mid, int32_t mode, const UserMem& filter_data);

    static std::shared_ptr<DocFilter> makeDocFilter(
        margo_instance_id mid, int32_t mode, const UserMem& filter_data);

    static std::shared_ptr<KeyValueFilter> docToKeyValueFilter(
            std::shared_ptr<DocFilter> filter,
            const char* collection);

    private:

    static std::unordered_map<
        std::string,
        std::function<
            std::shared_ptr<KeyValueFilter>(margo_instance_id, int32_t, const UserMem&)
        >
    > s_make_kv_filter;

    static std::unordered_map<
        std::string,
        std::function<
            std::shared_ptr<DocFilter>(margo_instance_id, int32_t, const UserMem&)
        >
    > s_make_doc_filter;

};

}

/**
 * @brief These macro should be used to register new filter types.
 */
#define YOKAN_REGISTER_KV_FILTER(__filter_name, __filter_type) \
    static __YOKANKeyValueFilterRegistration<__filter_type>   \
    __yk_##__filter_name##_kv_filter(#__filter_name)

#define YOKAN_REGISTER_DOC_FILTER(__filter_name, __filter_type) \
    static __YOKANDocFilterRegistration<__filter_type>         \
    __yk_##__filter_name##_doc_filter(#__filter_name)

template <typename T>
class __YOKANKeyValueFilterRegistration {

    public:

    __YOKANKeyValueFilterRegistration(const std::string &filter_name) {
        ::yokan::FilterFactory::s_make_kv_filter[filter_name] =
            [](margo_instance_id mid, int32_t mode, const ::yokan::UserMem& filter_data) {
                return std::make_shared<T>(mid, mode, filter_data);
            };
    }
};

template <typename T>
class __YOKANDocFilterRegistration {

    public:

    __YOKANDocFilterRegistration(const std::string &filter_name) {
        ::yokan::FilterFactory::s_make_doc_filter[filter_name] =
            [](margo_instance_id mid, int32_t mode, const ::yokan::UserMem& filter_data) {
                return std::make_shared<T>(mid, mode, filter_data);
            };
    }
};

#endif

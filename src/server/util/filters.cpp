/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_FILTERS_HPP
#define __YOKAN_FILTERS_HPP

#include "yokan/common.h"
#include "yokan/backend.hpp"
#include "config.h"
#include <cstring>
#include <iostream>
#ifdef YOKAN_HAS_LUA
#include <sol/sol.hpp>
#endif

namespace yokan {

struct KeyPrefixFilter : public KeyValueFilter {

    int32_t m_mode;
    UserMem m_prefix;

    KeyPrefixFilter(int32_t mode, UserMem prefix)
    : m_mode(mode), m_prefix(std::move(prefix)) {}

    bool requiresValue() const override {
        return false;
    }

    size_t minRequiredKeySize() const override {
        return m_prefix.size;
    }

    bool requiresFullKey() const override {
        return false;
    }

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        (void)val;
        (void)vsize;
        if(m_prefix.size > ksize)
            return false;
        return std::memcmp(key, m_prefix.data, m_prefix.size) == 0;
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize,
                   bool is_last) const override {
        if(m_mode & YOKAN_MODE_IGNORE_KEYS) {
            if(!(is_last && (m_mode & YOKAN_MODE_KEEP_LAST)))
                return 0;
        }
        if(!(m_mode & YOKAN_MODE_NO_PREFIX)) { // don't eliminate prefix/suffix
            if(max_dst_size < ksize) return YOKAN_SIZE_TOO_SMALL;
            std::memcpy(dst, key, ksize);
            return ksize;
        } else { // eliminate prefix
            auto final_ksize = ksize - m_prefix.size;
            if(max_dst_size < final_ksize)
                return YOKAN_SIZE_TOO_SMALL;
            std::memcpy(dst, (const char*)key + m_prefix.size, final_ksize);
            return final_ksize;
        }
    }

    size_t valCopy(void* dst, size_t max_dst_size,
                   const void* val, size_t vsize) const override {
        if(max_dst_size < vsize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, val, vsize);
        return vsize;
    }
};

struct FalseKeyValueFilter : public KeyValueFilter {


    FalseKeyValueFilter() = default;

    bool requiresValue() const override {
        return false;
    }

    size_t minRequiredKeySize() const override {
        return 0;
    }

    bool requiresFullKey() const override {
        return false;
    }

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        (void)key;
        (void)ksize;
        (void)val;
        (void)vsize;
        return false;
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize,
                   bool is_last) const override {
        (void)dst;
        (void)max_dst_size;
        (void)key;
        (void)ksize;
        (void)is_last;
        return 0;
    }

    size_t valCopy(void* dst, size_t max_dst_size,
                   const void* val, size_t vsize) const override {
        (void)dst;
        (void)max_dst_size;
        (void)val;
        (void)vsize;
        return 0;
    }
};

struct KeySuffixFilter : public KeyValueFilter {

    int32_t m_mode;
    UserMem m_suffix;

    KeySuffixFilter(int32_t mode, UserMem suffix)
    : m_mode(mode), m_suffix(std::move(suffix)) {}

    bool requiresValue() const override {
        return false;
    }

    size_t minRequiredKeySize() const override {
        return 0;
    }

    bool requiresFullKey() const override {
        return true;
    }

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        (void)val;
        (void)vsize;
        if(m_suffix.size > ksize)
            return false;
        return std::memcmp(((const char*)key)+ksize-m_suffix.size, m_suffix.data, m_suffix.size) == 0;
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize,
                   bool is_last) const override {
        if(m_mode & YOKAN_MODE_IGNORE_KEYS) {
            if(!(is_last && (m_mode & YOKAN_MODE_KEEP_LAST)))
                return 0;
        }
        if(!(m_mode & YOKAN_MODE_NO_PREFIX)) { // don't eliminate suffix
            if(max_dst_size < ksize) return YOKAN_SIZE_TOO_SMALL;
            std::memcpy(dst, key, ksize);
            return ksize;
        } else { // eliminate suffix
            auto final_ksize = ksize - m_suffix.size;
            if(max_dst_size < final_ksize)
                return YOKAN_SIZE_TOO_SMALL;
            std::memcpy(dst, (const char*)key, final_ksize);
            return final_ksize;
        }
    }

    size_t valCopy(void* dst, size_t max_dst_size,
                   const void* val, size_t vsize) const override {
        if(max_dst_size < vsize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, val, vsize);
        return vsize;
    }
};

#ifdef YOKAN_HAS_LUA
struct LuaKeyValueFilter : public KeyValueFilter {

    int32_t m_mode;
    UserMem m_code;
    mutable sol::state m_lua;

    LuaKeyValueFilter(int32_t mode, UserMem code)
    : m_mode(mode), m_code(std::move(code)) {
        m_lua.open_libraries(sol::lib::base);
        m_lua.open_libraries(sol::lib::string);
        m_lua.open_libraries(sol::lib::math);
    }

    bool requiresValue() const override {
        return m_mode & YOKAN_MODE_FILTER_VALUE;
    }

    size_t minRequiredKeySize() const override {
        return 0;
    }

    bool requiresFullKey() const override {
        return true;
    }

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        m_lua["__key__"] = std::string_view(static_cast<const char*>(key), ksize);
        if(m_mode & YOKAN_MODE_FILTER_VALUE)
            m_lua["__value__"] = std::string_view(static_cast<const char*>(val), vsize);
        auto result = m_lua.do_string(std::string_view{ m_code.data, m_code.size });
        if(!result.valid()) return false;
        return static_cast<bool>(result);
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize,
                   bool is_last) const override {
        if(m_mode & YOKAN_MODE_IGNORE_KEYS) {
            if(!(is_last && (m_mode & YOKAN_MODE_KEEP_LAST)))
                return 0;
        }
        if(max_dst_size < ksize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, key, ksize);
        return ksize;
    }

    size_t valCopy(void* dst, size_t max_dst_size,
                   const void* val, size_t vsize) const override {
        if(max_dst_size < vsize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, val, vsize);
        return vsize;
    }
};
#endif

struct FalseDocFilter : public DocFilter {

    FalseDocFilter() = default;

    bool check(yk_id_t id, const void* val, size_t vsize) const override {
        (void)id;
        (void)val;
        (void)vsize;
        return false;
    }
};
#ifdef YOKAN_HAS_LUA
struct LuaDocFilter : public DocFilter {

    int32_t m_mode;
    UserMem m_code;
    mutable sol::state m_lua;

    LuaDocFilter(int32_t mode, UserMem code)
    : m_mode(mode), m_code(std::move(code)) {
        m_lua.open_libraries(sol::lib::base);
        m_lua.open_libraries(sol::lib::string);
        m_lua.open_libraries(sol::lib::math);
    }

    bool check(yk_id_t id, const void* val, size_t vsize) const override {
        m_lua["__id__"] = id;
        m_lua["__doc__"] = std::string_view(static_cast<const char*>(val), vsize);
        auto result = m_lua.do_string(std::string_view{ m_code.data, m_code.size });
        if(!result.valid()) return false;
        return static_cast<bool>(result);
    }
};
#endif

struct CollectionFilterWrapper : public KeyPrefixFilter  {

    std::shared_ptr<DocFilter> m_doc_filter;
    size_t                     m_key_offset;

    CollectionFilterWrapper(const char* coll_name,
            std::shared_ptr<DocFilter> doc_filter)
    : KeyPrefixFilter(YOKAN_MODE_NO_PREFIX, UserMem{ const_cast<char*>(coll_name), strlen(coll_name)+1})
    , m_doc_filter(std::move(doc_filter)) {
        m_key_offset = m_prefix.size;
    }

    bool requiresValue() const override {
        return static_cast<bool>(m_doc_filter);
    }

    size_t minRequiredKeySize() const override {
        return KeyPrefixFilter::minRequiredKeySize() + sizeof(yk_id_t) + 1;
    }

    bool requiresFullKey() const override {
        return true;
    }

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        auto b = KeyPrefixFilter::check(key, ksize, nullptr, 0);
        if(!b) {
            return false;
        }
        if(ksize != (m_key_offset+sizeof(yk_id_t))) {
            return false;
        }
        if(!m_doc_filter) return true;
        yk_id_t id;
        std::memcpy(&id, (const char*)key + m_key_offset, sizeof(id));
        id = _ensureBigEndian(id);
        return m_doc_filter->check(id, val, vsize);
    }

    static yk_id_t _ensureBigEndian(yk_id_t id) {
        #define IS_BIG_ENDIAN (*(uint16_t *)"\0\xff" < 0x100)
        if(IS_BIG_ENDIAN) {
            return id;
        } else {
            uint64_t x = id;
            x = (x & 0x00000000FFFFFFFF) << 32 | (x & 0xFFFFFFFF00000000) >> 32;
            x = (x & 0x0000FFFF0000FFFF) << 16 | (x & 0xFFFF0000FFFF0000) >> 16;
            x = (x & 0x00FF00FF00FF00FF) << 8  | (x & 0xFF00FF00FF00FF00) >> 8;
            return x;
        }
    }
};

std::shared_ptr<KeyValueFilter> KeyValueFilter::makeFilter(
        margo_instance_id mid, int32_t mode, const UserMem& filter_data) {
    if(mode & YOKAN_MODE_LUA_FILTER) {
#ifdef YOKAN_HAS_LUA
        (void)mid;
        return std::make_shared<LuaKeyValueFilter>(mode, filter_data);
#else
        YOKAN_LOG_ERROR(mid, "Yokan wasn't compiled with Lua support!");
        return std::make_shared<FalseKeyValueFilter>();
#endif
    } else if(mode & YOKAN_MODE_SUFFIX) {
        return std::make_shared<KeySuffixFilter>(mode, filter_data);
    } else {
        return std::make_shared<KeyPrefixFilter>(mode, filter_data);
    }
}


std::shared_ptr<DocFilter> DocFilter::makeFilter(
        margo_instance_id mid, int32_t mode, const UserMem& filter_data) {
    if(mode & YOKAN_MODE_LUA_FILTER) {
#ifdef YOKAN_HAS_LUA
        (void)mid;
        return std::make_shared<LuaDocFilter>(mode, filter_data);
#else
        YOKAN_LOG_ERROR(mid, "Yokan wasn't compiled with Lua support!");
        return std::make_shared<FalseDocFilter>();
#endif
    }
    return std::make_shared<DocFilter>();
}

std::shared_ptr<KeyValueFilter> DocFilter::toKeyValueFilter(
            std::shared_ptr<DocFilter> filter,
            const char* collection) {
    return std::make_shared<CollectionFilterWrapper>(collection, std::move(filter));
}

}
#endif

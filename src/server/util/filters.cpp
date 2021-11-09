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
#ifdef YOKAN_HAS_LUA
#include <sol/sol.hpp>
#endif

namespace yokan {

struct KeyPrefixFilter : public KeyValueFilter {

    int32_t m_mode;
    UserMem m_prefix;

    KeyPrefixFilter(int32_t mode, UserMem prefix)
    : m_mode(mode), m_prefix(std::move(prefix)) {}

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

    size_t size() const override {
        return m_prefix.size;
    }
};

struct KeySuffixFilter : public KeyValueFilter {

    int32_t m_mode;
    UserMem m_suffix;

    KeySuffixFilter(int32_t mode, UserMem suffix)
    : m_mode(mode), m_suffix(std::move(suffix)) {}

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

    size_t size() const override {
        return m_suffix.size;
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

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        m_lua["__key__"] = std::string_view(static_cast<const char*>(key), ksize);
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

    size_t size() const override {
        return m_code.size;
    }
};
#endif

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

std::shared_ptr<KeyValueFilter> KeyValueFilter::makeFilter(int32_t mode, const UserMem& filter_data) {
    if(mode & YOKAN_MODE_LUA_FILTER) {
#ifdef YOKAN_HAS_LUA
        return std::make_shared<LuaKeyValueFilter>(mode, filter_data);
#endif
    }
    if(mode & YOKAN_MODE_SUFFIX) {
        return std::make_shared<KeySuffixFilter>(mode, filter_data);
    } else {
        return std::make_shared<KeyPrefixFilter>(mode, filter_data);
    }
}


std::shared_ptr<DocFilter> DocFilter::makeFilter(int32_t mode, const UserMem& filter_data) {
    if(mode & YOKAN_MODE_LUA_FILTER) {
#ifdef YOKAN_HAS_LUA
        return std::make_shared<LuaDocFilter>(mode, filter_data);
#endif
    }
    return std::make_shared<DocFilter>();
}

}
#endif

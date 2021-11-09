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

    UserMem m_prefix;

    KeyPrefixFilter(UserMem prefix)
    : m_prefix(std::move(prefix)) {}

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        (void)val;
        (void)vsize;
        if(m_prefix.size > ksize)
            return false;
        return std::memcmp(key, m_prefix.data, m_prefix.size) == 0;
    }
};

struct KeySuffixFilter : public KeyValueFilter {

    UserMem m_suffix;

    KeySuffixFilter(UserMem suffix)
    : m_suffix(std::move(suffix)) {}

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        (void)val;
        (void)vsize;
        if(m_suffix.size > ksize)
            return false;
        return std::memcmp(((const char*)key)+ksize-m_suffix.size, m_suffix.data, m_suffix.size) == 0;
    }
};

#ifdef YOKAN_HAS_LUA
struct LuaKeyValueFilter : public KeyValueFilter {

    UserMem m_code;
    mutable sol::state m_lua;

    LuaKeyValueFilter(UserMem code)
    : m_code(std::move(code)) {
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
};
#endif

#ifdef YOKAN_HAS_LUA
struct LuaDocFilter : public DocFilter {

    UserMem m_code;
    mutable sol::state m_lua;

    LuaDocFilter(UserMem code)
    : m_code(std::move(code)) {
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
        return std::make_shared<LuaKeyValueFilter>(filter_data);
#endif
    }
    if(mode & YOKAN_MODE_SUFFIX) {
        return std::make_shared<KeySuffixFilter>(filter_data);
    } else {
        return std::make_shared<KeyPrefixFilter>(filter_data);
    }
}


std::shared_ptr<DocFilter> DocFilter::makeFilter(int32_t mode, const UserMem& filter_data) {
    if(mode & YOKAN_MODE_LUA_FILTER) {
#ifdef YOKAN_HAS_LUA
        return std::make_shared<LuaDocFilter>(filter_data);
#endif
    }
    return std::make_shared<DocFilter>();
}

}
#endif

/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_FILTERS_HPP
#define __YOKAN_FILTERS_HPP

#include "yokan/common.h"
#include "yokan/filters.hpp"
#include "../../common/linker.hpp"
#include "../../common/logging.h"
#include "config.h"
#include <algorithm>
#include <memory>
#include <cstring>
#include <iostream>
#ifdef YOKAN_HAS_LUA
#include <sol/sol.hpp>
#include "lua-cjson/lua_cjson.h"
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

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        (void)val;
        (void)vsize;
        if(m_prefix.size > ksize)
            return false;
        return std::memcmp(key, m_prefix.data, m_prefix.size) == 0;
    }

    size_t keySizeFrom(const void* key, size_t ksize) const override {
        (void)key;
        if(m_mode & YOKAN_MODE_NO_PREFIX)
            return ksize - m_prefix.size;
        else
            return ksize;
    }

    size_t valSizeFrom(const void* val, size_t vsize) const override {
        (void)val;
        return vsize;
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize) const override {
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

    bool shouldStop(
        const void* key, size_t ksize,
        const void* val, size_t vsize) const override {
        (void)val;
        (void)vsize;
        auto x = std::memcmp(key, m_prefix.data, std::min<size_t>(ksize, m_prefix.size));
        return x > 0;
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

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        (void)val;
        (void)vsize;
        if(m_suffix.size > ksize)
            return false;
        return std::memcmp(((const char*)key)+ksize-m_suffix.size, m_suffix.data, m_suffix.size) == 0;
    }

    size_t keySizeFrom(const void* key, size_t ksize) const override {
        (void)key;
        if(m_mode & YOKAN_MODE_NO_PREFIX)
            return ksize - m_suffix.size;
        else
            return ksize;
    }

    size_t valSizeFrom(const void* val, size_t vsize) const override {
        (void)val;
        return vsize;
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize) const override {
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

    bool check(const void* key, size_t ksize, const void* val, size_t vsize) const override {
        m_lua["__key__"] = std::string_view(static_cast<const char*>(key), ksize);
        if(m_mode & YOKAN_MODE_FILTER_VALUE)
            m_lua["__value__"] = std::string_view(static_cast<const char*>(val), vsize);
        auto result = m_lua.do_string(std::string_view{ m_code.data, m_code.size });
        if(!result.valid()) return false;
        return static_cast<bool>(result);
    }

    size_t keySizeFrom(const void* key, size_t ksize) const override {
        (void)key;
        return ksize;
    }

    size_t valSizeFrom(const void* val, size_t vsize) const override {
        (void)val;
        return vsize;
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize) const override {
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

struct DefaultDocFilter : public DocFilter {

    DefaultDocFilter() = default;

    bool check(yk_id_t id, const void* val, size_t vsize) const override {
        (void)id;
        (void)val;
        (void)vsize;
        return true;
    }

    size_t docSizeFrom(const void* val, size_t vsize) const override {
        (void)val;
        return vsize;
    }

    size_t docCopy(
          void* dst, size_t max_dst_size,
          const void* doc, size_t docsize) const override {
        if (max_dst_size > docsize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, doc, docsize);
        return docsize;
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
        m_lua.require("cjson", luaopen_cjson);
    }

    bool check(yk_id_t id, const void* val, size_t vsize) const override {
        m_lua["__id__"] = id;
        m_lua["__doc__"] = std::string_view(static_cast<const char*>(val), vsize);
        auto result = m_lua.do_string(std::string_view{ m_code.data, m_code.size });
        if(!result.valid()) return false;
        return static_cast<bool>(result);
    }

    size_t docSizeFrom(const void* val, size_t vsize) const override {
        (void)val;
        return vsize;
    }

    size_t docCopy(
          void* dst, size_t max_dst_size,
          const void* doc, size_t docsize) const override {
        if (max_dst_size > docsize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, doc, docsize);
        return docsize;
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

    size_t valSizeFrom(const void* val, size_t vsize) const override {
        return m_doc_filter->docSizeFrom(val, vsize);
    }

    size_t valCopy(void* dst, size_t max_dst_size,
                   const void* val, size_t vsize) const override {
        return m_doc_filter->docCopy(dst, max_dst_size, val, vsize);
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

std::shared_ptr<KeyValueFilter> FilterFactory::makeKeyValueFilter(
        margo_instance_id mid, int32_t mode, const UserMem& filter_data) {
    if(mode & YOKAN_MODE_LUA_FILTER) {
#ifdef YOKAN_HAS_LUA
        (void)mid;
        return std::make_shared<LuaKeyValueFilter>(mode, filter_data);
#else
        YOKAN_LOG_ERROR(mid, "Yokan wasn't compiled with Lua support!");
        return nullptr;
#endif
    } else if(mode & YOKAN_MODE_LIB_FILTER) {
        const char* c1 = std::find(filter_data.data, filter_data.data + filter_data.size, ':');
        if(c1 == filter_data.data + filter_data.size) {
            YOKAN_LOG_ERROR(mid, "Invalid filter descriptor (should be \"<libname>:<filter>:<args>\")");
            return nullptr;
        }
        auto c2 = std::find(c1+1, (const char*)(filter_data.data + filter_data.size), ':');
        if(c2 == filter_data.data + filter_data.size) {
            YOKAN_LOG_ERROR(mid, "Invalid filter descriptor (should be \"<libname>:<filter>:<args>\")");
            return nullptr;
        }
        auto lib_name = std::string(filter_data.data, c1 - filter_data.data);
        auto filter_name = std::string(c1+1, c2-c1-1);
        auto filter_args = UserMem{
            const_cast<char*>(c2)+1,
            filter_data.size - (c2+1-filter_data.data)
        };
        if(!lib_name.empty()) Linker::open(lib_name);
        auto it = s_make_kv_filter.find(filter_name);
        if(it == s_make_kv_filter.end()) {
            YOKAN_LOG_ERROR(mid, "Could not find filter with name %s in FilterFactory", filter_name.c_str());
            return nullptr;
        }
        return (it->second)(mid, mode, filter_args);
    } else if(mode & YOKAN_MODE_SUFFIX) {
        return std::make_shared<KeySuffixFilter>(mode, filter_data);
    } else { // default is a prefix filter
        return std::make_shared<KeyPrefixFilter>(mode, filter_data);
    }
}


std::shared_ptr<DocFilter> FilterFactory::makeDocFilter(
        margo_instance_id mid, int32_t mode, const UserMem& filter_data) {
    if(mode & YOKAN_MODE_LUA_FILTER) {
#ifdef YOKAN_HAS_LUA
        (void)mid;
        return std::make_shared<LuaDocFilter>(mode, filter_data);
#else
        YOKAN_LOG_ERROR(mid, "Yokan wasn't compiled with Lua support!");
        return nullptr;
#endif
    } else if(mode & YOKAN_MODE_LIB_FILTER) {
        const char* c1 = std::find(filter_data.data, filter_data.data + filter_data.size, ':');
        if(c1 == filter_data.data + filter_data.size) {
            YOKAN_LOG_ERROR(mid, "Invalid filter descriptor (should be \"<libname>:<filter>:<args>\")");
            return nullptr;
        }
        auto c2 = std::find(c1+1, (const char*)(filter_data.data + filter_data.size), ':');
        if(c2 == filter_data.data + filter_data.size) {
            YOKAN_LOG_ERROR(mid, "Invalid filter descriptor (should be \"<libname>:<filter>:<args>\")");
            return nullptr;
        }
        auto lib_name = std::string(filter_data.data, c1-filter_data.data);
        auto filter_name = std::string(c1+1, c2-c1-1);
        auto filter_args = UserMem{
            const_cast<char*>(c2)+1,
            filter_data.size - (c2+1-filter_data.data)
        };
        if(!lib_name.empty()) Linker::open(lib_name);
        auto it = s_make_doc_filter.find(filter_name);
        if(it == s_make_doc_filter.end()) {
            YOKAN_LOG_ERROR(mid, "Could not find filter with name %s in FilterFactory", filter_name.c_str());
            return nullptr;
        }
        return (it->second)(mid, mode, filter_args);
    }
    return std::make_shared<DefaultDocFilter>();
}

std::shared_ptr<KeyValueFilter> FilterFactory::docToKeyValueFilter(
            std::shared_ptr<DocFilter> filter,
            const char* collection) {
    return std::make_shared<CollectionFilterWrapper>(collection, std::move(filter));
}

std::unordered_map<
        std::string,
        std::function<
            std::shared_ptr<KeyValueFilter>(margo_instance_id, int32_t, const UserMem&)
        >
    > FilterFactory::s_make_kv_filter;

std::unordered_map<
        std::string,
        std::function<
            std::shared_ptr<DocFilter>(margo_instance_id, int32_t, const UserMem&)
        >
    > FilterFactory::s_make_doc_filter;

}
#endif

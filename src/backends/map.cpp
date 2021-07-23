/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include "../common/locks.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <map>
#include <string>
#include <cstring>
#include <iostream>

namespace rkv {

using json = nlohmann::json;

template<typename KeyType>
struct MapKeyValueStoreCompare {

    // LCOV_EXCL_START
    static bool DefaultMemCmp(const void* lhs, size_t lhsize,
                              const void* rhs, size_t rhsize) {
        auto r = std::memcmp(lhs, rhs, std::min(lhsize, rhsize));
        if(r < 0) return true;
        if(r > 0) return false;
        if(lhsize < rhsize)
            return true;
        return false;
    }
    // LCOV_EXCL_STOP

    using cmp_type = bool (*)(const void*, size_t, const void*, size_t);

    cmp_type cmp = &DefaultMemCmp;

    MapKeyValueStoreCompare() = default;

    MapKeyValueStoreCompare(cmp_type comparator)
    : cmp(comparator) {}

    bool operator()(const KeyType& lhs, const KeyType& rhs) const {
        return cmp(lhs.data(), lhs.size(), rhs.data(), rhs.size());
    }

    bool operator()(const KeyType& lhs, const UserMem& rhs) const {
        return cmp(lhs.data(), lhs.size(), rhs.data, rhs.size);
    }

    bool operator()(const UserMem& lhs, const KeyType& rhs) const {
        return cmp(lhs.data, lhs.size, rhs.data(), rhs.size());
    }

    bool operator()(const UserMem& lhs, const UserMem& rhs) const {
        return cmp(lhs.data, lhs.size, rhs.data, rhs.size);
    }

    using is_transparent = int;
};

class MapKeyValueStore : public KeyValueStoreInterface {

    public:

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;
        cmp_type cmp = comparator::DefaultMemCmp;
        rkv_allocator_init_fn key_alloc_init, val_alloc_init, node_alloc_init;
        rkv_allocator_t key_alloc, val_alloc, node_alloc;
        std::string key_alloc_conf, val_alloc_conf, node_alloc_conf;

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            // check use_lock
            auto use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;
            // check comparator field
            if(!cfg.contains("comparator"))
                cfg["comparator"] = "default";
            auto comparator = cfg.value("comparator", "default");
            if(comparator != "default")
                cmp = Linker::load<cmp_type>(comparator);
            if(cmp == nullptr)
                return Status::InvalidConf;
            // check allocators
            if(!cfg.contains("allocators")) {
                cfg["allocators"]["key_allocator"] = "default";
                cfg["allocators"]["value_allocator"] = "default";
                cfg["allocators"]["pair_allocator"] = "default";
            } else if(!cfg["allocators"].is_object()) {
                return Status::InvalidConf;
            }

            auto& alloc_cfg = cfg["allocators"];

            // key allocator
            auto key_allocator_name = alloc_cfg.value("key_allocator", "default");
            auto key_allocator_config = alloc_cfg.value("key_allocator_config", json::object());
            alloc_cfg["key_allocator"] = key_allocator_name;
            alloc_cfg["key_allocator_config"] = key_allocator_config;
            if(key_allocator_name == "default")
                key_alloc_init = default_allocator_init;
            else
                key_alloc_init = Linker::load<decltype(key_alloc_init)>(key_allocator_name);
            if(key_alloc_init == nullptr) return Status::InvalidConf;
            key_alloc_init(&key_alloc, key_allocator_config.dump().c_str());

            // value allocator
            auto val_allocator_name = alloc_cfg.value("value_allocator", "default");
            auto val_allocator_config = alloc_cfg.value("value_allocator_config", json::object());
            alloc_cfg["value_allocator"] = val_allocator_name;
            alloc_cfg["value_allocator_config"] = val_allocator_config;
            if(val_allocator_name == "default")
                val_alloc_init = default_allocator_init;
            else
                val_alloc_init = Linker::load<decltype(val_alloc_init)>(val_allocator_name);
            if(val_alloc_init == nullptr) return Status::InvalidConf;
            val_alloc_init(&val_alloc, val_allocator_config.dump().c_str());

            // node allocator
            auto node_allocator_name = alloc_cfg.value("node_allocator", "default");
            auto node_allocator_config = alloc_cfg.value("node_allocator_config", json::object());
            alloc_cfg["node_allocator"] = node_allocator_name;
            alloc_cfg["node_allocator_config"] = node_allocator_config;
            if(node_allocator_name == "default")
                node_alloc_init = default_allocator_init;
            else
                node_alloc_init = Linker::load<decltype(node_alloc_init)>(node_allocator_name);
            if(node_alloc_init == nullptr) return Status::InvalidConf;
            node_alloc_init(&node_alloc, node_allocator_config.dump().c_str());

        } catch(...) {
            return Status::InvalidConf;
        }
        *kvs = new MapKeyValueStore(std::move(cfg), cmp, node_alloc, key_alloc, val_alloc);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "map";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual bool supportsMode(int32_t mode) const override {
        return mode == 0 || mode == 1;
    }

    virtual void destroy() override {
        ScopedWriteLock lock(m_lock);
        m_db->clear();
    }

    virtual Status exists(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            const UserMem key{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            flags[i] = m_db->count(key) > 0;
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            const UserMem key{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto it = m_db->find(key);
            if(it == m_db->end()) vsizes[i] = KeyNotFound;
            else vsizes[i] = it->second.size();
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status put(int32_t mode,
                       const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;

        size_t total_ksizes = std::accumulate(ksizes.data,
                                              ksizes.data + ksizes.size,
                                              0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        size_t total_vsizes = std::accumulate(vsizes.data,
                                              vsizes.data + vsizes.size,
                                              0);
        if(total_vsizes > vals.size) return Status::InvalidArg;

        ScopedWriteLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            auto p = m_db->emplace(std::piecewise_construct,
                std::forward_as_tuple(keys.data + key_offset,
                                      ksizes[i], m_key_allocator),
                std::forward_as_tuple(vals.data + val_offset,
                                      vsizes[i], m_val_allocator));
            if(!p.second) {
                p.first->second.assign(vals.data + val_offset,
                                       vsizes[i]);
            }
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        return Status::OK;
    }

    virtual Status get(int32_t mode, bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;
        ScopedReadLock lock(m_lock);

        if(!packed) {

            for(size_t i = 0; i < ksizes.size; i++) {
                const UserMem key{ keys.data + key_offset, ksizes[i] };
                auto it = m_db->find(key);
                const auto original_vsize = vsizes[i];
                if(it == m_db->end()) {
                    vsizes[i] = KeyNotFound;
                } else {
                    auto& v = it->second;
                    if(v.size() > vsizes[i]) {
                        vsizes[i] = BufTooSmall;
                    } else {
                        std::memcpy(vals.data + val_offset, v.data(), v.size());
                        vsizes[i] = v.size();
                    }
                }
                key_offset += ksizes[i];
                val_offset += original_vsize;
            }

        } else { // if packed

            size_t val_remaining_size = vals.size;

            for(size_t i = 0; i < ksizes.size; i++) {
                auto key = UserMem{ keys.data + key_offset, ksizes[i] };
                auto it = m_db->find(key);
                if(it == m_db->end()) {
                    vsizes[i] = KeyNotFound;
                } else if(it->second.size() > val_remaining_size) {
                    for(; i < ksizes.size; i++) {
                        vsizes[i] = BufTooSmall;
                    }
                } else {
                    auto& v = it->second;
                    std::memcpy(vals.data + val_offset, v.data(), v.size());
                    vsizes[i] = v.size();
                    val_remaining_size -= vsizes[i];
                    val_offset += vsizes[i];
                }
                key_offset += ksizes[i];
            }
            vals.size = vals.size - val_remaining_size;
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            auto key = UserMem{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto it = m_db->find(key);
            if(it != m_db->end()) {
                m_db->erase(it);
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        ScopedReadLock lock(m_lock);

        auto inclusive = mode & RKV_MODE_INCLUSIVE;

        if(!packed) {

            using iterator = decltype(m_db->begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db->begin();
            } else {
                fromKeyIt = inclusive ? m_db->lower_bound(fromKey) : m_db->upper_bound(fromKey);
            }
            const auto end = m_db->end();
            auto max = keySizes.size;
            size_t i = 0;
            size_t offset = 0;
            for(auto it = fromKeyIt; it != end && i < max; it++) {
                auto& key = it->first;
                if(prefix.size != 0) {
                    if(prefix.size > key.size()) continue;
                    if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                        continue;
                }
                size_t usize = keySizes[i];
                auto umem = static_cast<char*>(keys.data) + offset;
                if(usize < key.size()) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    i += 1;
                    offset += usize;
                    continue;
                }
                std::memcpy(umem, key.data(), key.size());
                keySizes[i] = key.size();
                offset += usize;
                i += 1;
            }
            keys.size = offset;
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
            }

        } else { // if packed

            using iterator = decltype(m_db->begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db->begin();
            } else {
                fromKeyIt = inclusive ? m_db->lower_bound(fromKey) : m_db->upper_bound(fromKey);
            }
            const auto end = m_db->end();
            auto max = keySizes.size;
            size_t i = 0;
            size_t offset = 0;
            bool buf_too_small = false;
            for(auto it = fromKeyIt; it != end && i < max; it++) {
                auto& key = it->first;
                if(prefix.size != 0) {
                    if(prefix.size > key.size()) continue;
                    if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                        continue;
                }
                auto umem = static_cast<char*>(keys.data) + offset;
                if(keys.size - offset < key.size() || buf_too_small) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    buf_too_small = true;
                } else {
                    std::memcpy(umem, key.data(), key.size());
                    keySizes[i] = key.size();
                    offset += key.size();
                }
                i += 1;
            }
            keys.size = offset;
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
            }

        }
        return Status::OK;
    }

    virtual Status listKeyValues(int32_t mode,
                                 bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        ScopedReadLock lock(m_lock);

        auto inclusive = mode & RKV_MODE_INCLUSIVE;

        if(!packed) {

            using iterator = decltype(m_db->begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db->begin();
            } else {
                fromKeyIt = inclusive ? m_db->lower_bound(fromKey) : m_db->upper_bound(fromKey);
            }
            const auto end = m_db->end();
            auto max = keySizes.size;
            size_t i = 0;
            size_t key_offset = 0;
            size_t val_offset = 0;
            for(auto it = fromKeyIt; it != end && i < max; it++) {
                auto& key = it->first;
                auto& val = it->second;
                if(prefix.size != 0) {
                    if(prefix.size > key.size()) continue;
                    if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                        continue;
                }
                size_t key_usize = keySizes[i];
                size_t val_usize = valSizes[i];
                auto key_umem = static_cast<char*>(keys.data) + key_offset;
                auto val_umem = static_cast<char*>(vals.data) + val_offset;
                if(key_usize < key.size()) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                } else {
                    std::memcpy(key_umem, key.data(), key.size());
                    keySizes[i] = key.size();
                }
                if(val_usize < val.size()) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                } else {
                    std::memcpy(val_umem, val.data(), val.size());
                    valSizes[i] = val.size();
                }
                key_offset += key_usize;
                val_offset += val_usize;
                i += 1;
            }
            keys.size = key_offset;
            vals.size = val_offset;
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
                valSizes[i] = RKV_NO_MORE_KEYS;
            }

        } else { // if packed

            using iterator = decltype(m_db->begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db->begin();
            } else {
                fromKeyIt = inclusive ? m_db->lower_bound(fromKey) : m_db->upper_bound(fromKey);
            }
            const auto end = m_db->end();
            auto max = keySizes.size;
            size_t i = 0;
            size_t key_offset = 0;
            size_t val_offset = 0;
            bool key_buf_too_small = false;
            bool val_buf_too_small = false;
            for(auto it = fromKeyIt; it != end && i < max; it++) {
                auto& key = it->first;
                auto& val = it->second;
                if(prefix.size != 0) {
                    if(prefix.size > key.size()) continue;
                    if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                        continue;
                }
                auto key_umem = static_cast<char*>(keys.data) + key_offset;
                auto val_umem = static_cast<char*>(vals.data) + val_offset;
                if(key_buf_too_small
                || keys.size - key_offset < key.size()) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_buf_too_small = true;
                } else {
                    std::memcpy(key_umem, key.data(), key.size());
                    keySizes[i] = key.size();
                    key_offset += key.size();
                }
                if(val_buf_too_small
                || vals.size - val_offset < val.size()) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                    val_buf_too_small = true;
                } else {
                    std::memcpy(val_umem, val.data(), val.size());
                    valSizes[i] = val.size();
                    val_offset += val.size();
                }
                i += 1;
            }
            keys.size = key_offset;
            vals.size = val_offset;
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
                valSizes[i] = RKV_NO_MORE_KEYS;
            }

        }
        return Status::OK;
    }

    ~MapKeyValueStore() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
        delete m_db;
        m_key_allocator.finalize(m_key_allocator.context);
        m_val_allocator.finalize(m_val_allocator.context);
        m_node_allocator.finalize(m_node_allocator.context);
    }

    private:

    using key_type = std::basic_string<char, std::char_traits<char>,
                                       Allocator<char>>;
    using value_type = std::basic_string<char, std::char_traits<char>,
                                         Allocator<char>>;
    using comparator = MapKeyValueStoreCompare<key_type>;
    using cmp_type = comparator::cmp_type;
    using allocator = Allocator<std::pair<const key_type, value_type>>;
    using map_type = std::map<key_type, value_type, comparator, allocator>;

    MapKeyValueStore(json cfg,
                     cmp_type cmp_fun,
                     const rkv_allocator_t& node_allocator,
                     const rkv_allocator_t& key_allocator,
                     const rkv_allocator_t& val_allocator)
    : m_config(std::move(cfg))
    , m_node_allocator(node_allocator)
    , m_key_allocator(key_allocator)
    , m_val_allocator(val_allocator)
    {
        if(m_config["use_lock"].get<bool>())
            ABT_rwlock_create(&m_lock);
        m_db = new map_type(cmp_fun, allocator(m_node_allocator));
    }

    map_type*       m_db;
    json            m_config;
    ABT_rwlock      m_lock = ABT_RWLOCK_NULL;
    rkv_allocator_t m_node_allocator;
    rkv_allocator_t m_key_allocator;
    rkv_allocator_t m_val_allocator;
};

}

RKV_REGISTER_BACKEND(map, rkv::MapKeyValueStore);

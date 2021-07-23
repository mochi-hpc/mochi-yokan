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
#include <unordered_set>
#include <string>
#include <cstring>
#include <iostream>
#if __cplusplus >= 201703L
#include <string_view>
#else
#include <experimental/string_view>
#endif

namespace rkv {

using json = nlohmann::json;

template<typename KeyType>
struct UnorderedSetKeyValueStoreHash {

#if __cplusplus >= 201703L
    using sv = std::string_view;
#else
    using sv = std::experimental::string_view;
#endif

    std::size_t operator()(KeyType const& key) const noexcept
    {
        return std::hash<sv>{}({ key.data(), key.size() });
    }
};

class UnorderedSetKeyValueStore : public KeyValueStoreInterface {

    public:

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;
        rkv_allocator_init_fn key_alloc_init, node_alloc_init;
        rkv_allocator_t key_alloc, node_alloc;
        std::string key_alloc_conf, node_alloc_conf;

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            // check use_lock
            bool use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;

            // bucket number
            if(!cfg.contains("initial_bucket_count")) {
                cfg["initial_bucket_count"] = 23;
            } else {
                if(!cfg["initial_bucket_count"].is_number_unsigned())
                    return Status::InvalidConf;
            }

            // check allocators
            if(!cfg.contains("allocators")) {
                cfg["allocators"]["key_allocator"] = "default";
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

            // node allocator
            auto node_allocator_name = alloc_cfg.value("node_allocator", "default");
            auto node_allocator_config = alloc_cfg.value("node_allocator_config", json::object());
            alloc_cfg["node_allocator"] = node_allocator_name;
            alloc_cfg["node_allocator_config"] = node_allocator_config;
            if(node_allocator_name == "default")
                node_alloc_init = default_allocator_init;
            else
                node_alloc_init = Linker::load<decltype(node_alloc_init)>(node_allocator_name);
            if(node_alloc_init == nullptr) {
                key_alloc.finalize(key_alloc.context);
                return Status::InvalidConf;
            }
            node_alloc_init(&node_alloc, node_allocator_config.dump().c_str());

        } catch(...) {
            return Status::InvalidConf;
        }
        *kvs = new UnorderedSetKeyValueStore(std::move(cfg), node_alloc, key_alloc);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "unordered_set";
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

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            flags[i] = m_db->count(key) > 0;
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            auto it = m_db->find(key);
            if(it == m_db->end()) vsizes[i] = KeyNotFound;
            else vsizes[i] = 0;
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status put(int32_t mode, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        (void)vals;
        size_t key_offset = 0;

        size_t total_ksizes = std::accumulate(ksizes.data,
                                              ksizes.data + ksizes.size,
                                              0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        size_t total_vsizes = std::accumulate(vsizes.data,
                                              vsizes.data + vsizes.size,
                                              0);
        if(total_vsizes > 0) return Status::InvalidArg;

        ScopedWriteLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            m_db->emplace(keys.data + key_offset,
                          ksizes[i], m_key_allocator);
            key_offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status get(int32_t mode, bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        (void)packed;
        (void)mode;
        size_t key_offset = 0;
        ScopedReadLock lock(m_lock);

        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            key.assign(keys.data + key_offset, ksizes[i]);
            auto it = m_db->find(key);
            if(it == m_db->end()) {
                vsizes[i] = KeyNotFound;
            } else {
                vsizes[i] = 0;
            }
            key_offset += ksizes[i];
        }
        vals.size = 0;
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            auto it = m_db->find(key);
            if(it != m_db->end()) {
                m_db->erase(it);
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    ~UnorderedSetKeyValueStore() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
        delete m_db;
        m_key_allocator.finalize(m_key_allocator.context);
        m_node_allocator.finalize(m_node_allocator.context);
    }

    private:

    using key_type = std::basic_string<char, std::char_traits<char>,
                                       Allocator<char>>;
    using equal_type = std::equal_to<key_type>;
    using allocator = Allocator<key_type>;
    using hash_type = UnorderedSetKeyValueStoreHash<key_type>;
    using unordered_set_type = std::unordered_set<key_type, hash_type, equal_type, allocator>;

    UnorderedSetKeyValueStore(json cfg,
                     const rkv_allocator_t& node_allocator,
                     const rkv_allocator_t& key_allocator)
    : m_config(std::move(cfg))
    , m_node_allocator(node_allocator)
    , m_key_allocator(key_allocator)
    {
        if(m_config["use_lock"].get<bool>())
            ABT_rwlock_create(&m_lock);
        m_db = new unordered_set_type(
                m_config["initial_bucket_count"].get<size_t>(),
                hash_type(),
                equal_type(),
                allocator(m_node_allocator));
    }

    unordered_set_type* m_db;
    json                m_config;
    ABT_rwlock          m_lock = ABT_RWLOCK_NULL;
    mutable rkv_allocator_t     m_node_allocator;
    mutable rkv_allocator_t     m_key_allocator;
};

}

RKV_REGISTER_BACKEND(unordered_set, rkv::UnorderedSetKeyValueStore);

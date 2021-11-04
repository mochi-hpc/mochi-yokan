/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/watcher.hpp"
#include "yokan/doc-mixin.hpp"
#include "yokan/util/locks.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include "../common/modes.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <unordered_map>
#include <string>
#include <cstring>
#include <iostream>
#if __cplusplus >= 201703L
#include <string_view>
#else
#include <experimental/string_view>
#endif

namespace yokan {

using json = nlohmann::json;

// TODO we could dependency-inject a hash function (and the to_equal<T> function)
template<typename KeyType>
struct UnorderedMapDatabaseHash {

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

class UnorderedMapDatabase : public DocumentStoreMixin<DatabaseInterface> {

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
        json cfg;
        yk_allocator_init_fn key_alloc_init, val_alloc_init, node_alloc_init;
        yk_allocator_t key_alloc, val_alloc, node_alloc;
        std::string key_alloc_conf, val_alloc_conf, node_alloc_conf;

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
                cfg["allocators"]["value_allocator"] = "default";
                cfg["allocators"]["node_allocator"] = "default";
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
            if(val_alloc_init == nullptr) {
                key_alloc.finalize(key_alloc.context);
                return Status::InvalidConf;
            }
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
            if(node_alloc_init == nullptr) {
                key_alloc.finalize(key_alloc.context);
                val_alloc.finalize(val_alloc.context);
                return Status::InvalidConf;
            }
            node_alloc_init(&node_alloc, node_allocator_config.dump().c_str());

        } catch(...) {
            return Status::InvalidConf;
        }
        *kvs = new UnorderedMapDatabase(std::move(cfg), node_alloc, key_alloc, val_alloc);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "unordered_map";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual bool supportsMode(int32_t mode) const override {
        // note we mark YOKAN_MODE_IGNORE_KEYS, KEEP_LAST, and SUFFIX
        // as supported, but the listKeys and listKeyvals are not
        // supported anyway.
        return mode ==
            (mode & (
                     YOKAN_MODE_INCLUSIVE
                    |YOKAN_MODE_APPEND
        //            |YOKAN_MODE_CONSUME
                    |YOKAN_MODE_WAIT
                    |YOKAN_MODE_NOTIFY
                    |YOKAN_MODE_NEW_ONLY
                    |YOKAN_MODE_EXIST_ONLY
                    |YOKAN_MODE_NO_PREFIX
                    |YOKAN_MODE_IGNORE_KEYS
                    |YOKAN_MODE_KEEP_LAST
                    |YOKAN_MODE_SUFFIX
#ifdef YOKAN_HAS_LUA
                    |YOKAN_MODE_LUA_FILTER
#endif
                    )
            );
    }

    virtual void destroy() override {
        ScopedWriteLock lock(m_lock);
        m_db->clear();
    }

    virtual Status count(int32_t mode, uint64_t* c) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        *c = m_db->size();
        return Status::OK;
    }

    virtual Status exists(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        const auto mode_wait = mode & YOKAN_MODE_WAIT;
        ScopedReadLock lock(m_lock);
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            retry:
            if(m_db->count(key) > 0)
                flags[i] = true;
            else if(mode_wait) {
                auto key_umem = UserMem{keys.data + offset, ksizes[i]};
                m_watcher.addKey(key_umem);
                lock.unlock();
                auto ret = m_watcher.waitKey(key_umem);
                lock.lock();
                if(ret == KeyWatcher::KeyPresent)
                    goto retry;
                else
                    return Status::TimedOut;
            } else {
                flags[i] = false;
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        const auto mode_wait = mode & YOKAN_MODE_WAIT;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            retry:
            auto it = m_db->find(key);
            if(it != m_db->end())
                vsizes[i] = it->second.size();
            else if(mode_wait) {
                auto key_umem = UserMem{keys.data + offset, ksizes[i]};
                m_watcher.addKey(key_umem);
                lock.unlock();
                auto ret = m_watcher.waitKey(key_umem);
                lock.lock();
                if(ret == KeyWatcher::KeyPresent)
                    goto retry;
                else
                    return Status::TimedOut;
            } else {
                vsizes[i] = KeyNotFound;
            }
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

        const auto mode_append     = mode & YOKAN_MODE_APPEND;
        const auto mode_new_only   = mode & YOKAN_MODE_NEW_ONLY;
        const auto mode_exist_only = mode & YOKAN_MODE_EXIST_ONLY;
        const auto mode_notify     = mode & YOKAN_MODE_NOTIFY;
        // note: mode_append and mode_new_only can't be provided
        // at the same time. mode_new_only and mode_exist_only either.
        // mode_append and mode_exists_only can.

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

            if(mode_new_only) {

                // contrary to std::map, std::unordered_map::find is not a template
                // function, and it needs to take a Key argument matching the type
                // stored. Templated find will be available in C++20, but for now
                // the following is not possible:
                // auto it = m_db->find(UserMem{ keys.data + key_offset, ksizes[i] });
                auto it = m_db->find(key_type{ keys.data + key_offset, ksizes[i], m_key_allocator });
                if(it == m_db->end()) {
                    m_db->emplace(std::piecewise_construct,
                            std::forward_as_tuple(keys.data + key_offset,
                                ksizes[i], m_key_allocator),
                            std::forward_as_tuple(vals.data + val_offset,
                                vsizes[i], m_val_allocator));
                    if(mode_notify)
                        m_watcher.notifyKey({keys.data + key_offset, ksizes[i]});
                }

            } else if(mode_exist_only) { // may of may not have mode_append

                // See earlier comment. Until C++20, the following is not possible:
                //auto it = m_db->find(UserMem{ keys.data + key_offset, ksizes[i] });
                auto it = m_db->find(key_type{ keys.data + key_offset, ksizes[i], m_key_allocator });
                if(it != m_db->end()) {
                    if(mode_append) {
                        it->second.append(vals.data + val_offset, vsizes[i]);
                    } else {
                        it->second.assign(vals.data + val_offset, vsizes[i]);
                    }
                    if(mode_notify)
                        m_watcher.notifyKey({keys.data + key_offset, ksizes[i]});
                }

            } else if(mode_append) { // but not mode_exist_only

                // See earlier comment. Until C++20, the following is not possible:
                // auto it = m_db->find(UserMem{ keys.data + key_offset, ksizes[i] });
                auto it = m_db->find(key_type{ keys.data + key_offset, ksizes[i], m_key_allocator });
                if(it != m_db->end()) {
                    it->second.append(vals.data + val_offset, vsizes[i]);
                } else {
                    m_db->emplace(std::piecewise_construct,
                            std::forward_as_tuple(keys.data + key_offset,
                                ksizes[i], m_key_allocator),
                            std::forward_as_tuple(vals.data + val_offset,
                                vsizes[i], m_val_allocator));
                }
                if(mode_notify)
                    m_watcher.notifyKey({keys.data + key_offset, ksizes[i]});

            } else { // normal mode

                auto p = m_db->emplace(std::piecewise_construct,
                        std::forward_as_tuple(keys.data + key_offset,
                                              ksizes[i], m_key_allocator),
                        std::forward_as_tuple(vals.data + val_offset,
                                              vsizes[i], m_val_allocator));
                if(!p.second) {
                    p.first->second.assign(vals.data + val_offset,
                                           vsizes[i]);
                }
                if(mode_notify)
                    m_watcher.notifyKey({keys.data + key_offset, ksizes[i]});

            }
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        return Status::OK;
    }

    virtual Status get(int32_t mode,
                       bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        const auto mode_wait = mode & YOKAN_MODE_WAIT;

        size_t key_offset = 0;
        size_t val_offset = 0;
        ScopedReadLock lock(m_lock);

        if(!packed) {

            auto key = key_type(m_key_allocator);
            for(size_t i = 0; i < ksizes.size; i++) {
                key.assign(keys.data + key_offset, ksizes[i]);
                const auto original_vsize = vsizes[i];
                retry:
                auto it = m_db->find(key);
                if(it == m_db->end()) {
                    if(mode_wait) {
                        auto key_umem = UserMem{keys.data + key_offset, ksizes[i]};
                        m_watcher.addKey(key_umem);
                        lock.unlock();
                        auto ret = m_watcher.waitKey(key_umem);
                        lock.lock();
                        if(ret == KeyWatcher::KeyPresent)
                            goto retry;
                        else
                            return Status::TimedOut;
                    } else {
                        vsizes[i] = KeyNotFound;
                    }
                } else {
                    auto& v = it->second;
                    vsizes[i] = valCopy(mode, vals.data + val_offset,
                                        original_vsize,
                                        v.data(), v.size());
                }
                key_offset += ksizes[i];
                val_offset += original_vsize;
            }

        } else { // if packed

            size_t val_remaining_size = vals.size;
            auto key = key_type(m_key_allocator);
            bool buf_too_small = false;

            for(size_t i = 0; i < ksizes.size; i++) {
                key.assign(keys.data + key_offset, ksizes[i]);
                retry_packed:
                auto it = m_db->find(key);
                if(it == m_db->end()) {
                    if(mode_wait) {
                        auto key_umem = UserMem{keys.data + key_offset, ksizes[i]};
                        m_watcher.addKey(key_umem);
                        lock.unlock();
                        auto ret = m_watcher.waitKey(key_umem);
                        lock.lock();
                        if(ret == KeyWatcher::KeyPresent)
                            goto retry_packed;
                        else
                            return Status::TimedOut;
                    } else {
                        vsizes[i] = KeyNotFound;
                    }
                } else if(buf_too_small) {
                    vsizes[i] = BufTooSmall;
                } else {
                    auto& v = it->second;
                    vsizes[i] = valCopy(mode, vals.data+val_offset,
                                        val_remaining_size,
                                        v.data(), v.size());
                    if(vsizes[i] == BufTooSmall)
                        buf_too_small = true;
                    else {
                        val_remaining_size -= vsizes[i];
                        val_offset += vsizes[i];
                    }
                }
                key_offset += ksizes[i];
            }
            vals.size = vals.size - val_remaining_size;
        }

        if(mode & YOKAN_MODE_CONSUME) {
            lock.unlock();
            return erase(mode, keys, ksizes);
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        const auto mode_wait = mode & YOKAN_MODE_WAIT;
        ScopedReadLock lock(m_lock);
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            retry:
            auto it = m_db->find(key);
            if(it != m_db->end()) {
                m_db->erase(it);
            } else if(mode_wait) {
                auto key_umem = UserMem{keys.data + offset, ksizes[i]};
                m_watcher.addKey(key_umem);
                lock.unlock();
                auto ret = m_watcher.waitKey(key_umem);
                lock.lock();
                if(ret == KeyWatcher::KeyPresent)
                    goto retry;
                else
                    return Status::TimedOut;
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    ~UnorderedMapDatabase() {
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
    using equal_type = std::equal_to<key_type>;
    using allocator = Allocator<std::pair<const key_type, value_type>>;
    using hash_type = UnorderedMapDatabaseHash<key_type>;
    using unordered_map_type = std::unordered_map<key_type, value_type, hash_type, equal_type, allocator>;

    UnorderedMapDatabase(json cfg,
                     const yk_allocator_t& node_allocator,
                     const yk_allocator_t& key_allocator,
                     const yk_allocator_t& val_allocator)
    : m_config(std::move(cfg))
    , m_node_allocator(node_allocator)
    , m_key_allocator(key_allocator)
    , m_val_allocator(val_allocator)
    {
        if(m_config["use_lock"].get<bool>())
            ABT_rwlock_create(&m_lock);
        m_db = new unordered_map_type(
                m_config["initial_bucket_count"].get<size_t>(),
                hash_type(),
                equal_type(),
                allocator(m_node_allocator));
    }

    unordered_map_type* m_db;
    json                m_config;
    ABT_rwlock          m_lock = ABT_RWLOCK_NULL;
    mutable yk_allocator_t     m_node_allocator;
    mutable yk_allocator_t     m_key_allocator;
    mutable yk_allocator_t     m_val_allocator;
    mutable KeyWatcher m_watcher;
};

}

YOKAN_REGISTER_BACKEND(unordered_map, yokan::UnorderedMapDatabase);

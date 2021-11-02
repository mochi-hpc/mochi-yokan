/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/watcher.hpp"
#include "yokan/util/locks.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include "../common/modes.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <map>
#include <string>
#include <cstring>
#include <iostream>

namespace yokan {

using json = nlohmann::json;

template<typename KeyType>
struct MapDatabaseCompare {

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

    MapDatabaseCompare() = default;

    MapDatabaseCompare(cmp_type comparator)
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

class MapDatabase : public DatabaseInterface {

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
        json cfg;
        cmp_type cmp = comparator::DefaultMemCmp;
        yk_allocator_init_fn key_alloc_init, val_alloc_init, node_alloc_init;
        yk_allocator_t key_alloc, val_alloc, node_alloc;
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
        *kvs = new MapDatabase(std::move(cfg), cmp, node_alloc, key_alloc, val_alloc);
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
        return mode ==
            (mode & (
                     YOKAN_MODE_INCLUSIVE
                    |YOKAN_MODE_APPEND
                    |YOKAN_MODE_CONSUME
                    |YOKAN_MODE_WAIT
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
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        auto mode_wait = mode & YOKAN_MODE_WAIT;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            const UserMem key{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            retry:
            auto it = m_db->find(key);
            if(it != m_db->end()) {
                flags[i] = true;
            } else if(mode_wait) {
                m_watcher.addKey(key);
                lock.unlock();
                auto ret = m_watcher.waitKey(key);
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
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        auto mode_wait = mode & YOKAN_MODE_WAIT;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            const UserMem key{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            retry:
            auto it = m_db->find(key);
            if(it != m_db->end()) {
                vsizes[i] = it->second.size();
            } else if(mode_wait) {
                m_watcher.addKey(key);
                lock.unlock();
                auto ret = m_watcher.waitKey(key);
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

        const auto mode_append     = mode & YOKAN_MODE_APPEND;
        const auto mode_new_only   = mode & YOKAN_MODE_NEW_ONLY;
        const auto mode_exist_only = mode & YOKAN_MODE_EXIST_ONLY;
        const auto mode_notify     = mode & YOKAN_MODE_NOTIFY;
        // note: mode_append and mode_new_only can't be provided
        // at the same time. mode_new_only and mode_exist_only either.
        // mode_append and mode_exists_only can.

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

            auto key_umem = UserMem{ keys.data + key_offset, ksizes[i] };

            if(mode_new_only) {

                auto it = m_db->find(key_umem);
                if(it == m_db->end()) {
                    m_db->emplace(std::piecewise_construct,
                            std::forward_as_tuple(keys.data + key_offset,
                                ksizes[i], m_key_allocator),
                            std::forward_as_tuple(vals.data + val_offset,
                                vsizes[i], m_val_allocator));
                    if(mode_notify)
                        m_watcher.notifyKey(key_umem);
                }

            } else if(mode_exist_only) { // may of may not have mode_append

                auto it = m_db->find(key_umem);
                if(it != m_db->end()) {
                    if(mode_append) {
                        it->second.append(vals.data + val_offset, vsizes[i]);
                    } else {
                        it->second.assign(vals.data + val_offset, vsizes[i]);
                    }
                    if(mode_notify)
                        m_watcher.notifyKey(key_umem);
                }

            } else if(mode_append) { // but not mode_exist_only
                auto it = m_db->find(key_umem);
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
                    m_watcher.notifyKey(key_umem);

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
                    m_watcher.notifyKey(key_umem);

            }
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        return Status::OK;
    }

    virtual Status get(int32_t mode, bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        bool mode_wait = mode & YOKAN_MODE_WAIT;

        size_t key_offset = 0;
        size_t val_offset = 0;
        ScopedReadLock lock(m_lock);

        if(!packed) {

            for(size_t i = 0; i < ksizes.size; i++) {
                const UserMem key{ keys.data + key_offset, ksizes[i] };
                const auto original_vsize = vsizes[i];
                retry:
                auto it = m_db->find(key);
                if(it == m_db->end()) {
                    if(mode_wait) {
                        m_watcher.addKey(key);
                        lock.unlock();
                        auto ret = m_watcher.waitKey(key);
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
            bool buf_too_small = false;

            for(size_t i = 0; i < ksizes.size; i++) {
                auto key = UserMem{ keys.data + key_offset, ksizes[i] };
                retry_packed:
                auto it = m_db->find(key);
                if(it == m_db->end()) {
                    if(mode_wait) {
                        m_watcher.addKey(key);
                        lock.unlock();
                        auto ret = m_watcher.waitKey(key);
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
                    vsizes[i] = valCopy(mode, vals.data + val_offset,
                                        val_remaining_size,
                                        v.data(), v.size());
                    if(vsizes[i] == BufTooSmall) {
                        buf_too_small = true;
                    } else {
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
        size_t offset = 0;
        auto mode_wait = mode & YOKAN_MODE_WAIT;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            auto key = UserMem{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            retry:
            auto it = m_db->find(key);
            if(it != m_db->end()) {
                m_db->erase(it);
            } else if(mode_wait) {
                m_watcher.addKey(key);
                lock.unlock();
                auto ret = m_watcher.waitKey(key);
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

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const UserMem& filter,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        ScopedReadLock lock(m_lock);

        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;
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
        auto key_filter = Filter{ mode, filter.data, filter.size };

        for(auto it = fromKeyIt; it != end && i < max; ++it) {
            auto& key = it->first;
            if(!key_filter.check(key.data(), key.size()))
                continue;

            size_t usize = packed ? (keys.size - offset) : keySizes[i];
            auto umem = static_cast<char*>(keys.data) + offset;

            bool is_last = false;
            if(mode & YOKAN_MODE_KEEP_LAST) {
                auto next = it;
                ++next;
                is_last = (i+1 == max) || (next == end);
            }

            if(!packed) {
                keySizes[i] = keyCopy(mode, umem, usize, key.data(),
                                      key.size(), filter.size, is_last);
                offset += usize;
            } else {
                if(buf_too_small) {
                    keySizes[i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    keySizes[i] = keyCopy(mode, umem, usize, key.data(),
                                          key.size(), filter.size, is_last);
                    if(keySizes[i] == YOKAN_SIZE_TOO_SMALL) {
                        buf_too_small = true;
                    } else {
                        offset += keySizes[i];
                    }
                }
            }
            i += 1;
        }

        keys.size = offset;
        for(; i < max; i++) {
            keySizes[i] = YOKAN_NO_MORE_KEYS;
        }

        return Status::OK;
    }

    virtual Status listKeyValues(int32_t mode,
                                 bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& filter,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        ScopedReadLock lock(m_lock);

        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;

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
        auto key_filter = Filter{ mode, filter.data, filter.size };

        for(auto it = fromKeyIt; it != end && i < max; it++) {
            auto& key = it->first;
            auto& val = it->second;
            if(!key_filter.check(key.data(), key.size()))
                continue;

            auto key_umem = static_cast<char*>(keys.data) + key_offset;
            auto val_umem = static_cast<char*>(vals.data) + val_offset;

            bool is_last = false;
            if(mode & YOKAN_MODE_KEEP_LAST) {
                auto next = it;
                ++next;
                is_last = (i+1 == max) || (next == end);
            }


            if(!packed) {

                size_t key_usize = keySizes[i];
                size_t val_usize = valSizes[i];
                keySizes[i] = keyCopy(mode, key_umem, key_usize,
                                      key.data(), key.size(),
                                      filter.size, is_last);
                valSizes[i] = valCopy(mode, val_umem, val_usize,
                                      val.data(), val.size());
                key_offset += key_usize;
                val_offset += val_usize;

            } else {

                size_t key_usize = keys.size - key_offset;
                size_t val_usize = vals.size - val_offset;

                if(key_buf_too_small) {
                    keySizes[i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    keySizes[i] = keyCopy(mode, key_umem, key_usize,
                                          key.data(), key.size(),
                                          filter.size, is_last);
                    if(keySizes[i] != YOKAN_SIZE_TOO_SMALL)
                        key_offset += keySizes[i];
                    else
                        key_buf_too_small = true;
                }
                if(val_buf_too_small) {
                    valSizes[i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    valSizes[i] = valCopy(mode, val_umem, val_usize,
                                          val.data(), val.size());
                    if(valSizes[i] != YOKAN_SIZE_TOO_SMALL)
                        val_offset += valSizes[i];
                    else
                        val_buf_too_small = true;
                }
            }
            i += 1;
        }

        keys.size = key_offset;
        vals.size = val_offset;
        for(; i < max; i++) {
            keySizes[i] = YOKAN_NO_MORE_KEYS;
            valSizes[i] = YOKAN_NO_MORE_KEYS;
        }

        return Status::OK;
    }

    ~MapDatabase() {
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
    using comparator = MapDatabaseCompare<key_type>;
    using cmp_type = comparator::cmp_type;
    using allocator = Allocator<std::pair<const key_type, value_type>>;
    using map_type = std::map<key_type, value_type, comparator, allocator>;

    MapDatabase(json cfg,
                     cmp_type cmp_fun,
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
        m_db = new map_type(cmp_fun, allocator(m_node_allocator));
    }

    map_type*          m_db;
    json               m_config;
    ABT_rwlock         m_lock = ABT_RWLOCK_NULL;
    yk_allocator_t     m_node_allocator;
    yk_allocator_t     m_key_allocator;
    yk_allocator_t     m_val_allocator;
    mutable KeyWatcher m_watcher;
};

}

YOKAN_REGISTER_BACKEND(map, yokan::MapDatabase);

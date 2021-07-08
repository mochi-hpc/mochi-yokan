/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <leveldb/db.h>
#include <leveldb/comparator.h>
#include <leveldb/env.h>
#include <string>
#include <cstring>
#include <iostream>
#include <experimental/filesystem>

namespace rkv {

using json = nlohmann::json;

#if 0
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

    bool operator()(const std::string& lhs, const std::string& rhs) const {
        return cmp(lhs.data(), lhs.size(), rhs.data(), rhs.size());
    }

    bool operator()(const std::string& lhs, const UserMem& rhs) const {
        return cmp(lhs.data(), lhs.size(), rhs.data, rhs.size);
    }

    bool operator()(const UserMem& lhs, const std::string& rhs) const {
        return cmp(lhs.data, lhs.size, rhs.data(), rhs.size());
    }

    bool operator()(const UserMem& lhs, const UserMem& rhs) const {
        return cmp(lhs.data, lhs.size, rhs.data, rhs.size);
    }

    using is_transparent = int;
};
#endif

class LevelDBKeyValueStore : public KeyValueStoreInterface {

    public:

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;
        try {
            cfg = json::parse(config);
        } catch(...) {
            return Status::InvalidConf;
        }
        // fill options and complete configuration
        leveldb::Options options;

#define SET_AND_COMPLETE(__field__, __value__) \
        options.__field__ = cfg.value(#__field__, __value__); \
        cfg[#__field__] = options.__field__

        SET_AND_COMPLETE(create_if_missing, false);
        SET_AND_COMPLETE(error_if_exists, false);
        SET_AND_COMPLETE(paranoid_checks, false);
        SET_AND_COMPLETE(write_buffer_size, (size_t)(4*1024*1024));
        SET_AND_COMPLETE(max_open_files, 1000);
        SET_AND_COMPLETE(block_size, (size_t)(4*1024));
        SET_AND_COMPLETE(block_restart_interval, 16);
        SET_AND_COMPLETE(max_file_size, (size_t)(2*1024*1024));
        SET_AND_COMPLETE(reuse_logs, false);
        options.compression = cfg.value("compression", true) ?
            leveldb::kSnappyCompression : leveldb::kNoCompression;
        cfg["compression"] = options.compression == leveldb::kSnappyCompression;
        // TODO set logger, env, block_cache, and filter_policy
        std::string path = cfg.value("path", "");
        if(path.empty()) {
            return Status::InvalidConf;
        }
        leveldb::Status status;
        leveldb::DB* db = nullptr;
        status = leveldb::DB::Open(options, path, &db);

        *kvs = new LevelDBKeyValueStore(db, std::move(cfg));

        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "leveldb";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual void destroy() override {
        auto path = m_config["path"].get<std::string>();
        std::experimental::filesystem::remove_all(path);
    }

    virtual Status exists(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
#if 0
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            const UserMem key{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            flags[i] = m_db.count(key) > 0;
            offset += ksizes[i];
        }
        return Status::OK;
#endif
        return Status::NotSupported;
    }

    virtual Status length(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
#if 0
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            const UserMem key{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto it = m_db.find(key);
            if(it == m_db.end()) vsizes[i] = KeyNotFound;
            else vsizes[i] = it->second.size();
            offset += ksizes[i];
        }
        return Status::OK;
#endif
        return Status::NotSupported;
    }

    virtual Status put(const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) override {
#if 0
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
            auto p = m_db.emplace(std::piecewise_construct,
                std::forward_as_tuple(keys.data + key_offset,
                                      ksizes[i]),
                std::forward_as_tuple(vals.data + val_offset,
                                      vsizes[i]));
            if(!p.second) {
                p.first->second.assign(vals.data + val_offset,
                                       vsizes[i]);
            }
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        return Status::OK;
#endif
        return Status::NotSupported;
    }

    virtual Status get(bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) const override {
#if 0
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;
        ScopedReadLock lock(m_lock);

        if(!packed) {

            for(size_t i = 0; i < ksizes.size; i++) {
                const UserMem key{ keys.data + key_offset, ksizes[i] };
                auto it = m_db.find(key);
                const auto original_vsize = vsizes[i];
                if(it == m_db.end()) {
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
                auto it = m_db.find(key);
                if(it == m_db.end()) {
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
#endif
        return Status::NotSupported;
    }

    virtual Status erase(const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
#if 0
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            auto key = UserMem{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto it = m_db.find(key);
            if(it != m_db.end()) {
                m_db.erase(it);
            }
            offset += ksizes[i];
        }
        return Status::OK;
#endif
        return Status::NotSupported;
    }

    virtual Status listKeys(bool packed, const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
#if 0
        ScopedReadLock lock(m_lock);

        if(!packed) {

            using iterator = decltype(m_db.begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db.begin();
            } else {
                fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
            }
            const auto end = m_db.end();
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

            using iterator = decltype(m_db.begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db.begin();
            } else {
                fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
            }
            const auto end = m_db.end();
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
#endif
        return Status::NotSupported;
    }

    virtual Status listKeyValues(bool packed,
                                 const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
#if 0
        ScopedReadLock lock(m_lock);

        if(!packed) {

            using iterator = decltype(m_db.begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db.begin();
            } else {
                fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
            }
            const auto end = m_db.end();
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

            using iterator = decltype(m_db.begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db.begin();
            } else {
                fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
            }
            const auto end = m_db.end();
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
#endif
        return Status::NotSupported;
    }

    ~LevelDBKeyValueStore() {
        delete m_db;
    }

    private:

    LevelDBKeyValueStore(leveldb::DB* db, json&& cfg)
    : m_db(db)
    , m_config(std::move(cfg)) {}

    leveldb::DB* m_db;
    json m_config;
};

}

RKV_REGISTER_BACKEND(leveldb, rkv::LevelDBKeyValueStore);

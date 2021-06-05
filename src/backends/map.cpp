/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include <abt.h>
#include <map>
#include <string>
#include <cstring>

namespace rkv {

class ScopedWriteLock {

    public:

    ScopedWriteLock(ABT_rwlock lock)
    : m_lock(lock) {
        ABT_rwlock_wrlock(m_lock);
    }

    ~ScopedWriteLock() {
        ABT_rwlock_unlock(m_lock);
    }

    private:

    ABT_rwlock m_lock;
};

class ScopedReadLock {

    public:

    ScopedReadLock(ABT_rwlock lock)
    : m_lock(lock) {
        ABT_rwlock_rdlock(m_lock);
    }

    ~ScopedReadLock() {
        ABT_rwlock_unlock(m_lock);
    }

    private:

    ABT_rwlock m_lock;
};

struct MapKeyValueStoreCompare {

    static bool DefaultMemCmp(const void* lhs, size_t lhsize,
                              const void* rhs, size_t rhsize) {
        auto r = std::memcmp(lhs, rhs, std::min(lhsize, rhsize));
        if(r < 0) return true;
        if(r > 0) return false;
        if(lhsize < rhsize)
            return true;
        return false;
    }

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

class MapKeyValueStore : public KeyValueStoreInterface {

    public:

    static KeyValueStoreInterface* create(const std::string& config) {
        // TODO handle custom comparator
        return new MapKeyValueStore();
    }

    virtual std::string name() const override {
        return "map";
    }

    virtual std::string config() const override {
        return "{}";
    }

    virtual void destroy() override {
        ScopedWriteLock lock(m_lock);
        m_db.clear();
    }

    virtual Status exists(const UserMem& key, bool& b) const override {
        ScopedReadLock lock(m_lock);
        b = m_db.count(key) > 0;
        return Status::OK;
    }

    virtual Status existsMulti(const std::vector<UserMem>& keys,
                               std::vector<bool>& b) const override {
        b.resize(keys.size());
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < keys.size(); i++) {
            b[i] = m_db.count(keys[i]) > 0;
        }
        return Status::OK;
    }

    virtual Status length(const UserMem& key, size_t& size) const override {
        ScopedReadLock lock(m_lock);
        auto it = m_db.find(key);
        if(it == m_db.end())
            return Status::NotFound;
        else
            size = it->second.size();
        return Status::OK;
    }

    virtual Status lengthMulti(const std::vector<UserMem>& keys,
                               std::vector<size_t>& sizes) const override {
        sizes.resize(keys.size());
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < keys.size(); i++) {
            auto it = m_db.find(keys[i]);
            if(it == m_db.end()) sizes[i] = KeyNotFound;
            else sizes[i] = it->second.size();
        }
        return Status::OK;
    }

    virtual Status put(const UserMem& key, const UserMem& value) override {
        ScopedWriteLock lock(m_lock);
        auto p = m_db.emplace(std::piecewise_construct,
                              std::forward_as_tuple(
                                  static_cast<const char*>(key.data),
                                  key.size),
                              std::forward_as_tuple(
                                  static_cast<const char*>(value.data),
                                  value.size));
        if(!p.second) {
            p.first->second.assign(static_cast<const char*>(value.data),
                                   value.size);
        }
        return Status::OK;
    }

    virtual Status putMulti(const std::vector<UserMem>& keys,
                            const std::vector<UserMem>& vals) override {
        ScopedWriteLock lock(m_lock);
        if(keys.size() != vals.size())
            return Status::InvalidArg;

        for(size_t i = 0; i < keys.size(); i++) {
            auto p = m_db.emplace(std::piecewise_construct,
                    std::forward_as_tuple(
                        static_cast<const char*>(keys[i].data),
                        keys[i].size),
                    std::forward_as_tuple(
                        static_cast<const char*>(vals[i].data),
                        vals[i].size));
            if(!p.second) {
                p.first->second.assign(
                        static_cast<const char*>(vals[i].data),
                        vals[i].size);
            }
        }
        return Status::OK;
    }

    virtual Status get(const UserMem& key, UserMem& value) const override {
        ScopedReadLock lock(m_lock);
        auto it = m_db.find(key);
        if(it == m_db.end()) {
            return Status::NotFound;
        }
        auto& v = it->second;
        if(v.size() > value.size) {
            value.size = v.size();
            return Status::SizeError;
        }
        std::memcpy(value.data, v.data(), v.size());
        value.size = v.size();
        return Status::OK;
    }

    virtual Status getMulti(const std::vector<UserMem>& keys,
                            std::vector<UserMem>& values) const override {
        ScopedReadLock lock(m_lock);
        if(keys.size() != values.size())
            return Status::InvalidArg;

        for(size_t i = 0; i < keys.size(); i++) {
            auto& key = keys[i];
            auto& value = values[i];
            auto it = m_db.find(key);
            if(it == m_db.end()) {
                value.size = KeyNotFound;
                continue;
            }
            auto& v = it->second;
            if(v.size() > value.size) {
                value.size = BufTooSmall;
                continue;
            }
            std::memcpy(value.data, v.data(), v.size());
            value.size = v.size();
        }
        return Status::OK;
    }

    virtual Status get(const UserMem& key, std::string& value) const override {
        ScopedReadLock lock(m_lock);
        auto it = m_db.find(key);
        if(it == m_db.end()) {
            return Status::NotFound;
        }
        value = it->second;
        return Status::OK;
    }

    virtual Status getMulti(const std::vector<UserMem>& keys,
                            std::vector<std::string>& values,
                            const std::string& defaultValue = "") const override {
        if(keys.size() != values.size())
            return Status::InvalidArg;

        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < keys.size(); i++) {
            auto& key = keys[i];
            auto& value = values[i];
            auto it = m_db.find(key);
            if(it == m_db.end()) {
                value = defaultValue;
            } else {
                value = it->second;
            }
        }
        return Status::OK;
    }

    virtual Status erase(const UserMem& key) override {
        ScopedWriteLock lock(m_lock);
        auto it = m_db.find(key);
        if(it == m_db.end())
            return Status::NotFound;
        else {
            m_db.erase(it);
            return Status::OK;
        }
    }

    virtual Status eraseMulti(const std::vector<UserMem>& keys) override {
        ScopedWriteLock lock(m_lock);
        for(auto& key : keys) {
            auto it = m_db.find(key);
            if(it == m_db.end()) continue;
            m_db.erase(it);
        }
        return Status::OK;
    }

    virtual Status listKeys(const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            size_t max, std::vector<std::string>& keys) const override {
        ScopedReadLock lock(m_lock);
        using iterator = decltype(m_db.begin());
        iterator fromKeyIt;
        if(fromKey.size == 0) {
            fromKeyIt = m_db.begin();
        } else {
            fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
        }
        const auto end = m_db.end();
        keys.resize(0);
        size_t count = 0;
        for(auto it = fromKeyIt; it != end && count < max; it++) {
            auto& key = it->first;
            if(prefix.size != 0) {
                if(prefix.size > key.size()) continue;
                if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                    continue;
            }
            keys.push_back(key);
            count += 1;
        }
        return Status::OK;
    }

    virtual Status listKeys(const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            std::vector<UserMem>& keys) const override {
        ScopedReadLock lock(m_lock);
        using iterator = decltype(m_db.begin());
        iterator fromKeyIt;
        if(fromKey.size == 0) {
            fromKeyIt = m_db.begin();
        } else {
            fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
        }
        const auto end = m_db.end();
        auto max = keys.size();
        size_t count = 0;
        for(auto it = fromKeyIt; it != end && count < max; it++) {
            auto& key = it->first;
            if(prefix.size != 0) {
                if(prefix.size > key.size()) continue;
                if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                    continue;
            }
            auto& umem = keys[count];
            if(umem.size < key.size()) {
                umem.size = BufTooSmall;
            } else {
                std::memcpy(umem.data, key.data(), key.size());
                umem.size = key.size();
            }
            count += 1;
        }
        keys.resize(count);
        return Status::OK;
    }

    virtual Status listKeys(const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        ScopedReadLock lock(m_lock);
        using iterator = decltype(m_db.begin());
        iterator fromKeyIt;
        if(fromKey.size == 0) {
            fromKeyIt = m_db.begin();
        } else {
            fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
        }
        const auto end = m_db.end();
        auto max = keySizes.size;
        size_t count = 0;
        size_t offset = 0;
        for(auto it = fromKeyIt; it != end && count < max; it++) {
            auto& key = it->first;
            if(prefix.size != 0) {
                if(prefix.size > key.size()) continue;
                if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                    continue;
            }
            auto umem = static_cast<char*>(keys.data) + offset;
            if(keys.size - offset < key.size())
                break;
            std::memcpy(umem, key.data(), key.size());
            keySizes.data[count] = key.size();
            offset += key.size();
            count += 1;
        }
        keySizes.size = count;
        keys.size = offset;
        return Status::OK;
    }

    virtual Status listKeyValues(const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix, size_t max,
                                 std::vector<std::string>& keys,
                                 std::vector<std::string>& vals) const override {
        ScopedReadLock lock(m_lock);
        using iterator = decltype(m_db.begin());
        iterator fromKeyIt;
        if(fromKey.size == 0) {
            fromKeyIt = m_db.begin();
        } else {
            fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
        }
        const auto end = m_db.end();
        keys.resize(0);
        vals.resize(0);
        size_t count = 0;
        for(auto it = fromKeyIt; it != end && count < max; it++) {
            auto& key = it->first;
            auto& val = it->second;
            if(prefix.size != 0) {
                if(prefix.size > key.size()) continue;
                if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                    continue;
            }
            keys.push_back(key);
            vals.push_back(val);
            count += 1;
        }
        return Status::OK;
    }

    virtual Status listKeyValues(const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix,
                                 std::vector<UserMem>& keys, std::vector<UserMem>& vals) const override {
        ScopedReadLock lock(m_lock);
        if(keys.size() != vals.size()) {
            return Status::InvalidArg;
        }
        using iterator = decltype(m_db.begin());
        iterator fromKeyIt;
        if(fromKey.size == 0) {
            fromKeyIt = m_db.begin();
        } else {
            fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
        }
        const auto end = m_db.end();
        auto max = keys.size();
        size_t count = 0;
        for(auto it = fromKeyIt; it != end && count < max; it++) {
            auto& key = it->first;
            auto& val = it->second;
            if(prefix.size != 0) {
                if(prefix.size > key.size()) continue;
                if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                    continue;
            }
            auto& key_umem = keys[count];
            auto& val_umem = vals[count];
            if(key_umem.size < key.size()) {
                key_umem.size = BufTooSmall;
                val_umem.size = 0;
            } else if(val_umem.size < val.size()) {
                val_umem.size = BufTooSmall;
                key_umem.size = 0;
            } else {
                std::memcpy(key_umem.data, key.data(), key.size());
                key_umem.size = key.size();
                std::memcpy(val_umem.data, val.data(), val.size());
                val_umem.size = val.size();
            }
            count += 1;
        }
        keys.resize(count);
        vals.resize(count);
        return Status::OK;
    }

    virtual Status listKeyValues(const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        ScopedReadLock lock(m_lock);
        if(keySizes.size != valSizes.size)
            return Status::InvalidArg;
        using iterator = decltype(m_db.begin());
        iterator fromKeyIt;
        if(fromKey.size == 0) {
            fromKeyIt = m_db.begin();
        } else {
            fromKeyIt = inclusive ? m_db.lower_bound(fromKey) : m_db.upper_bound(fromKey);
        }
        const auto end = m_db.end();
        auto max = keySizes.size;
        size_t count = 0;
        size_t key_offset = 0;
        size_t val_offset = 0;
        for(auto it = fromKeyIt; it != end && count < max; it++) {
            auto& key = it->first;
            auto& val = it->second;
            if(prefix.size != 0) {
                if(prefix.size > key.size()) continue;
                if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                    continue;
            }
            auto key_umem = static_cast<char*>(keys.data) + key_offset;
            auto val_umem = static_cast<char*>(vals.data) + val_offset;
            if(keys.size - key_offset < key.size())
                break;
            if(vals.size - val_offset < val.size())
                break;
            std::memcpy(key_umem, key.data(), key.size());
            keySizes.data[count] = key.size();
            key_offset += key.size();
            std::memcpy(val_umem, val.data(), val.size());
            valSizes.data[count] = val.size();
            val_offset += val.size();
            count += 1;
        }
        keySizes.size = count;
        keys.size = key_offset;
        valSizes.size = count;
        vals.size = val_offset;
        return Status::OK;
    }

    ~MapKeyValueStore() {
        ABT_rwlock_free(&m_lock);
    }

    private:

    MapKeyValueStore() {
        ABT_rwlock_create(&m_lock);
    }

    MapKeyValueStore(MapKeyValueStoreCompare::cmp_type cmp_fun)
    : m_db(cmp_fun) {
        ABT_rwlock_create(&m_lock);
    }

    std::map<std::string, std::string, MapKeyValueStoreCompare> m_db;
    ABT_rwlock m_lock;
};

}

RKV_REGISTER_BACKEND(map, rkv::MapKeyValueStore);

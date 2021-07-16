/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include <nlohmann/json.hpp>
#include <db_cxx.h>
#include <dbstl_map.h>
#include <abt.h>
#include <string>
#include <cstring>
#include <iostream>
#include <experimental/filesystem>

namespace rkv {

using json = nlohmann::json;
namespace fs = std::experimental::filesystem;

class BerkeleyDBKeyValueStore : public KeyValueStoreInterface {

    public:

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;

#define CHECK_TYPE_AND_SET_DEFAULT(__cfg__, __field__, __type__, __default__) \
        do { if(__cfg__.contains(__field__)) { \
            if(!__cfg__[__field__].is_##__type__()) { \
                return Status::InvalidConf; \
            } \
        } else { \
            __cfg__[__field__] = __default__; \
        } } while(0)

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            CHECK_TYPE_AND_SET_DEFAULT(cfg, "create_if_missing", boolean, true);
            CHECK_TYPE_AND_SET_DEFAULT(cfg, "home", string, ".");
            CHECK_TYPE_AND_SET_DEFAULT(cfg, "file", string, "");
            CHECK_TYPE_AND_SET_DEFAULT(cfg, "name", string, "");

        } catch(...) {
            return Status::InvalidConf;
        }
        auto db_file = cfg["file"].get<std::string>();
        auto db_name = cfg["name"].get<std::string>();
        auto db_home = cfg["home"].get<std::string>();
        if(!db_home.empty()) db_home += "/rkv";

        uint32_t db_env_flags =
                DB_CREATE     |
                DB_RECOVER    |
                DB_INIT_LOCK  |
                DB_INIT_LOG   |
                DB_INIT_TXN   |
                DB_THREAD     |
                DB_INIT_MPOOL;
        uint32_t db_flags = 0;
        if(cfg["create_if_missing"].get<bool>()) {
           db_flags |= DB_CREATE;
        }

        if(!db_home.empty()) {
            std::error_code ec;
            fs::create_directories(db_home, ec);
            // TODO log on error
        }

        auto db_env = new DbEnv(DB_CXX_NO_EXCEPTIONS);
        db_env->open(db_home.c_str(), db_env_flags, 0);
        auto db = new Db(db_env, 0);
        db->open(nullptr, db_file.empty() ? nullptr : db_file.c_str(),
                          db_name.empty() ? nullptr : db_name.c_str(),
                          DB_BTREE, db_flags, 0);

        *kvs = new BerkeleyDBKeyValueStore(cfg, db_env, db);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "berkeleydb";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual void destroy() override {
        auto db_file = m_config["file"].get<std::string>();
        auto db_home = m_config["home"].get<std::string>();
        auto db_name = m_config["name"].get<std::string>();
        if(!db_home.empty()) db_home += "/rkv";

        m_db->close(0);
        m_db->remove(db_file.empty() ? nullptr : db_file.c_str(),
                     db_name.empty() ? nullptr : db_name.c_str(), 0);
        delete m_db;
        m_db = nullptr;

        m_db_env->close(0);
        delete m_db_env;
        m_db_env = nullptr;

        std::error_code ec;
        fs::remove_all(db_home, ec);
        // TODO log error if necessary
    }

    virtual Status exists(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            Dbt key{ keys.data + offset, (u_int32_t)ksizes[i] };
            key.set_flags(DB_DBT_USERMEM);
            key.set_ulen(ksizes[i]);
            int status = m_db->exists(nullptr, &key, 0);
            flags[i] = (status != DB_NOTFOUND);
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            Dbt key{ keys.data + offset, (u_int32_t)ksizes[i] };
            key.set_flags(DB_DBT_USERMEM);
            key.set_ulen(ksizes[i]);
            Dbt val{ nullptr, 0 };
            val.set_flags(DB_DBT_USERMEM);
            val.set_ulen(0);
            int status = m_db->get(nullptr, &key, &val, 0);
            if(status == DB_BUFFER_SMALL || status == 0) {
                vsizes[i] = val.get_size();
            } else if(status == DB_NOTFOUND) {
                vsizes[i] = KeyNotFound;
            } else {
                return Status::Other; // TODO add proper status conversion
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status put(const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) override {
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

        // TODO enable the use of transactions
        for(size_t i = 0; i < ksizes.size; i++) {
            Dbt key(keys.data + key_offset, ksizes[i]);
            Dbt val(vals.data + val_offset, vsizes[i]);
            key.set_flags(DB_DBT_USERMEM);
            key.set_ulen(ksizes[i]);
            val.set_flags(DB_DBT_USERMEM);
            val.set_ulen(vsizes[i]);
            // TODO enable DB_NOOVERWRITE is requested
            int flag = 0;
            int status = m_db->put(nullptr, &key, &val, flag);
            // TODO handle status conversion
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        return Status::OK;
    }

    virtual Status get(bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;

        if(!packed) {

            for(size_t i = 0; i < ksizes.size; i++) {
                Dbt key{ keys.data + key_offset, (u_int32_t)ksizes[i] };
                Dbt val{ vals.data + val_offset, (u_int32_t)vsizes[i] };
                key.set_flags(DB_DBT_USERMEM);
                key.set_ulen(ksizes[i]);
                val.set_flags(DB_DBT_USERMEM);
                val.set_ulen(vsizes[i]);
                int status = m_db->get(nullptr, &key, &val, 0);
                const auto original_vsize = vsizes[i];
                if(status == 0) {
                    vsizes[i] = val.get_size();
                } else if(status == DB_NOTFOUND) {
                    vsizes[i] = KeyNotFound;
                } else if(status == DB_BUFFER_SMALL) {
                    vsizes[i] = BufTooSmall;
                } else {
                    // TODO handle other status
                }
                key_offset += ksizes[i];
                val_offset += original_vsize;
            }

        } else { // if packed

            size_t val_remaining_size = vals.size;

            for(size_t i = 0; i < ksizes.size; i++) {
                Dbt key{ keys.data + key_offset, (u_int32_t)ksizes[i] };
                Dbt val{ vals.data + val_offset, (u_int32_t)val_remaining_size };
                key.set_flags(DB_DBT_USERMEM);
                key.set_ulen(ksizes[i]);
                val.set_flags(DB_DBT_USERMEM);
                val.set_ulen(val_remaining_size);
                int status = m_db->get(nullptr, &key, &val, 0);
                if(status == 0) {
                    vsizes[i] = val.get_size();
                    val_remaining_size -= vsizes[i];
                    val_offset += vsizes[i];
                } else if(status == DB_NOTFOUND) {
                    vsizes[i] = KeyNotFound;
                } else if(status == DB_BUFFER_SMALL) {
                    for(; i < ksizes.size; i++) {
                        vsizes[i] = BufTooSmall;
                    }
                    break;
                } else {
                    // TODO handle other status conversion
                }
                key_offset += ksizes[i];
            }
            vals.size = vals.size - val_remaining_size;
        }
        return Status::OK;
    }

    virtual Status erase(const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
#if 0
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
#endif
        return Status::NotSupported;
    }

    virtual Status listKeys(bool packed, const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
#if 0
        ScopedReadLock lock(m_lock);

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
#endif
        return Status::NotSupported;
    }

    ~BerkeleyDBKeyValueStore() {
        if(m_db) {
            m_db->close(0);
            delete m_db;
        }
        if(m_db_env) {
            m_db_env->close(0);
            delete m_db_env;
        }
    }

    private:

    json   m_config;
    DbEnv* m_db_env;
    Db*    m_db;

    BerkeleyDBKeyValueStore(json cfg, DbEnv* env, Db* db)
    : m_config(std::move(cfg))
    , m_db_env(env)
    , m_db(db) {}
};

}

RKV_REGISTER_BACKEND(berkeleydb, rkv::BerkeleyDBKeyValueStore);

/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include "../common/locks.hpp"
#include <unqlite.h>
#include <nlohmann/json.hpp>
#include <abt.h>
#include <string>
#include <cstring>
#include <experimental/filesystem>
#include <iostream>

namespace rkv {

using json = nlohmann::json;

class UnQLiteKeyValueStore : public KeyValueStoreInterface {

    public:

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;
        bool use_lock;
        std::string path;

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            // check use_lock
            use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;
            path = cfg.value("path", "");
        } catch(...) {
            return Status::InvalidConf;
        }
        if(path.empty())
            return Status::InvalidConf;

        unqlite* db = nullptr;
        int ret = unqlite_open(&db, path.c_str(), UNQLITE_OPEN_CREATE);
        if(ret != UNQLITE_OK)
            return Status::Other; // TODO
        *kvs = new UnQLiteKeyValueStore(std::move(cfg), use_lock, db);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "unqlite";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual void destroy() override {
        ScopedWriteLock lock(m_lock);
        unqlite_close(m_db);
        m_db = nullptr;
        auto path = m_config["path"].get<std::string>();
        std::experimental::filesystem::remove(path);
    }

    virtual Status exists(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
#if 0
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const datum key{ keys.data + offset, (int)ksizes[i] };
            flags[i] = gdbm_exists(m_db, key);
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
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const datum key{ keys.data + offset, (int)ksizes[i] };
            datum val = gdbm_fetch(m_db, key);
            if(val.dptr == nullptr)
                vsizes[i] = KeyNotFound;
            else {
                vsizes[i] = val.dsize;
                free(val.dptr);
            }
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
            const auto key = datum { keys.data + key_offset, (int)ksizes[i] };
            const auto val = datum { vals.data + val_offset, (int)vsizes[i] };
            int ret = gdbm_store(m_db, key, val, UnQLite_REPLACE);
            if(ret != 0) // TODO convert status ?
                return Status::Other;
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
                const datum key{ keys.data + key_offset, (int)ksizes[i] };
                datum val = gdbm_fetch(m_db, key);
                const auto original_vsize = vsizes[i];
                if(val.dptr == nullptr) {
                    vsizes[i] = KeyNotFound;
                } else {
                    if(val.dsize > (int)vsizes[i]) {
                        vsizes[i] = BufTooSmall;
                    } else {
                        std::memcpy(vals.data + val_offset, val.dptr, val.dsize);
                        vsizes[i] = val.dsize;
                    }
                    free(val.dptr);
                }
                key_offset += ksizes[i];
                val_offset += original_vsize;
            }

        } else { // if packed

            size_t val_remaining_size = vals.size;

            for(size_t i = 0; i < ksizes.size; i++) {
                const auto key = datum{ keys.data + key_offset, (int)ksizes[i] };
                datum val = gdbm_fetch(m_db, key);
                if(val.dptr == nullptr) {
                    vsizes[i] = KeyNotFound;
                } else if(val.dsize > (int)val_remaining_size) {
                    for(; i < ksizes.size; i++) {
                        vsizes[i] = BufTooSmall;
                    }
                    free(val.dptr);
                } else {
                    std::memcpy(vals.data + val_offset, val.dptr, val.dsize);
                    vsizes[i] = val.dsize;
                    val_remaining_size -= vsizes[i];
                    val_offset += vsizes[i];
                    free(val.dptr);
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
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const auto key = datum{ keys.data + offset, (int)ksizes[i] };
            gdbm_delete(m_db, key);
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
        (void)packed;
        (void)fromKey;
        (void)inclusive;
        (void)prefix;
        (void)keys;
        (void)keySizes;
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
        (void)packed;
        (void)fromKey;
        (void)inclusive;
        (void)prefix;
        (void)keys;
        (void)keySizes;
        (void)vals;
        (void)valSizes;
#endif
        return Status::NotSupported;
    }

    ~UnQLiteKeyValueStore() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
        if(m_db)
            unqlite_close(m_db);
    }

    private:

    UnQLiteKeyValueStore(json cfg, bool use_lock, unqlite* db)
    : m_config(std::move(cfg))
    , m_db(db)
    {
        if(use_lock)
            ABT_rwlock_create(&m_lock);
    }

    unqlite*   m_db;
    json       m_config;
    ABT_rwlock m_lock = ABT_RWLOCK_NULL;
};

}

RKV_REGISTER_BACKEND(unqlite, rkv::UnQLiteKeyValueStore);

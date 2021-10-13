/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "../common/locks.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <string>
#include <cstring>
#include <iostream>
#include <gdbm.h>
#if __cplusplus >= 201703L
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

namespace yokan {

using json = nlohmann::json;

#if __cplusplus >= 201703L
namespace fs = std::filesystem;
#else
namespace fs = std::experimental::filesystem;
#endif

class GDBMKeyValueStore : public KeyValueStoreInterface {

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

        auto db = gdbm_open(path.c_str(), 0, GDBM_WRCREAT, 0600, 0);
        *kvs = new GDBMKeyValueStore(std::move(cfg), use_lock, db);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "gdbm";
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
        //            |YOKAN_MODE_APPEND
                    |YOKAN_MODE_CONSUME
        //            |YOKAN_MODE_WAIT
        //            |YOKAN_MODE_NOTIFY
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
        gdbm_close(m_db);
        m_db = nullptr;
        auto path = m_config["path"].get<std::string>();
        fs::remove(path);
    }

    virtual Status count(int32_t mode, uint64_t* c) const override {
        (void)mode;
        (void)c;
        return Status::NotSupported;
    }

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
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
    }

    virtual Status length(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
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

        auto mode_new_only = mode & YOKAN_MODE_NEW_ONLY;
        auto mode_exist_only = mode & YOKAN_MODE_EXIST_ONLY;

        ScopedWriteLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            const auto key = datum { keys.data + key_offset, (int)ksizes[i] };
            const auto val = datum { vals.data + val_offset, (int)vsizes[i] };
            int ret = 0;
            if(mode_exist_only) {
                if(gdbm_exists(m_db, key))
                    ret = gdbm_store(m_db, key, val, GDBM_REPLACE);
            } else {
                auto flag = mode_new_only ? GDBM_INSERT : GDBM_REPLACE;
                ret = gdbm_store(m_db, key, val, flag);
            }
            if(ret != 0 && gdbm_errno != GDBM_CANNOT_REPLACE) { // TODO convert status ?
                std::cerr << "errno = " << gdbm_errno << std::endl;
                return Status::Other;
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
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const auto key = datum{ keys.data + offset, (int)ksizes[i] };
            gdbm_delete(m_db, key);
            offset += ksizes[i];
        }
        return Status::OK;
    }

    ~GDBMKeyValueStore() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
        if(m_db) gdbm_close(m_db);
    }

    private:

    GDBMKeyValueStore(json cfg, bool use_lock, GDBM_FILE db)
    : m_config(std::move(cfg))
    , m_db(db)
    {
        if(use_lock)
            ABT_rwlock_create(&m_lock);
    }

    json       m_config;
    GDBM_FILE  m_db;
    ABT_rwlock m_lock = ABT_RWLOCK_NULL;
};

}

YOKAN_REGISTER_BACKEND(gdbm, yokan::GDBMKeyValueStore);

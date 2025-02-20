/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/util/locks.hpp"
#include "yokan/doc-mixin.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <string>
#include <cstring>
#include <iostream>
#include <gdbm.h>
#ifdef YOKAN_USE_STD_FILESYSTEM
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

namespace yokan {

using json = nlohmann::json;

#ifdef YOKAN_USE_STD_FILESYSTEM
namespace fs = std::filesystem;
#else
namespace fs = std::experimental::filesystem;
#endif

class GDBMDatabase : public DocumentStoreMixin<DatabaseInterface> {

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
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
        *kvs = new GDBMDatabase(std::move(cfg), path, use_lock, db);
        return Status::OK;
    }

    static Status recover(
            const std::string& config,
            const std::string& migrationConfig,
            const std::string& root,
            const std::list<std::string>& files,
            DatabaseInterface** kvs) {
        json cfg;
        bool use_lock;
        std::string path;

        (void)migrationConfig;
        if(files.empty()) return Status::IOError;

        path = root + "/" + files.front();

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            // check use_lock
            use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;
            cfg["path"] = path;
        } catch(...) {
            return Status::InvalidConf;
        }
        if(path.empty())
            return Status::InvalidConf;

        auto db = gdbm_open(path.c_str(), 0, GDBM_WRCREAT, 0600, 0);
        *kvs = new GDBMDatabase(std::move(cfg), path, use_lock, db);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string type() const override {
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
                    |YOKAN_MODE_LUA_FILTER   // not actually used
#endif
                    |YOKAN_MODE_IGNORE_DOCS  // not actually used
                    |YOKAN_MODE_FILTER_VALUE // not actually used
                    |YOKAN_MODE_LIB_FILTER   // not actually used
                    |YOKAN_MODE_NO_RDMA
                    |YOKAN_MODE_UPDATE_NEW
                    )
            );
    }

    bool isSorted() const override {
        return false;
    }

    virtual void destroy() override {
        ScopedWriteLock lock(m_lock);
        if(m_migrated) return;
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
        if(m_migrated) return Status::Migrated;
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
        if(m_migrated) return Status::Migrated;
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
                                              (size_t)0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        size_t total_vsizes = std::accumulate(vsizes.data,
                                              vsizes.data + vsizes.size,
                                              (size_t)0);
        if(total_vsizes > vals.size) return Status::InvalidArg;

        auto mode_new_only = mode & YOKAN_MODE_NEW_ONLY;
        auto mode_exist_only = mode & YOKAN_MODE_EXIST_ONLY;

        ScopedWriteLock lock(m_lock);
        if(m_migrated) return Status::Migrated;
        for(size_t i = 0; i < ksizes.size; i++) {
            const auto key = datum { keys.data + key_offset, (int)ksizes[i] };
            const auto val = datum { vals.data + val_offset, (int)vsizes[i] };
            int ret = 0;
            if(mode_exist_only) {
                if(gdbm_exists(m_db, key))
                    ret = gdbm_store(m_db, key, val, GDBM_REPLACE);
                else if(ksizes.size == 1) {
                    return Status::NotFound;
                }
            } else {
                auto flag = mode_new_only ? GDBM_INSERT : GDBM_REPLACE;
                ret = gdbm_store(m_db, key, val, flag);
                if(ret == 1 && mode_new_only && ksizes.size == 1)
                    return Status::KeyExists;
            }
            if(ret != 0 && gdbm_errno != GDBM_CANNOT_REPLACE) { // TODO convert status ?
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
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;
        ScopedReadLock lock(m_lock);
        if(m_migrated) return Status::Migrated;

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

    Status fetch(int32_t mode, const UserMem& keys,
                 const BasicUserMem<size_t>& ksizes,
                 const FetchCallback& func) override {
        size_t key_offset = 0;
        ScopedReadLock lock(m_lock);
        if(m_migrated) return Status::Migrated;

        for(size_t i = 0; i < ksizes.size; i++) {
            const datum key{ keys.data + key_offset, (int)ksizes[i] };
            datum val = gdbm_fetch(m_db, key);
            auto key_umem = UserMem{key.dptr, (size_t)key.dsize};
            auto val_umem = UserMem{val.dptr, (size_t)val.dsize};
            if(val.dptr == nullptr)
                val_umem.size = KeyNotFound;
            auto status = func(key_umem, val_umem);
            free(val.dptr);
            if(status != Status::OK) return status;
            key_offset += ksizes[i];
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
        if(m_migrated) return Status::Migrated;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const auto key = datum{ keys.data + offset, (int)ksizes[i] };
            gdbm_delete(m_db, key);
            offset += ksizes[i];
        }
        return Status::OK;
    }

    struct GDBMMigrationHandle : public MigrationHandle {

        GDBMDatabase&   m_db;
        bool            m_cancel = false;
        ScopedWriteLock m_lock;

        GDBMMigrationHandle(GDBMDatabase& db)
        : m_db(db)
        , m_lock(db.m_lock) {
            gdbm_close(m_db.m_db);
        }

        ~GDBMMigrationHandle() {
            if(m_cancel) {
                m_db.m_db = gdbm_open(m_db.m_path.c_str(), 0, GDBM_WRCREAT, 0600, 0);
            } else {
                fs::remove(m_db.m_path);
                m_db.m_migrated = true;
                m_db.m_db = nullptr;
            }
        }

        std::string getRoot() const override {
            auto i = m_db.m_path.find_last_of('/');
            return m_db.m_path.substr(0, i+1);
        }

        std::list<std::string> getFiles() const override {
            auto i = m_db.m_path.find_last_of('/');
            return {m_db.m_path.substr(i+1)};
        }

        void cancel() override {
            m_cancel = true;
        }
    };

    Status startMigration(std::unique_ptr<MigrationHandle>& mh) override {
        if(m_migrated) return Status::Migrated;
        try {
            mh.reset(new GDBMMigrationHandle(*this));
        } catch(...) {
            return Status::IOError;
        }
        return Status::OK;
    }

    ~GDBMDatabase() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
        if(m_db) gdbm_close(m_db);
    }

    private:

    GDBMDatabase(json cfg, const std::string& path, bool use_lock, GDBM_FILE db)
    : m_config(std::move(cfg))
    , m_db(db)
    , m_path(path)
    {
        if(use_lock)
            ABT_rwlock_create(&m_lock);
        auto disable_doc_mixin_lock = m_config.value("disable_doc_mixin_lock", false);
        if(disable_doc_mixin_lock) disableDocMixinLock();
    }

    json        m_config;
    GDBM_FILE   m_db;
    std::string m_path;
    ABT_rwlock  m_lock = ABT_RWLOCK_NULL;
    bool        m_migrated = false;
};

}

YOKAN_REGISTER_BACKEND(gdbm, yokan::GDBMDatabase);

/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/doc-mixin.hpp"
#include "../common/modes.hpp"
#include "util/key-copy.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <lmdb.h>
#include <string>
#include <cstring>
#include <iostream>
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

class LMDBDatabase : public DocumentStoreMixin<DatabaseInterface> {

    public:

    static inline Status convertStatus(int s) {
        switch(s) {
            case MDB_SUCCESS:
                return Status::OK;
            case MDB_KEYEXIST:
                return Status::KeyExists;
            case MDB_NOTFOUND:
                return Status::NotFound;
            case MDB_CORRUPTED:
                return Status::Corruption;
            case MDB_INVALID:
                return Status::InvalidArg;
            case MDB_PAGE_NOTFOUND:
            case MDB_PANIC:
            case MDB_VERSION_MISMATCH:
            case MDB_MAP_FULL:
            case MDB_DBS_FULL:
            case MDB_READERS_FULL:
            case MDB_TLS_FULL:
            case MDB_TXN_FULL:
            case MDB_CURSOR_FULL:
            case MDB_PAGE_FULL:
            case MDB_MAP_RESIZED:
            case MDB_INCOMPATIBLE:
            case MDB_BAD_RSLOT:
            case MDB_BAD_TXN:
            case MDB_BAD_VALSIZE:
            case MDB_BAD_DBI:
            default:
                return Status::Other;
        };
        return Status::Other;
    }

    static Status processConfig(const std::string& config, json& cfg) {
        try {
            cfg = json::parse(config);
        } catch(...) {
            return Status::InvalidConf;
        }

#define CHECK_AND_ADD_MISSING(__json__, __field__, __type__, __default__, __required__) \
        do {                                                                            \
            if(!__json__.contains(__field__)) {                                         \
                if(__required__) {                                                      \
                    return Status::InvalidConf;                                         \
                } else {                                                                \
                    __json__[__field__] = __default__;                                  \
                }                                                                       \
            } else if(!__json__[__field__].is_##__type__()) {                           \
                return Status::InvalidConf;                                             \
            }                                                                           \
        } while(0)

        CHECK_AND_ADD_MISSING(cfg, "path", string, "", true);
        CHECK_AND_ADD_MISSING(cfg, "create_if_missing", boolean, true, false);
        CHECK_AND_ADD_MISSING(cfg, "no_lock", boolean, false, false);

        return Status::OK;
    }

    static Status create(const std::string& name, const std::string& config, DatabaseInterface** kvs) {
        json cfg;
        if(processConfig(config, cfg) != Status::OK)
            return Status::InvalidConf;

        auto path = cfg["path"].get<std::string>();
        std::error_code ec;
        fs::create_directories(path, ec);
        int ret;
        MDB_env* env = nullptr;
        MDB_txn *txn = nullptr;
        MDB_dbi db;

        ret = mdb_env_create(&env);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        int flags = MDB_WRITEMAP;
        if(cfg["no_lock"].get<bool>()) flags |= MDB_NOLOCK;
        ret = mdb_env_open(env, path.c_str(), flags, 0644);
        if(ret != MDB_SUCCESS) {
            mdb_env_close(env);
            return convertStatus(ret);
        }
        ret = mdb_txn_begin(env, nullptr, 0, &txn);
        if(ret != MDB_SUCCESS) {
            mdb_env_close(env);
            return convertStatus(ret);
        }
        flags = cfg["create_if_missing"].get<bool>() ? MDB_CREATE : 0;
        ret = mdb_dbi_open(txn, nullptr, flags, &db);
        if(ret != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            mdb_env_close(env);
            return convertStatus(ret);
        }
        ret = mdb_txn_commit(txn);
        if(ret != MDB_SUCCESS) {
            mdb_env_close(env);
            return convertStatus(ret);
        }

        *kvs = new LMDBDatabase(std::move(cfg), env, db, name);

        return Status::OK;
    }

    static Status recover(
            const std::string& name,
            const std::string& config,
            const std::string& migrationConfig,
            const std::list<std::string>& files,
            DatabaseInterface** kvs) {

        (void)migrationConfig;
        json cfg;
        if(processConfig(config, cfg) != Status::OK)
            return Status::InvalidConf;

        if(files.empty()) return Status::IOError;
        std::string path = files.front();
        path = path.substr(0, path.find_last_of('/'));
        cfg["path"] = path;

        std::error_code ec;
        fs::create_directories(path, ec);
        int ret;
        MDB_env* env = nullptr;
        MDB_txn *txn = nullptr;
        MDB_dbi db;

        ret = mdb_env_create(&env);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        int flags = MDB_WRITEMAP;
        if(cfg["no_lock"].get<bool>()) flags |= MDB_NOLOCK;
        ret = mdb_env_open(env, path.c_str(), flags, 0644);
        if(ret != MDB_SUCCESS) {
            mdb_env_close(env);
            return convertStatus(ret);
        }
        ret = mdb_txn_begin(env, nullptr, 0, &txn);
        if(ret != MDB_SUCCESS) {
            mdb_env_close(env);
            return convertStatus(ret);
        }
        ret = mdb_dbi_open(txn, nullptr, 0, &db);
        if(ret != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            mdb_env_close(env);
            return convertStatus(ret);
        }
        ret = mdb_txn_commit(txn);
        if(ret != MDB_SUCCESS) {
            mdb_env_close(env);
            return convertStatus(ret);
        }

        *kvs = new LMDBDatabase(std::move(cfg), env, db, name);

        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string type() const override {
        return "lmdb";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return m_name;
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
        //            |YOKAN_MODE_NEW_ONLY
        //            |YOKAN_MODE_EXIST_ONLY
                    |YOKAN_MODE_NO_PREFIX
                    |YOKAN_MODE_IGNORE_KEYS
                    |YOKAN_MODE_KEEP_LAST
                    |YOKAN_MODE_SUFFIX
#ifdef YOKAN_HAS_LUA
                    |YOKAN_MODE_LUA_FILTER
#endif
                    |YOKAN_MODE_IGNORE_DOCS
                    |YOKAN_MODE_FILTER_VALUE
                    |YOKAN_MODE_LIB_FILTER
                    |YOKAN_MODE_NO_RDMA
                    )
            );
    }

    bool isSorted() const override {
        return true;
    }

    virtual void destroy() override {
        if(m_env) {
            mdb_dbi_close(m_env, m_db);
            mdb_env_close(m_env);
        }
        m_env = nullptr;
        auto path = m_config["path"].get<std::string>();
        fs::remove_all(path);
    }

    virtual Status count(int32_t mode, uint64_t* c) const override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
        (void)mode;
        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        MDB_stat stats;
        ret = mdb_stat(txn, m_db, &stats);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        *c = stats.ms_entries;
        return Status::OK;
    }

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        auto count = ksizes.size;
        size_t offset = 0;
        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        for(size_t i = 0; i < count; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            MDB_val key{ ksizes[i], keys.data + offset };
            MDB_val val { 0, nullptr };
            ret = mdb_get(txn, m_db, &key, &val);
            if(ret == MDB_NOTFOUND) flags[i] = false;
            else if(ret == MDB_SUCCESS) flags[i] = true;
            else {
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
            offset += ksizes[i];
        }
        mdb_txn_abort(txn);
        return Status::OK;
    }

    virtual Status length(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
        (void)mode;
        if(ksizes.size > vsizes.size) return Status::InvalidArg;
        auto count = ksizes.size;
        size_t offset = 0;
        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        for(size_t i = 0; i < count; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            MDB_val key{ ksizes[i], keys.data + offset };
            MDB_val val { 0, nullptr };
            ret = mdb_get(txn, m_db, &key, &val);
            if(ret == MDB_NOTFOUND) {
                vsizes[i] = KeyNotFound;
            } else if(ret == MDB_SUCCESS) {
                vsizes[i] = val.mv_size;
            } else {
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
            offset += ksizes[i];
        }
        mdb_txn_abort(txn);
        return Status::OK;
    }

    virtual Status put(int32_t mode, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
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

        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, 0, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        for(size_t i = 0; i < ksizes.size; i++) {
            MDB_val key{ ksizes[i], keys.data + key_offset};
            MDB_val val{ vsizes[i], vals.data + val_offset };
            ret = mdb_put(txn, m_db, &key, &val, 0);
            key_offset += ksizes[i];
            val_offset += vsizes[i];
            if(ret != 0) {
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
        }
        ret = mdb_txn_commit(txn);
        return convertStatus(ret);
    }

    Status get(int32_t mode, bool packed, const UserMem& keys,
               const BasicUserMem<size_t>& ksizes,
               UserMem& vals,
               BasicUserMem<size_t>& vsizes) override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;

        if(!packed) {

            MDB_txn* txn = nullptr;
            int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
            if(ret != MDB_SUCCESS) return convertStatus(ret);
            for(size_t i = 0; i < ksizes.size; i++) {
                MDB_val key{ ksizes[i], keys.data + key_offset };
                MDB_val val{ 0, nullptr };
                ret = mdb_get(txn, m_db, &key, &val);
                size_t original_vsize = vsizes[i];
                if(ret == MDB_NOTFOUND) {
                    vsizes[i] = KeyNotFound;
                } else if(ret == MDB_SUCCESS) {
                    if(val.mv_size > vsizes[i]) {
                        vsizes[i] = BufTooSmall;
                    }else {
                        vsizes[i] = val.mv_size;
                        std::memcpy(vals.data + val_offset, val.mv_data, val.mv_size);
                    }
                } else {
                    mdb_txn_abort(txn);
                    return convertStatus(ret);
                }
                key_offset += ksizes[i];
                val_offset += original_vsize;
            }
            mdb_txn_abort(txn);

        } else { // if packed

            size_t val_remaining_size = vals.size;
            MDB_txn* txn = nullptr;
            int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
            if(ret != MDB_SUCCESS) return convertStatus(ret);
            for(size_t i = 0; i < ksizes.size; i++) {
                MDB_val key{ ksizes[i], keys.data + key_offset };
                MDB_val val{ 0, nullptr };
                ret = mdb_get(txn, m_db, &key, &val);
                if(ret == MDB_NOTFOUND) {
                    vsizes[i] = KeyNotFound;
                } else if(ret == MDB_SUCCESS) {
                    if(val.mv_size > val_remaining_size) {
                        for(; i < ksizes.size; i++) {
                            vsizes[i] = BufTooSmall;
                        }
                    }else {
                        vsizes[i] = val.mv_size;
                        std::memcpy(vals.data + val_offset, val.mv_data, val.mv_size);
                        val_remaining_size -= vsizes[i];
                        val_offset += vsizes[i];
                    }
                } else {
                    mdb_txn_abort(txn);
                    return convertStatus(ret);
                }
                key_offset += ksizes[i];
            }
            mdb_txn_abort(txn);
            vals.size = vals.size - val_remaining_size;
        }
        if(mode & YOKAN_MODE_CONSUME) {
            return erase(mode, keys, ksizes);
        }
        return Status::OK;
    }

    Status fetch(int32_t mode, const UserMem& keys,
                 const BasicUserMem<size_t>& ksizes,
                 const FetchCallback& func) override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;

        size_t key_offset = 0;

        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        for(size_t i = 0; i < ksizes.size; i++) {
            MDB_val key{ ksizes[i], keys.data + key_offset };
            MDB_val val{ 0, nullptr };
            ret = mdb_get(txn, m_db, &key, &val);
            auto key_umem = UserMem{(char*)key.mv_data, key.mv_size};
            auto val_umem = UserMem{(char*)val.mv_data, val.mv_size};
            if(ret == MDB_NOTFOUND) {
                val_umem.size = KeyNotFound;
            } else if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
            auto status = func(key_umem, val_umem);
            if(status != Status::OK) {
                mdb_txn_abort(txn);
                return status;
            }
            key_offset += ksizes[i];
        }
        mdb_txn_abort(txn);

        if(mode & YOKAN_MODE_CONSUME) {
            return erase(mode, keys, ksizes);
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
        (void)mode;
        size_t key_offset = 0;
        size_t total_ksizes = std::accumulate(ksizes.data,
                                              ksizes.data + ksizes.size,
                                              (size_t)0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, 0, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);
        for(size_t i = 0; i < ksizes.size; i++) {
            MDB_val key{ ksizes[i], keys.data + key_offset};
            MDB_val val{ 0, nullptr };
            ret = mdb_del(txn, m_db, &key, &val);
            key_offset += ksizes[i];
            if(ret != MDB_SUCCESS && ret != MDB_NOTFOUND) {
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
        }
        ret = mdb_txn_commit(txn);
        return convertStatus(ret);
    }

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const std::shared_ptr<KeyValueFilter>& filter,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;

        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);

        MDB_cursor* cursor = nullptr;
        ret = mdb_cursor_open(txn, m_db, &cursor);
        if(ret != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            return convertStatus(ret);
        }

        auto max = keySizes.size;

        if(fromKey.size == 0) {
            MDB_val k, v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                auto status = convertStatus(ret);
                if(status == Status::NotFound) {
                    keys.size = 0;
                    for(unsigned i=0; i < max; i++) {
                        keySizes[i] = YOKAN_NO_MORE_KEYS;
                    }
                    return Status::OK;
                }
                return status;
            }
        } else {
            MDB_val k{ fromKey.size, fromKey.data };
            MDB_val v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                auto status = convertStatus(ret);
                if(status == Status::NotFound) {
                    keys.size = 0;
                    for(unsigned i=0; i < max; i++) {
                        keySizes[i] = YOKAN_NO_MORE_KEYS;
                    }
                    return Status::OK;
                }
                return status;
            }
            if(!inclusive) {
                if(k.mv_size == fromKey.size
                && std::memcmp(k.mv_data, fromKey.data, fromKey.size) == 0) {
                    ret = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
                    if(ret != MDB_SUCCESS) {
                        mdb_txn_abort(txn);
                        return convertStatus(ret);
                    }
                }
            }
        }

        size_t i = 0;
        size_t key_offset = 0;

        while(i < max) {
            MDB_val key, val;
            ret = mdb_cursor_get(cursor, &key, &val, MDB_GET_CURRENT);
            if(ret == MDB_NOTFOUND) {
                break;
            }
            if(ret != MDB_SUCCESS) {
                mdb_cursor_close(cursor);
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }

            if(!filter->check(key.mv_data, key.mv_size, val.mv_data, val.mv_size)) {
                if(filter->shouldStop(key.mv_data, key.mv_size, val.mv_data, val.mv_size))
                    break;
                ret = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
                if(ret == MDB_NOTFOUND)
                    break;
                if(ret != MDB_SUCCESS) {
                    mdb_cursor_close(cursor);
                    mdb_txn_abort(txn);
                    return convertStatus(ret);
                }
                continue;
            }

            size_t key_usize = keySizes[i];
            auto key_umem = static_cast<char*>(keys.data) + key_offset;
            if(packed) {
                auto dst_max_size = keys.size - key_offset;
                keySizes[i] = keyCopy(mode, i == max-1, filter,
                    key_umem, dst_max_size, key.mv_data, key.mv_size);
                if(keySizes[i] == YOKAN_SIZE_TOO_SMALL) {
                    while(i < max) {
                        keySizes[i] = YOKAN_SIZE_TOO_SMALL;
                        i += 1;
                    }
                    break;
                } else {
                    key_offset += keySizes[i];
                }
            } else {
                keySizes[i] = keyCopy(mode, i == max-1, filter,
                                      key_umem, key_usize, key.mv_data, key.mv_size);
                key_offset += key_usize;
            }
            i += 1;
            ret = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
            if(ret == MDB_NOTFOUND)
                break;
            if(ret != MDB_SUCCESS) {
                mdb_cursor_close(cursor);
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);

        keys.size = key_offset;
        for(; i < max; i++) {
            keySizes[i] = YOKAN_NO_MORE_KEYS;
        }
        return Status::OK;
    }

    virtual Status listKeyValues(int32_t mode,
                                 bool packed,
                                 const UserMem& fromKey,
                                 const std::shared_ptr<KeyValueFilter>& filter,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        ScopedReadLock mlock(m_migration_lock);
        if(m_migrated) return Status::Migrated;
        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;

        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);

        MDB_cursor* cursor = nullptr;
        ret = mdb_cursor_open(txn, m_db, &cursor);
        if(ret != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            return convertStatus(ret);
        }
        auto max = keySizes.size;

        if(fromKey.size == 0) {
            MDB_val k, v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                auto status = convertStatus(ret);
                if(status == Status::NotFound) {
                    keys.size = 0;
                    vals.size = 0;
                    for(unsigned i=0; i < max; i++) {
                        keySizes[i] = YOKAN_NO_MORE_KEYS;
                        valSizes[i] = YOKAN_NO_MORE_KEYS;
                    }
                    return Status::OK;
                }
                return status;
            }
        } else {
            MDB_val k{ fromKey.size, fromKey.data };
            MDB_val v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                auto status = convertStatus(ret);
                if(status == Status::NotFound) {
                    keys.size = 0;
                    vals.size = 0;
                    for(unsigned i=0; i < max; i++) {
                        keySizes[i] = YOKAN_NO_MORE_KEYS;
                        valSizes[i] = YOKAN_NO_MORE_KEYS;
                    }
                    return Status::OK;
                }
                return status;
            }
            if(!inclusive) {
                if(k.mv_size == fromKey.size
                && std::memcmp(k.mv_data, fromKey.data, fromKey.size) == 0) {
                    ret = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
                    if(ret != MDB_SUCCESS) {
                        mdb_txn_abort(txn);
                        return convertStatus(ret);
                    }
                }
            }
        }

        size_t i = 0;
        size_t key_offset = 0;
        size_t val_offset = 0;
        bool key_buf_too_small = false;
        bool val_buf_too_small = false;

        while(i < max) {
            MDB_val key, val;
            ret = mdb_cursor_get(cursor, &key, &val, MDB_GET_CURRENT);
            if(ret == MDB_NOTFOUND)
                break;
            if(ret != MDB_SUCCESS) {
                mdb_cursor_close(cursor);
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }

            if(!filter->check(key.mv_data, key.mv_size, val.mv_data, val.mv_size)) {
                if(filter->shouldStop(key.mv_data, key.mv_size, val.mv_data, val.mv_size))
                    break;
                ret = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
                if(ret == MDB_NOTFOUND)
                    break;
                if(ret != MDB_SUCCESS) {
                    mdb_cursor_close(cursor);
                    mdb_txn_abort(txn);
                    return convertStatus(ret);
                }
                continue;
            }

            size_t key_usize = keySizes[i];
            size_t val_usize = valSizes[i];
            auto key_umem = static_cast<char*>(keys.data) + key_offset;
            auto val_umem = static_cast<char*>(vals.data) + val_offset;
            if(packed) {
                if(key_buf_too_small) {
                    keySizes[i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    keySizes[i] = keyCopy(mode, i == max-1, filter,
                                          key_umem, keys.size - key_offset,
                                          key.mv_data, key.mv_size);
                    if(keySizes[i] == YOKAN_SIZE_TOO_SMALL) {
                        key_buf_too_small = true;
                    } else {
                        key_offset += keySizes[i];
                    }
                }
                if(val_buf_too_small) {
                    valSizes[i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    valSizes[i] = filter->valCopy(val_umem, vals.size - val_offset,
                                                  val.mv_data, val.mv_size);
                    if(valSizes[i] == YOKAN_SIZE_TOO_SMALL) {
                        val_buf_too_small = true;
                    } else {
                        val_offset += valSizes[i];
                    }
                }
                if(val_buf_too_small && key_buf_too_small) {
                    while(i < max) {
                        keySizes[i] = YOKAN_SIZE_TOO_SMALL;
                        valSizes[i] = YOKAN_SIZE_TOO_SMALL;
                        i += 1;
                    }
                    break;
                }
            } else {
                keySizes[i] = keyCopy(mode, i == max-1, filter,
                                      key_umem, key_usize,
                                      key.mv_data, key.mv_size);
                valSizes[i] = filter->valCopy(val_umem, val_usize,
                                              val.mv_data, val.mv_size);
                key_offset += key_usize;
                val_offset += val_usize;
            }
            i += 1;
            ret = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
            if(ret == MDB_NOTFOUND)
                break;
            if(ret != MDB_SUCCESS) {
                mdb_cursor_close(cursor);
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);

        keys.size = key_offset;
        vals.size = val_offset;
        for(; i < max; i++) {
            keySizes[i] = YOKAN_NO_MORE_KEYS;
            valSizes[i] = YOKAN_NO_MORE_KEYS;
        }
        return Status::OK;
    }

    struct LMDBMigrationHandle : public MigrationHandle {

        LMDBDatabase&   m_db;
        bool            m_cancel = false;
        ScopedWriteLock m_lock;
        std::string     m_path;

        LMDBMigrationHandle(LMDBDatabase& db)
        : m_db(db)
        , m_lock(db.m_migration_lock) {
            m_path = m_db.m_config["path"];
        }

        ~LMDBMigrationHandle() {
            if(m_cancel) return;
            m_db.destroy();
            m_db.m_migrated = true;
        }

        std::string getRoot() const override {
            return m_path;
        }

        std::list<std::string> getFiles() const override {
            return {"/"};
        }

        void cancel() override {
            m_cancel = true;
        }
    };

    Status startMigration(std::unique_ptr<MigrationHandle>& mh) override {
        if(m_migrated) return Status::Migrated;
        try {
            mh.reset(new LMDBMigrationHandle(*this));
        } catch(...) {
            return Status::IOError;
        }
        return Status::OK;
    }

    ~LMDBDatabase() {
        if(m_env) {
            mdb_dbi_close(m_env, m_db);
            mdb_env_close(m_env);
        }
        m_env = nullptr;
        ABT_rwlock_free(&m_migration_lock);
    }

    private:

    LMDBDatabase(json&& cfg, MDB_env* env, MDB_dbi db, const std::string& name)
    : m_config(std::move(cfg))
    , m_env(env)
    , m_db(db)
    , m_name(name) {
        auto disable_doc_mixin_lock = m_config.value("disable_doc_mixin_lock", false);
        if(disable_doc_mixin_lock) disableDocMixinLock();
        ABT_rwlock_create(&m_migration_lock);
    }

    json        m_config;
    MDB_env*    m_env = nullptr;
    MDB_dbi     m_db;
    std::string m_name;

    bool        m_migrated = false;
    ABT_rwlock  m_migration_lock = ABT_RWLOCK_NULL;
};

}

YOKAN_REGISTER_BACKEND(lmdb, yokan::LMDBDatabase);

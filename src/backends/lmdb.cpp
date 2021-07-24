/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include "../common/modes.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <lmdb.h>
#include <string>
#include <cstring>
#include <iostream>
#if __cplusplus >= 201703L
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

namespace rkv {

using json = nlohmann::json;

#if __cplusplus >= 201703L
namespace fs = std::filesystem;
#else
namespace fs = std::experimental::filesystem;
#endif

class LMDBKeyValueStore : public KeyValueStoreInterface {

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

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;
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

        *kvs = new LMDBKeyValueStore(std::move(cfg), env, db);

        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "lmdb";
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
                     RKV_MODE_INCLUSIVE
        //            |RKV_MODE_APPEND
        //            |RKV_MODE_CONSUME
        //            |RKV_MODE_WAIT
        //            |RKV_MODE_NEW_ONLY
        //            |RKV_MODE_EXIST_ONLY
        //            |RKV_MODE_NO_PREFIX
        //            |RKV_MODE_IGNORE_KEYS
        //            |RKV_MODE_KEEP_LAST
                    |RKV_MODE_SUFFIX
                    )
            );
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

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
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

    virtual Status get(int32_t mode, bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) const override {
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
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t key_offset = 0;
        size_t total_ksizes = std::accumulate(ksizes.data,
                                              ksizes.data + ksizes.size,
                                              0);
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
                            const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        auto inclusive = mode & RKV_MODE_INCLUSIVE;

        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);

        MDB_cursor* cursor = nullptr;
        ret = mdb_cursor_open(txn, m_db, &cursor);
        if(ret != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            return convertStatus(ret);
        }

        if(fromKey.size == 0) {
            MDB_val k, v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
        } else {
            MDB_val k{ fromKey.size, fromKey.data };
            MDB_val v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                return convertStatus(ret);
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

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        bool key_buf_too_small = false;

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

            if((key.mv_size < prefix.size)
            || !checkPrefix(mode, key.mv_data, key.mv_size,
                           prefix.data, prefix.size)) {
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
                if(keys.size - key_offset < key.mv_size || key_buf_too_small) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_buf_too_small = true;
                } else {
                    std::memcpy(key_umem, key.mv_data, key.mv_size);
                    keySizes[i] = key.mv_size;
                    key_offset += key.mv_size;
                }
            } else {
                if(key_usize < key.mv_size) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_offset += key_usize;
                } else {
                    std::memcpy(key_umem, key.mv_data, key.mv_size);
                    keySizes[i] = key.mv_size;
                    key_offset += key_usize;
                }
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
            keySizes[i] = RKV_NO_MORE_KEYS;
        }
        return Status::OK;
    }

    virtual Status listKeyValues(int32_t mode,
                                 bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        auto inclusive = mode & RKV_MODE_INCLUSIVE;

        MDB_txn* txn = nullptr;
        int ret = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
        if(ret != MDB_SUCCESS) return convertStatus(ret);

        MDB_cursor* cursor = nullptr;
        ret = mdb_cursor_open(txn, m_db, &cursor);
        if(ret != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            return convertStatus(ret);
        }

        if(fromKey.size == 0) {
            MDB_val k, v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                return convertStatus(ret);
            }
        } else {
            MDB_val k{ fromKey.size, fromKey.data };
            MDB_val v;
            ret = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
            if(ret != MDB_SUCCESS) {
                mdb_txn_abort(txn);
                return convertStatus(ret);
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

        auto max = keySizes.size;
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

            if(key.mv_size < prefix.size
            || !checkPrefix(mode, key.mv_data, key.mv_size,
                            prefix.data, prefix.size)) {
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
                if(keys.size - key_offset < key.mv_size || key_buf_too_small) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_buf_too_small = true;
                } else {
                    std::memcpy(key_umem, key.mv_data, key.mv_size);
                    keySizes[i] = key.mv_size;
                    key_offset += key.mv_size;
                }
                if(vals.size - val_offset < val.mv_size || val_buf_too_small) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                    val_buf_too_small = true;
                } else {
                    std::memcpy(val_umem, val.mv_data, val.mv_size);
                    valSizes[i] = val.mv_size;
                    val_offset += val.mv_size;
                }
            } else {
                if(key_usize < key.mv_size) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_offset += key_usize;
                } else {
                    std::memcpy(key_umem, key.mv_data, key.mv_size);
                    keySizes[i] = key.mv_size;
                    key_offset += key_usize;
                }
                if(val_usize < val.mv_size) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                    val_offset += val_usize;
                } else {
                    std::memcpy(val_umem, val.mv_data, val.mv_size);
                    valSizes[i] = val.mv_size;
                    val_offset += val_usize;
                }
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
            keySizes[i] = RKV_NO_MORE_KEYS;
            valSizes[i] = RKV_NO_MORE_KEYS;
        }
        return Status::OK;
    }

    ~LMDBKeyValueStore() {
        if(m_env) {
            mdb_dbi_close(m_env, m_db);
            mdb_env_close(m_env);
        }
        m_env = nullptr;
    }

    private:

    LMDBKeyValueStore(json&& cfg, MDB_env* env, MDB_dbi db)
    : m_config(std::move(cfg))
    , m_env(env)
    , m_db(db) {}

    json     m_config;
    MDB_env* m_env = nullptr;
    MDB_dbi  m_db;
};

}

RKV_REGISTER_BACKEND(lmdb, rkv::LMDBKeyValueStore);

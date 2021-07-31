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

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            CHECK_AND_ADD_MISSING(cfg, "use_abt_lock", boolean, true, false);
            CHECK_AND_ADD_MISSING(cfg, "path", string, "", true);
            CHECK_AND_ADD_MISSING(cfg, "mode", string, "create", false);
            CHECK_AND_ADD_MISSING(cfg, "temporary", boolean, false, false);
            CHECK_AND_ADD_MISSING(cfg, "no_journaling", boolean, false, false);
            CHECK_AND_ADD_MISSING(cfg, "no_unqlite_mutex", boolean, false, false);
            CHECK_AND_ADD_MISSING(cfg, "max_page_cache", number, -1, false);
            CHECK_AND_ADD_MISSING(cfg, "disable_auto_commit", boolean, false, false);
            // TODO add all the parameters from unqlite_lib_config
        } catch(...) {
            return Status::InvalidConf;
        }
        auto mode_str = cfg["mode"].get<std::string>();
        int mode;
        if(mode_str == "create")          mode = UNQLITE_OPEN_CREATE;
        else if(mode_str == "read_write") mode = UNQLITE_OPEN_READWRITE;
        else if(mode_str == "read_only")  mode = UNQLITE_OPEN_READONLY;
        else if(mode_str == "mmap")       mode = UNQLITE_OPEN_MMAP;
        else if(mode_str == "memory")     mode = UNQLITE_OPEN_IN_MEMORY;
        else return Status::InvalidConf;

        auto path = cfg["path"].get<std::string>();
        if(path.empty() && mode_str != "memory")
            return Status::InvalidConf;
        if(mode_str == "memory") {
            cfg["path"] = "";
            // TODO display warning
            path = ":mem:";
        }

        auto use_lock = cfg["use_abt_lock"].get<bool>();

        if(cfg["temporary"].get<bool>())        mode |= UNQLITE_OPEN_TEMP_DB;
        if(cfg["no_journaling"].get<bool>())    mode |= UNQLITE_OPEN_OMIT_JOURNALING;
        if(cfg["no_unqlite_mutex"].get<bool>()) mode |= UNQLITE_OPEN_NOMUTEX;

        unqlite* db = nullptr;
        int ret = unqlite_open(&db, path.c_str(), mode);
        if(ret != UNQLITE_OK)
            return Status::Other; // TODO

        if(cfg["max_page_cache"].get<int>() >= 0)
            unqlite_config(db, UNQLITE_CONFIG_MAX_PAGE_CACHE, cfg["max_page_cache"].get<int>());
        if(cfg["disable_auto_commit"].get<bool>())
            unqlite_config(db, UNQLITE_CONFIG_DISABLE_AUTO_COMMIT);

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

    virtual bool supportsMode(int32_t mode) const override {
        return mode ==
            (mode & (
                     RKV_MODE_INCLUSIVE
                    |RKV_MODE_APPEND
        //            |RKV_MODE_CONSUME
        //            |RKV_MODE_WAIT
        //            |RKV_MODE_NEW_ONLY
        //            |RKV_MODE_EXIST_ONLY
        //            |RKV_MODE_NO_PREFIX
        //            |RKV_MODE_IGNORE_KEYS
        //            |RKV_MODE_KEEP_LAST
        //            |RKV_MODE_SUFFIX
                    )
            );
    }

    virtual void destroy() override {
        ScopedWriteLock lock(m_lock);
        unqlite_close(m_db);
        m_db = nullptr;
        auto path = m_config["path"].get<std::string>();
        auto temp = m_config["temporary"].get<bool>();
        if(path != "" && path != ":mem:" && !temp) {
            std::experimental::filesystem::remove(path);
        }
    }

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto key_umem = keys.data + offset;
            auto key_size = ksizes[i];
            unqlite_int64 val_size = 0;
            int ret = unqlite_kv_fetch(m_db, key_umem, key_size, nullptr, &val_size);
            if(ret == UNQLITE_OK || ret == UNQLITE_NOMEM)
                flags[i] = true;
            else if(ret == UNQLITE_NOTFOUND)
                flags[i] = false;
            else {
                return Status::Other; // TODO convert status
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto key_umem = keys.data + offset;
            auto key_size = ksizes[i];
            unqlite_int64 val_size = 0;
            int ret = unqlite_kv_fetch(m_db, key_umem, key_size, nullptr, &val_size);
            if(ret == UNQLITE_OK || ret == UNQLITE_NOMEM)
                vsizes[i] = val_size;
            else if(ret == UNQLITE_NOTFOUND)
                vsizes[i] = KeyNotFound;
            else {
                return Status::Other; // TODO convert status
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status put(int32_t mode, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;

        auto mode_append = mode & RKV_MODE_APPEND;

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
            auto key_umem = keys.data + key_offset;
            auto val_umem = vals.data + val_offset;
            int ret;
            if(mode_append) {
                ret = unqlite_kv_append(m_db, key_umem, ksizes[i],
                                              val_umem, vsizes[i]);
            } else {
                ret = unqlite_kv_store(m_db, key_umem, ksizes[i],
                                             val_umem, vsizes[i]);
            }
            if(ret != UNQLITE_OK) // TODO convert status
                return Status::Other;
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

        struct CallbackArgs {
            size_t* val_size;
            void*   val_umem;
            bool*   buf_too_small;
        };

        auto Callback = [](const void *pData,unsigned int iDataLen,void *pUserData) -> int {
                            auto args = static_cast<CallbackArgs*>(pUserData);
                            if(*(args->buf_too_small) || iDataLen > *(args->val_size))
                                *(args->val_size) = BufTooSmall;
                            else {
                                std::memcpy(args->val_umem, pData, iDataLen);
                                *(args->val_size) = iDataLen;
                            }
                            return UNQLITE_OK;
                        };

        if(!packed) {

            bool buf_too_small = false;

            for(size_t i = 0; i < ksizes.size; i++) {

                auto key_umem = keys.data + key_offset;
                auto key_size = ksizes[i];
                auto original_vsize = vsizes[i];

                CallbackArgs args{ vsizes.data + i, vals.data + val_offset, &buf_too_small };

                int ret = unqlite_kv_fetch_callback(
                        m_db, key_umem, key_size, Callback,
                        static_cast<void*>(&args));

                if(ret != UNQLITE_OK) {
                    if(ret == UNQLITE_NOTFOUND) {
                        vsizes[i] = KeyNotFound;
                    } else {
                        return Status::Other; // TODO convert status;
                    }
                }

                key_offset += ksizes[i];
                val_offset += original_vsize;
            }

        } else { // if packed

            size_t val_remaining_size = vals.size;
            bool buf_too_small = false;

            for(size_t i = 0; i < ksizes.size; i++) {

                auto key_umem = keys.data + key_offset;
                auto key_size = ksizes[i];
                vsizes[i] = val_remaining_size;

                CallbackArgs args{ vsizes.data + i, vals.data + val_offset, &buf_too_small };

                int ret = unqlite_kv_fetch_callback(
                        m_db, key_umem, key_size,
                        Callback, static_cast<void*>(&args));

                if(ret != UNQLITE_OK) {
                    if(ret == UNQLITE_NOTFOUND) {
                        vsizes[i] = KeyNotFound;
                    } else {
                        return Status::Other; // TODO convert status;
                    }
                } else {
                    if(vsizes[i] == BufTooSmall)
                        buf_too_small = true;
                    else {
                        val_offset += vsizes[i];
                        val_remaining_size -= vsizes[i];
                    }
                }

                key_offset += ksizes[i];
            }

            vals.size = vals.size - val_remaining_size;
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto key_umem = keys.data + offset;
            auto key_size = ksizes[i];
            int ret = unqlite_kv_delete(m_db, key_umem, (int)key_size);
            if(ret != UNQLITE_OK && ret != UNQLITE_NOTFOUND)
                return Status::Other; // TODO convert status
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const UserMem& prefix,
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

    virtual Status listKeyValues(int32_t mode, bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& prefix,
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

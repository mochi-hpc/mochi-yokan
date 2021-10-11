/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "../common/locks.hpp"
#include "../common/modes.hpp"
#include <unqlite.h>
#include <nlohmann/json.hpp>
#include <abt.h>
#include <string>
#include <cstring>
#include <experimental/filesystem>
#include <iostream>

namespace yokan {

using json = nlohmann::json;

class UnQLiteKeyValueStore : public KeyValueStoreInterface {

    static Status convertStatus(int ret) {
        switch(ret) {
        case UNQLITE_NOMEM: return Status::SizeError;
        case UNQLITE_ABORT: return Status::Aborted;
        case UNQLITE_IOERR: return Status::Other;
        case UNQLITE_CORRUPT: return Status::Corruption;
        case UNQLITE_LOCKED: return Status::TryAgain;
        case UNQLITE_BUSY: return Status::Busy;
        case UNQLITE_DONE: return Status::Other;
        case UNQLITE_PERM: return Status::Permission;
        case UNQLITE_NOTIMPLEMENTED: return Status::NotSupported;
        case UNQLITE_NOTFOUND: return Status::NotFound;
        case UNQLITE_NOOP: return Status::Other;
        case UNQLITE_INVALID: return Status::InvalidArg;
        case UNQLITE_EOF: return Status::Other;
        case UNQLITE_UNKNOWN: return Status::Other;
        case UNQLITE_LIMIT: return Status::Other;
        case UNQLITE_EXISTS: return Status::KeyExists;
        case UNQLITE_EMPTY: return Status::Other;
        case UNQLITE_COMPILE_ERR: return Status::Other;
        case UNQLITE_VM_ERR: return Status::Other;
        case UNQLITE_FULL: return Status::Other;
        case UNQLITE_CANTOPEN: return Status::Other;
        case UNQLITE_READ_ONLY: return Status::Other;
        case UNQLITE_LOCKERR: return Status::Other;
        }
        return Status::Other;
    }

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
            return convertStatus(ret);

        if(cfg["max_page_cache"].get<int>() >= 0) {
            ret = unqlite_config(db, UNQLITE_CONFIG_MAX_PAGE_CACHE, cfg["max_page_cache"].get<int>());
            if(ret != UNQLITE_OK) return convertStatus(ret);
        }
        if(cfg["disable_auto_commit"].get<bool>()) {
            ret = unqlite_config(db, UNQLITE_CONFIG_DISABLE_AUTO_COMMIT);
            if(ret != UNQLITE_OK) return convertStatus(ret);
        }

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
                     YOKAN_MODE_INCLUSIVE
                    |YOKAN_MODE_APPEND
                    |YOKAN_MODE_CONSUME
        //            |YOKAN_MODE_WAIT
        //            |YOKAN_MODE_NEW_ONLY
        //            |YOKAN_MODE_EXIST_ONLY
        //            |YOKAN_MODE_NO_PREFIX
        //            |YOKAN_MODE_IGNORE_KEYS
        //            |YOKAN_MODE_KEEP_LAST
        //            |YOKAN_MODE_SUFFIX
#ifdef HAS_LUA
                    |YOKAN_MODE_LUA_FILTER
#endif
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
            auto key_umem = keys.data + offset;
            auto key_size = ksizes[i];
            unqlite_int64 val_size = 0;
            int ret = unqlite_kv_fetch(m_db, key_umem, key_size, nullptr, &val_size);
            if(ret == UNQLITE_OK || ret == UNQLITE_NOMEM)
                flags[i] = true;
            else if(ret == UNQLITE_NOTFOUND)
                flags[i] = false;
            else {
                return convertStatus(ret);
            }
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
            auto key_umem = keys.data + offset;
            auto key_size = ksizes[i];
            unqlite_int64 val_size = 0;
            int ret = unqlite_kv_fetch(m_db, key_umem, key_size, nullptr, &val_size);
            if(ret == UNQLITE_OK || ret == UNQLITE_NOMEM)
                vsizes[i] = val_size;
            else if(ret == UNQLITE_NOTFOUND)
                vsizes[i] = KeyNotFound;
            else {
                return convertStatus(ret);
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

        auto mode_append = mode & YOKAN_MODE_APPEND;

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
            if(ret != UNQLITE_OK)
                return convertStatus(ret);
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
                        return convertStatus(ret);
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
                        return convertStatus(ret);
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

        if(mode & YOKAN_MODE_CONSUME)
            return erase(mode, keys, ksizes);

        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto key_umem = keys.data + offset;
            auto key_size = ksizes[i];
            int ret = unqlite_kv_delete(m_db, key_umem, (int)key_size);
            if(ret != UNQLITE_OK && ret != UNQLITE_NOTFOUND)
                return convertStatus(ret);
            offset += ksizes[i];
        }
        return Status::OK;
    }

    private:

    struct check_from_key_args {
        const void* from_key_umem;
        size_t      from_key_size;
        bool        key_matches = false;
    };

    static int check_from_key_callback(const void* key, unsigned int ksize, void* uargs) {
        auto args = static_cast<check_from_key_args*>(uargs);
        if(ksize != args->from_key_size) return UNQLITE_OK;
        if(std::memcmp(args->from_key_umem, key, ksize) == 0)
            args->key_matches = true;
        return UNQLITE_OK;
    }

    struct check_filter_args {
        Filter filter_checker;
        bool          filter_matches = false;
    };

    static int check_filter_callback(const void* key, unsigned int ksize, void* uargs) {
        auto args = static_cast<check_filter_args*>(uargs);
        args->filter_matches =
            args->filter_checker.check(key, ksize);
        return UNQLITE_OK;
    }

    public:

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const UserMem& filter,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;
        ScopedReadLock lock(m_lock);

        unqlite_kv_cursor* cursor = nullptr;

        // initialize cursor
        int ret = unqlite_kv_cursor_init(m_db, &cursor);
        if(ret != UNQLITE_OK) return convertStatus(ret);

        // position cursor to the right start key
        if(fromKey.size == 0) {
            ret = unqlite_kv_cursor_first_entry(cursor);
            if(ret != UNQLITE_OK) {
                unqlite_kv_cursor_release(m_db, cursor);
                return convertStatus(ret);
            }
        } else {
            ret = unqlite_kv_cursor_seek(cursor, fromKey.data, fromKey.size, UNQLITE_CURSOR_MATCH_GE);
            if(ret != UNQLITE_OK) {
                unqlite_kv_cursor_release(m_db, cursor);
                return convertStatus(ret);
            }
            if(unqlite_kv_cursor_valid_entry(cursor) && !inclusive) {
                auto args = check_from_key_args{ fromKey.data, fromKey.size };
                ret = unqlite_kv_cursor_key_callback(cursor, check_from_key_callback, &args);
                if(ret != UNQLITE_OK) {
                    unqlite_kv_cursor_release(m_db, cursor);
                    return convertStatus(ret);
                }
                if(args.key_matches) {
                    unqlite_kv_cursor_next_entry(cursor);
                }
            }
        }

        struct read_key_args {
            int32_t               mode;
            bool                  packed;
            UserMem&              keys;
            BasicUserMem<size_t>& keySizes;
            const UserMem&        filter;
            size_t                key_offset = 0;
            size_t                i = 0;
            bool                  key_buf_too_small = false;
            bool                  is_last = false;
        };

        auto read_key = [](const void* key, unsigned int ksize, void* uargs) -> int {
            auto ctx = static_cast<read_key_args*>(uargs);

            size_t key_usize = ctx->packed ? (ctx->keys.size - ctx->key_offset) : ctx->keySizes[ctx->i];
            auto key_umem = ctx->keys.data + ctx->key_offset;

            if(!ctx->packed) {
                ctx->keySizes[ctx->i] = keyCopy(ctx->mode, key_umem, key_usize, key,
                                               ksize, ctx->filter.size, ctx->is_last);
                ctx->key_offset += key_usize;
            } else {
                if(ctx->key_buf_too_small) {
                    ctx->keySizes[ctx->i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    ctx->keySizes[ctx->i] = keyCopy(ctx->mode, key_umem, key_usize, key,
                                                    ksize, ctx->filter.size, ctx->is_last);
                    if(ctx->keySizes[ctx->i] == YOKAN_SIZE_TOO_SMALL) {
                        ctx->key_buf_too_small = true;
                    } else {
                        ctx->key_offset += ctx->keySizes[ctx->i];
                    }
                }
            }

            return UNQLITE_OK;
        };

        auto max = keySizes.size;
        auto ctx = read_key_args{ mode, packed, keys, keySizes, filter };

        auto filter_args = check_filter_args{ Filter{mode, filter.data, filter.size} };

        for(; unqlite_kv_cursor_valid_entry(cursor) && ctx.i < max; unqlite_kv_cursor_next_entry(cursor)) {

            if(filter.size != 0) {
                unqlite_kv_cursor_key_callback(cursor, check_filter_callback, &filter_args);
                if(!filter_args.filter_matches)
                    continue;
            }

            if(mode & YOKAN_MODE_KEEP_LAST) {
                if(ctx.i + 1 == max) {
                    ctx.is_last = true;
                } else {
                    unqlite_kv_cursor_next_entry(cursor);
                    ctx.is_last = !unqlite_kv_cursor_valid_entry(cursor);
                    unqlite_kv_cursor_prev_entry(cursor);
                }
            }

            unqlite_kv_cursor_key_callback(cursor, read_key, &ctx);
            // TODO check return value
            ctx.i += 1;
        }

        keys.size = ctx.key_offset;
        for(; ctx.i < max; ctx.i++) {
            keySizes[ctx.i] = YOKAN_NO_MORE_KEYS;
        }

        unqlite_kv_cursor_release(m_db, cursor);

        return Status::OK;
    }

    virtual Status listKeyValues(int32_t mode, bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& filter,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;
        ScopedReadLock lock(m_lock);

        unqlite_kv_cursor* cursor = nullptr;

        // initialize cursor
        int ret = unqlite_kv_cursor_init(m_db, &cursor);
        if(ret != UNQLITE_OK) return convertStatus(ret);

        // position cursor to the right start key
        if(fromKey.size == 0) {
            ret = unqlite_kv_cursor_first_entry(cursor);
            if(ret != UNQLITE_OK) {
                unqlite_kv_cursor_release(m_db, cursor);
                return convertStatus(ret);
            }
        } else {
            unqlite_kv_cursor_seek(cursor, fromKey.data, fromKey.size, UNQLITE_CURSOR_MATCH_GE);
            if(unqlite_kv_cursor_valid_entry(cursor) && !inclusive) {
                auto args = check_from_key_args{ fromKey.data, fromKey.size };
                ret = unqlite_kv_cursor_key_callback(cursor, check_from_key_callback, &args);
                if(ret != UNQLITE_OK) {
                    unqlite_kv_cursor_release(m_db, cursor);
                    return convertStatus(ret);
                }
                if(args.key_matches) {
                    unqlite_kv_cursor_next_entry(cursor);
                }
            }
        }

        struct read_keyval_args {
            int32_t               mode;
            bool                  packed;
            UserMem&              keys;
            BasicUserMem<size_t>& keySizes;
            UserMem&              vals;
            BasicUserMem<size_t>& valSizes;
            const UserMem&        filter;
            size_t                key_offset = 0;
            size_t                val_offset = 0;
            size_t                i = 0;
            bool                  key_buf_too_small = false;
            bool                  val_buf_too_small = false;
            bool                  is_last = false;
        };

        auto read_key = [](const void* key, unsigned int ksize, void* uargs) -> int {
            auto ctx = static_cast<read_keyval_args*>(uargs);

            size_t key_usize = ctx->packed ? (ctx->keys.size - ctx->key_offset) : ctx->keySizes[ctx->i];
            auto key_umem = ctx->keys.data + ctx->key_offset;

            if(!ctx->packed) {
                ctx->keySizes[ctx->i] = keyCopy(ctx->mode, key_umem, key_usize, key,
                                               ksize, ctx->filter.size, ctx->is_last);
                ctx->key_offset += key_usize;
            } else {
                if(ctx->key_buf_too_small) {
                    ctx->keySizes[ctx->i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    ctx->keySizes[ctx->i] = keyCopy(ctx->mode, key_umem, key_usize, key,
                                                    ksize, ctx->filter.size, ctx->is_last);
                    if(ctx->keySizes[ctx->i] == YOKAN_SIZE_TOO_SMALL) {
                        ctx->key_buf_too_small = true;
                    } else {
                        ctx->key_offset += ctx->keySizes[ctx->i];
                    }
                }
            }

            return UNQLITE_OK;
        };

        auto read_val = [](const void* val, unsigned int vsize, void* uargs) -> int {
            auto ctx = static_cast<read_keyval_args*>(uargs);

            size_t val_usize = ctx->packed ? (ctx->vals.size - ctx->val_offset) : ctx->valSizes[ctx->i];
            auto val_umem = ctx->vals.data + ctx->val_offset;

            if(!ctx->packed) {
                ctx->valSizes[ctx->i] = valCopy(ctx->mode, val_umem, val_usize, val, vsize);
                ctx->val_offset += val_usize;
            } else {
                if(ctx->val_buf_too_small) {
                    ctx->valSizes[ctx->i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    ctx->valSizes[ctx->i] = valCopy(ctx->mode, val_umem, val_usize, val, vsize);
                    if(ctx->valSizes[ctx->i] == YOKAN_SIZE_TOO_SMALL) {
                        ctx->val_buf_too_small = true;
                    } else {
                        ctx->val_offset += ctx->valSizes[ctx->i];
                    }
                }
            }

            return UNQLITE_OK;
        };

        auto max = keySizes.size;
        auto ctx = read_keyval_args{ mode, packed, keys, keySizes, vals, valSizes, filter };

        auto filter_args = check_filter_args{ Filter{mode, filter.data, filter.size} };

        for(; unqlite_kv_cursor_valid_entry(cursor) && ctx.i < max; unqlite_kv_cursor_next_entry(cursor)) {

            if(filter.size != 0) {
                unqlite_kv_cursor_key_callback(cursor, check_filter_callback, &filter_args);
                if(!filter_args.filter_matches)
                    continue;
            }

            if(mode & YOKAN_MODE_KEEP_LAST) {
                if(ctx.i + 1 == max) {
                    ctx.is_last = true;
                } else {
                    unqlite_kv_cursor_next_entry(cursor);
                    ctx.is_last = !unqlite_kv_cursor_valid_entry(cursor);
                    unqlite_kv_cursor_prev_entry(cursor);
                }
            }

            unqlite_kv_cursor_key_callback(cursor, read_key, &ctx);
            unqlite_kv_cursor_data_callback(cursor, read_val, &ctx);
            ctx.i += 1;
        }

        keys.size = ctx.key_offset;
        vals.size = ctx.val_offset;
        for(; ctx.i < max; ctx.i++) {
            keySizes[ctx.i] = YOKAN_NO_MORE_KEYS;
            valSizes[ctx.i] = YOKAN_NO_MORE_KEYS;
        }

        unqlite_kv_cursor_release(m_db, cursor);

        return Status::OK;
    }

    ~UnQLiteKeyValueStore() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
        if(m_db)
            unqlite_close(m_db);
    }

    private:

    UnQLiteKeyValueStore(json cfg, bool use_lock, unqlite* db)
    : m_db(db)
    , m_config(std::move(cfg))
    {
        if(use_lock)
            ABT_rwlock_create(&m_lock);
    }

    unqlite*   m_db;
    json       m_config;
    ABT_rwlock m_lock = ABT_RWLOCK_NULL;
};

}

YOKAN_REGISTER_BACKEND(unqlite, yokan::UnQLiteKeyValueStore);

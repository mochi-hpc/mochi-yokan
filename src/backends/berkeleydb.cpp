/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/doc-mixin.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include "../common/modes.hpp"
#include "util/key-copy.hpp"
#include <nlohmann/json.hpp>
#include <db_cxx.h>
#include <dbstl_map.h>
#include <abt.h>
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

static inline Status convertStatus(int bdb_status) {
    switch(bdb_status) {
        case 0:
            return Status::OK;
        case DB_BUFFER_SMALL:
            return Status::SizeError;
        case DB_KEYEMPTY:
        case DB_NOTFOUND:
            return Status::NotFound;
        case DB_KEYEXIST:
            return Status::KeyExists;
        case DB_TIMEOUT:
            return Status::TimedOut;
        default:
            break;
    };
    return Status::Other;
}

class BerkeleyDBDatabase : public DocumentStoreMixin<DatabaseInterface> {

    public:

    static Status create(const std::string& name, const std::string& config, DatabaseInterface** kvs) {
        json cfg;

#define CHECK_TYPE_AND_SET_DEFAULT(__cfg__, __field__, __type__, __default__) \
        do { if(__cfg__.contains(__field__)) { \
            if(!__cfg__[__field__].is_##__type__()) { \
                return Status::InvalidConf; \
            } \
        } else { \
            __cfg__[__field__] = __default__; \
        } } while(0)

        auto db_type = DB_BTREE;

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            if(!cfg.contains("type") || !cfg["type"].is_string())
                return Status::InvalidConf;
            if(cfg["type"].get<std::string>() == "btree") {
                db_type = DB_BTREE;
            } else if(cfg["type"].get<std::string>() == "hash") {
                db_type = DB_HASH;
            } else {
                return Status::InvalidConf;
            }
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
        if(!db_home.empty()) db_home += "/yokan";

        uint32_t db_env_flags =
                DB_CREATE     |
                DB_PRIVATE    |
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
        }

        auto db_env = new DbEnv(DB_CXX_NO_EXCEPTIONS);
        int status = db_env->open(db_home.c_str(), db_env_flags, 0);
        if(status != 0)
            return convertStatus(status);
        auto db = new Db(db_env, 0);
        status = db->open(nullptr, db_file.empty() ? nullptr : db_file.c_str(),
                          db_name.empty() ? nullptr : db_name.c_str(),
                          db_type, db_flags, 0);
        if(status != 0) {
            db_env->close(0);
            delete db_env;
            return convertStatus(status);
        }

        *kvs = new BerkeleyDBDatabase(cfg, name, db_type, db_env, db);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string type() const override {
        return "berkeleydb";
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
                    |YOKAN_MODE_NEW_ONLY
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
        return m_is_sorted;
    }

    virtual void destroy() override {
        auto db_file = m_config["file"].get<std::string>();
        auto db_home = m_config["home"].get<std::string>();
        auto db_name = m_config["name"].get<std::string>();
        if(!db_home.empty()) db_home += "/yokan";

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

    virtual Status count(int32_t mode, uint64_t* c) const override {
        (void)mode;
        (void)c;
        // we don't support it because the function that allows getting
        // this number only gives an estimate, and only of the keys
        // that were committed to disk, which generally isn't useful.
        return Status::NotSupported;
    }

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
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

    virtual Status length(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
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
                return convertStatus(status);
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

        size_t total_ksizes = std::accumulate(ksizes.data,
                                              ksizes.data + ksizes.size,
                                              (size_t)0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        size_t total_vsizes = std::accumulate(vsizes.data,
                                              vsizes.data + vsizes.size,
                                              (size_t)0);
        if(total_vsizes > vals.size) return Status::InvalidArg;

        // TODO enable the use of transactions
        for(size_t i = 0; i < ksizes.size; i++) {
            Dbt key(keys.data + key_offset, ksizes[i]);
            Dbt val(vals.data + val_offset, vsizes[i]);
            key.set_flags(DB_DBT_USERMEM);
            key.set_ulen(ksizes[i]);
            val.set_flags(DB_DBT_USERMEM);
            val.set_ulen(vsizes[i]);
            int flag = (mode & YOKAN_MODE_NEW_ONLY) ?  DB_NOOVERWRITE : 0;
            int status = m_db->put(nullptr, &key, &val, flag);
            if(status != 0 && status != DB_KEYEXIST)
                return convertStatus(status);
            if(status == DB_KEYEXIST && (mode & YOKAN_MODE_NEW_ONLY) && ksizes.size == 1)
                return Status::KeyExists;
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
                    return convertStatus(status);
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
                    return convertStatus(status);
                }
                key_offset += ksizes[i];
            }
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
        size_t key_offset = 0;
        Dbt val{ nullptr, 0 };
        val.set_flags(DB_DBT_REALLOC);
        for(size_t i = 0; i < ksizes.size; i++) {
                Dbt key{ keys.data + key_offset, (u_int32_t)ksizes[i] };
                key.set_flags(DB_DBT_USERMEM);
                key.set_ulen(ksizes[i]);
                int ret = m_db->get(nullptr, &key, &val, 0);
                UserMem val_umem{nullptr, 0};
                UserMem key_umem{(char*)key.get_data(), key.get_size()};
                Status status = Status::OK;
                if(ret == 0) {
                    val_umem = UserMem{(char*)val.get_data(), val.get_size()};
                    status = func(key_umem, val_umem);
                } else if(ret == DB_NOTFOUND) {
                    val_umem = UserMem{nullptr, KeyNotFound};
                    status = func(key_umem, val_umem);
                } else {
                    status = convertStatus(ret);
                }
                if(status != Status::OK) return status;
                key_offset += ksizes[i];
        }
        free(val.get_data());
        if(mode & YOKAN_MODE_CONSUME) {
            return erase(mode, keys, ksizes);
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            auto key = Dbt{ keys.data + offset, (u_int32_t)ksizes[i] };
            key.set_flags(DB_DBT_USERMEM);
            key.set_ulen(ksizes[i]);
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            int status = m_db->del(nullptr, &key, 0);
            if(status != 0 && status != DB_NOTFOUND)
                return convertStatus(status);
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const std::shared_ptr<KeyValueFilter>& filter,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        if(m_db_type != DB_BTREE)
            return Status::NotSupported;

        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        bool key_buf_too_small = false;
        uint32_t flag = DB_CURRENT;

        auto ret = Status::OK;

        auto fromKeySlice = Dbt{ fromKey.data, (u_int32_t)fromKey.size };
        fromKeySlice.set_flags(DB_DBT_USERMEM);
        fromKeySlice.set_ulen(fromKey.size);

        auto key = Dbt{ nullptr, 0};
        key.set_flags(DB_DBT_REALLOC);

        // dummy key used to move the cursor
        auto dummy_key = Dbt{ nullptr, 0 };
        dummy_key.set_ulen(0);
        dummy_key.set_dlen(0);
        dummy_key.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        // dummy value to prevent berkeleydb from reading values,
        // or actual reallocatable value if the filter needs the value.
        auto dummy_val = Dbt{ nullptr, 0 };
        dummy_val.set_ulen(0);
        dummy_val.set_dlen(0);
        dummy_val.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        // if the filter requires a value, we need somewhere to store it.
        auto val = Dbt{ nullptr, 0 };
        if(filter->requiresValue()) {
            val.set_flags(DB_DBT_REALLOC);
        } else {
            val.set_ulen(0);
            val.set_dlen(0);
            val.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);
        }


        Dbc* cursor = nullptr;
        int status = m_db->cursor(nullptr, &cursor, 0);

        if(fromKey.size == 0) {
            // move the cursor to the beginning of the database
            status = cursor->get(&dummy_key, &dummy_val, DB_FIRST);
        } else {
            // move the cursors to fromKeySlice, or right after if not found
            status = cursor->get(&fromKeySlice, &dummy_val, DB_SET_RANGE);
            if(status == 0) {
                // move was correctly done, now check if the key we point to
                // is fromKeySlice and move to next if not inclusive
                if(!inclusive) {
                    bool start_key_found =
                        (fromKeySlice.get_size() == fromKey.size)
                        && (std::memcmp(fromKeySlice.get_data(), fromKey.data, fromKey.size) == 0);
                    if(start_key_found) {
                        // make it point to the next key
                        status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                    }
                }
            } else if(status == DB_BUFFER_SMALL) {
                // fromKeySlice buffer too small to hold the next key,
                // retry with DB_DBT_PARTIAL to make the move succeed
                fromKeySlice.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);
                status = cursor->get(&fromKeySlice, &dummy_val, DB_SET_RANGE);
            }
        }

        if(status == DB_NOTFOUND) { // empty database
            goto complete;
        }

        for(i = 0; i < max; i++) {

            // find the next key that matches the filter
            while(true) {
                status = cursor->get(&key, &val, flag);
                flag = DB_NEXT;
                if(status == DB_NOTFOUND) {
                    goto complete;
                }
                if(status != 0) {
                    ret = convertStatus(status);
                    goto complete;
                }
                if(filter->check(key.get_data(), key.get_size(), val.get_data(), val.get_size()))
                    break;
                else if(filter->shouldStop(key.get_data(), key.get_size(), val.get_data(), val.get_size()))
                    goto complete;
            }

            if(packed && key_buf_too_small) {
                keySizes[i] = YOKAN_SIZE_TOO_SMALL;
                continue;
            }

            auto key_ulen = packed ? (keys.size - key_offset) : keySizes[i];
            auto key_umem = keys.data + key_offset;

            keySizes[i] = keyCopy(mode, i == max-1, filter,
                                  key_umem, key_ulen,
                                  key.get_data(), key.get_size());
            if(keySizes[i] == YOKAN_SIZE_TOO_SMALL) {
                key_buf_too_small = true;
                if(!packed) key_offset += key_ulen;
                continue;
            } else {
                if(packed) key_offset += keySizes[i];
                else       key_offset += key_ulen;
            }
        }

    complete:
        if(ret == Status::OK) {
            keys.size = key_offset;
            for(; i < max; i++) {
                keySizes[i] = YOKAN_NO_MORE_KEYS;
            }
        }
        free(key.get_data());
        free(val.get_data());
        cursor->close();

        return ret;
    }

    virtual Status listKeyValues(int32_t mode,
                                 bool packed,
                                 const UserMem& fromKey,
                                 const std::shared_ptr<KeyValueFilter>& filter,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        if(m_db_type != DB_BTREE)
            return Status::NotSupported;

        auto inclusive = mode & YOKAN_MODE_INCLUSIVE;

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        size_t val_offset = 0;
        bool key_buf_too_small = false;
        bool val_buf_too_small = false;
        uint32_t flag = DB_CURRENT;
        auto ret = Status::OK;

        auto fromKeySlice = Dbt{ fromKey.data, (u_int32_t)fromKey.size };
        fromKeySlice.set_flags(DB_DBT_USERMEM);
        fromKeySlice.set_ulen(fromKey.size);

        auto dummy_key = Dbt{ nullptr, 0 };
        dummy_key.set_ulen(0);
        dummy_key.set_dlen(0);
        dummy_key.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        auto dummy_val = Dbt{ nullptr, 0 };
        dummy_val.set_ulen(0);
        dummy_val.set_dlen(0);
        dummy_val.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        // Dbt key used to retrieve actual keys
        auto key = Dbt{ nullptr, 0 };
        key.set_flags(DB_DBT_REALLOC);

        // Dbt val used to retrieve actual values
        auto val = Dbt{ nullptr, 0 };
        val.set_flags(DB_DBT_REALLOC);

        Dbc* cursor = nullptr;
        int status = m_db->cursor(nullptr, &cursor, 0);

        if(fromKey.size == 0) {
            // move the cursor to the beginning of the database
            status = cursor->get(&dummy_key, &dummy_val, DB_FIRST);
        } else {
            // move the cursos to fromKeySlice, or right after if not found
            status = cursor->get(&fromKeySlice, &dummy_val, DB_SET_RANGE);
            if(status == 0) {
                // move was correctly done, now check if the key we point to
                // is fromKeySlice and move to next if not inclusive
                if(!inclusive) {
                    bool start_key_found =
                        (fromKeySlice.get_size() == fromKey.size)
                        && (std::memcmp(fromKeySlice.get_data(), fromKey.data, fromKey.size) == 0);
                    if(start_key_found) {
                        // make it point to the next key
                        status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                    }
                }
            } else if(status == DB_BUFFER_SMALL) {
                // fromKeySlice buffer too small to hold the next key,
                // retry with DB_DBT_PARTIAL to make the move succeed
                fromKeySlice.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);
                status = cursor->get(&fromKeySlice, &dummy_val, DB_SET_RANGE);
            }
        }

        if(status == DB_NOTFOUND) { // empty database
            goto complete;
        }

        for(i = 0; i < max; i++) {

            // find the next key that matches the filter
            while(true) {
                status = cursor->get(&key, &val, flag);
                flag = DB_NEXT;
                if(status == DB_NOTFOUND) {
                    goto complete;
                }
                if(status != 0) {
                    ret = convertStatus(status);
                    goto complete;
                }
                if(filter->check(key.get_data(), key.get_size(), val.get_data(), val.get_size()))
                    break;
                else if(filter->shouldStop(key.get_data(), key.get_size(), val.get_data(), val.get_size()))
                    goto complete;
            }

            // compute available size in current key buffer
            auto key_ulen = packed ? (keys.size - key_offset) : keySizes[i];
            auto key_umem = keys.data + key_offset;

            // compute available size in current val buffer
            auto val_ulen = packed ? (vals.size - val_offset) : valSizes[i];
            auto val_umem = vals.data + val_offset;

            if(packed) {
                if(key_buf_too_small) {
                    keySizes[i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    keySizes[i] = keyCopy(mode, i == max-1, filter,
                                          key_umem, key_ulen,
                                          key.get_data(), key.get_size());
                    if(keySizes[i] == YOKAN_SIZE_TOO_SMALL) {
                        key_buf_too_small = true;
                    } else {
                        key_offset += keySizes[i];
                    }
                }
                if(val_buf_too_small) {
                    valSizes[i] = YOKAN_SIZE_TOO_SMALL;
                } else {
                    valSizes[i] = filter->valCopy(val_umem, val_ulen,
                                                  val.get_data(), val.get_size());
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
                                          key_umem, key_ulen,
                                          key.get_data(), key.get_size());
                    valSizes[i] = filter->valCopy(val_umem, val_ulen,
                                                  val.get_data(), val.get_size());
                    key_offset += key_ulen;
                    val_offset += val_ulen;
            }
        }

    complete:
        keys.size = key_offset;
        vals.size = val_offset;
        if(ret == Status::OK) {
            for(; i < max; i++) {
                keySizes[i] = YOKAN_NO_MORE_KEYS;
                valSizes[i] = YOKAN_NO_MORE_KEYS;
            }
        }
        free(key.get_data());
        free(val.get_data());
        cursor->close();

        return ret;
    }

    ~BerkeleyDBDatabase() {
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

    json        m_config;
    int         m_db_type;
    DbEnv*      m_db_env = nullptr;
    Db*         m_db = nullptr;
    std::string m_name;
    bool        m_is_sorted;

    BerkeleyDBDatabase(json cfg, const std::string& name, int db_type, DbEnv* env, Db* db)
    : m_config(std::move(cfg))
    , m_db_type(db_type)
    , m_db_env(env)
    , m_db(db)
    , m_name(name) {
        auto disable_doc_mixin_lock = m_config.value("disable_doc_mixin_lock", false);
        if(disable_doc_mixin_lock) disableDocMixinLock();
        m_is_sorted = m_config["type"] == "btree";
    }
};

}

YOKAN_REGISTER_BACKEND(berkeleydb, yokan::BerkeleyDBDatabase);

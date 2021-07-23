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

        *kvs = new BerkeleyDBKeyValueStore(cfg, db_type, db_env, db);
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
            if(status != 0)
                return convertStatus(status);
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        return Status::OK;
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
                            const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        if(m_db_type != DB_BTREE)
            return Status::NotSupported;

        auto inclusive = mode & RKV_MODE_INCLUSIVE;

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        bool key_buf_too_small = false;
        uint32_t flag = DB_CURRENT;

        auto ret = Status::OK;

        // this buffer is used in dummy_key so we can at least load the prefix
        std::vector<char> prefix_check_buffer(prefix.size);

        auto fromKeySlice = Dbt{ fromKey.data, (u_int32_t)fromKey.size };
        fromKeySlice.set_flags(DB_DBT_USERMEM);
        fromKeySlice.set_ulen(fromKey.size);

        // dummy_key is a 0-sized key from user memory that expects
        // a partial write hence it is used to move the cursor
        auto dummy_key = Dbt{ prefix_check_buffer.data(), (u_int32_t)prefix.size };
        dummy_key.set_ulen(prefix.size);
        dummy_key.set_dlen(prefix.size);
        dummy_key.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        // same a dummy_key
        auto dummy_val = Dbt{ nullptr, 0 };
        dummy_val.set_ulen(0);
        dummy_val.set_dlen(0);
        dummy_val.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        // Dbt key used to retrieve actual keys
        auto key = Dbt{ nullptr, 0 };
        key.set_ulen(0);
        key.set_flags(DB_DBT_USERMEM);

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

            // find the next key that matches the prefix
            while(true) {
                status = cursor->get(&dummy_key, &dummy_val, flag);
                flag = DB_NEXT;
                if(status == DB_NOTFOUND) {
                    goto complete;
                }
                if(status != 0) {
                    ret = convertStatus(status);
                    goto complete;
                }
                if(prefix.size == 0)
                    break;
                else if((dummy_key.get_size() >= prefix.size) &&
                  (std::memcmp(dummy_key.get_data(), prefix.data, prefix.size) == 0))
                    break;
            }

            if(packed && key_buf_too_small) {
                keySizes[i] = RKV_SIZE_TOO_SMALL;
                continue;
            }

            // compute available size in current key buffer
            auto key_ulen = packed ? (keys.size - key_offset) : keySizes[i];
            key.set_data(keys.data + key_offset);
            key.set_ulen(key_ulen);

            // here we know the cursor points to the right element,
            // se we can get it in key, this time

            if(key_ulen < dummy_key.get_size()) { // early easy check
                keySizes[i] = RKV_SIZE_TOO_SMALL;
                key_buf_too_small = true;
                continue;
            }

            status = cursor->get(&key, &dummy_val, DB_CURRENT);
            if(status == 0) {

                if(packed) key_offset += key.get_size();
                else       key_offset += keySizes[i];
                keySizes[i] = key.get_size();

            } else if(status == DB_BUFFER_SMALL) {

                if(!packed) key_offset += keySizes[i];
                keySizes[i] = RKV_SIZE_TOO_SMALL;
                key_buf_too_small = true;

            } else {
                ret = convertStatus(status);
                goto complete;
            }
        }

    complete:
        if(ret == Status::OK) {
            keys.size = key_offset;
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
            }
        }
        cursor->close();

        return ret;
    }

    virtual Status listKeyValues(int32_t mode,
                                 bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        if(m_db_type != DB_BTREE)
            return Status::NotSupported;

        auto inclusive = mode & RKV_MODE_INCLUSIVE;

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        size_t val_offset = 0;
        bool key_buf_too_small = false;
        bool val_buf_too_small = false;
        uint32_t flag = DB_CURRENT;
        auto ret = Status::OK;

        // this buffer is used in dummy_key so we can at least load the prefix
        std::vector<char> prefix_check_buffer(prefix.size);

        auto fromKeySlice = Dbt{ fromKey.data, (u_int32_t)fromKey.size };
        fromKeySlice.set_flags(DB_DBT_USERMEM);
        fromKeySlice.set_ulen(fromKey.size);

        auto dummy_key = Dbt{ prefix_check_buffer.data(), (u_int32_t)prefix.size };
        dummy_key.set_ulen(prefix.size);
        dummy_key.set_dlen(prefix.size);
        dummy_key.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        auto dummy_val = Dbt{ nullptr, 0 };
        dummy_val.set_ulen(0);
        dummy_val.set_dlen(0);
        dummy_val.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        // Dbt key used to retrieve actual keys
        auto key = Dbt{ nullptr, 0 };
        key.set_ulen(0);
        key.set_flags(DB_DBT_USERMEM);

        // Dbt val used to retrieve actual values
        auto val = Dbt{ nullptr, 0 };
        val.set_ulen(0);
        val.set_flags(DB_DBT_USERMEM);

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

            // find the next key that matches the prefix
            while(true) {
                status = cursor->get(&dummy_key, &dummy_val, flag);
                flag = DB_NEXT;
                if(status == DB_NOTFOUND) {
                    goto complete;
                }
                if(status != 0) {
                    ret = convertStatus(status);
                    goto complete;
                }
                if(prefix.size == 0)
                    break;
                else if((dummy_key.get_size() >= prefix.size) &&
                  (std::memcmp(dummy_key.get_data(), prefix.data, prefix.size) == 0))
                    break;
            }

            // compute available size in current key buffer
            auto key_ulen = packed ? (keys.size - key_offset) : keySizes[i];
            key.set_data(keys.data + key_offset);
            key.set_ulen(key_ulen);

            // compute available size in current val buffer
            auto val_ulen = packed ? (vals.size - val_offset) : valSizes[i];
            val.set_data(vals.data + val_offset);
            val.set_ulen(val_ulen);

            // here we know the cursor points to the right element,
            // se we can get it in key/val, this time

            status = cursor->get(&key, &val, DB_CURRENT);
            if(status == 0 || status == DB_BUFFER_SMALL) {

                bool key_was_too_small = key.get_size() > key.get_ulen();
                key_buf_too_small = key_buf_too_small || key_was_too_small;

                if(key_was_too_small) {
                    // berkeleydb is annoying and won't load
                    // the value if the key was too small
                    key.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);
                    status = cursor->get(&key, &val, DB_CURRENT);
                    key.set_flags(DB_DBT_USERMEM);
                    if(status != 0 && status != DB_BUFFER_SMALL) {
                        ret = convertStatus(status);
                        goto complete;
                    }
                }

                bool val_was_too_small = val.get_size() > val.get_ulen();
                val_buf_too_small = val_buf_too_small || val_was_too_small;

                if(packed) {
                    if(key_buf_too_small) {
                        keySizes[i] = RKV_SIZE_TOO_SMALL;
                    } else {
                        key_offset += key.get_size();
                        keySizes[i] = key.get_size();
                    }
                } else {
                    key_offset += keySizes[i];
                    if(key_was_too_small) {
                        keySizes[i] = RKV_SIZE_TOO_SMALL;
                    } else {
                        keySizes[i] = key.get_size();
                    }
                }

                if(packed) {
                    if(val_buf_too_small) {
                        valSizes[i] = RKV_SIZE_TOO_SMALL;
                    } else {
                        val_offset += val.get_size();
                        valSizes[i] = val.get_size();
                    }
                } else {
                    val_offset += valSizes[i];
                    if(val_was_too_small) {
                        valSizes[i] = RKV_SIZE_TOO_SMALL;
                    } else {
                        valSizes[i] = val.get_size();
                    }
                }

            } else {
                ret = convertStatus(status);
                goto complete;
            }
        }

    complete:
        keys.size = key_offset;
        vals.size = val_offset;
        if(ret == Status::OK) {
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
                valSizes[i] = RKV_NO_MORE_KEYS;
            }
        }
        cursor->close();

        return ret;
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
    int    m_db_type;
    DbEnv* m_db_env = nullptr;
    Db*    m_db = nullptr;

    BerkeleyDBKeyValueStore(json cfg, int db_type, DbEnv* env, Db* db)
    : m_config(std::move(cfg))
    , m_db_type(db_type)
    , m_db_env(env)
    , m_db(db) {}
};

}

RKV_REGISTER_BACKEND(berkeleydb, rkv::BerkeleyDBKeyValueStore);

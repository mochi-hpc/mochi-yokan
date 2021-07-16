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
            (void)status;
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
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            auto key = Dbt{ keys.data + offset, (u_int32_t)ksizes[i] };
            key.set_flags(DB_DBT_USERMEM);
            key.set_ulen(ksizes[i]);
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            int status = m_db->del(nullptr, &key, 0);
            (void)status;
            // TODO handle status
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status listKeys(bool packed, const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {

        auto fromKeySlice = Dbt{ fromKey.data, (u_int32_t)fromKey.size };
        fromKeySlice.set_ulen(fromKey.size);
        fromKeySlice.set_flags(DB_DBT_USERMEM);

        auto prefixSlice = Dbt{ prefix.data, (u_int32_t)prefix.size };
        prefixSlice.set_ulen(prefix.size);
        prefixSlice.set_flags(DB_DBT_USERMEM);

        auto dummy_key = Dbt{ nullptr, 0 };
        dummy_key.set_ulen(0);
        dummy_key.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        auto dummy_val = Dbt{ nullptr, 0 };
        dummy_val.set_ulen(0);
        dummy_val.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        auto key = Dbt{ nullptr, 0 };
        key.set_ulen(0);
        key.set_flags(DB_DBT_USERMEM);

        auto val = Dbt{ nullptr, 0 };
        val.set_ulen(0);
        val.set_flags(DB_DBT_USERMEM);

        Dbc* cursor = nullptr;
        int status = m_db->cursor(nullptr, &cursor, 0);

        if(fromKey.size == 0) {
            status = cursor->get(&dummy_key, &dummy_val, DB_FIRST);
        } else {
            status = cursor->get(&fromKeySlice, &dummy_val,  DB_SET_RANGE);
            bool start_key_found =
                   (status == 0 || status == DB_BUFFER_SMALL)
                && (fromKeySlice.get_size() == fromKey.size)
                && (std::memcmp(fromKeySlice.get_data(), fromKey.data, fromKey.size) == 0);
            if(start_key_found && !inclusive) {
                // not inclusive, make it point to the next key
                cursor->get(&dummy_key, &dummy_val, DB_NEXT);
            }
        }

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        bool buf_too_small = false;

        while(i < max) {

            if(packed && buf_too_small) {
                keySizes[i] = RKV_SIZE_TOO_SMALL;
                i += 1;
                status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                if(status == DB_NOTFOUND) break;
                else continue;
            }

            auto key_ulen = packed ? (keys.size - key_offset) : keySizes[i];
            key.set_data(keys.data + key_offset);
            key.set_ulen(key_ulen);

            status = cursor->get(&key, &dummy_val, DB_CURRENT);
            if(status != 0 && status != DB_BUFFER_SMALL) {
                // We got an unexpected error
                // ...
                // TODO handle status properly
                break;
            }

            // check if we had enough space for the key
            if(key.get_size() > key.get_ulen()) {
                // we did not
                // XXX here, if the current key doesn't start with the prefix, then it doesn't matter!
                // (1) if get_size() < prefix.size we are good
                // then we set DB_DBT_PARTIAL to get at least the beginning of the key
                buf_too_small = true;
                if(!packed) key_offset += keySizes[i];
                keySizes[i] = RKV_SIZE_TOO_SMALL;
                i += 1;
                status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                if(status == DB_NOTFOUND) break;
                else continue;
            }

            // check if the key starts with the prefix
            if((key.get_size() < prefix.size)
            || (std::memcmp(key.get_data(), prefix.data, prefix.size) != 0)) {
                // key doesn't start with the prefix
                status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                if(status == DB_NOTFOUND) break;
                else continue;
            }

            // here we know that the key was retrieved correctly
            if(packed) {
                key_offset += key.get_size();
            } else {
                key_offset += keySizes[i];
            }
            keySizes[i] = key.get_size();
            i += 1;
            status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
            if(status == DB_NOTFOUND) break;
        }
        keys.size = key_offset;
        for(; i < max; i++) {
            keySizes[i] = RKV_NO_MORE_KEYS;
        }
        cursor->close();

        return Status::OK;
    }

    virtual Status listKeyValues(bool packed,
                                 const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
#if 0
        auto fromKeySlice = Dbt{ fromKey.data, (u_int32_t)fromKey.size };
        fromKeySlice.set_ulen(fromKey.size);
        fromKeySlice.set_flags(DB_DBT_USERMEM);

        auto prefixSlice = Dbt{ prefix.data, (u_int32_t)prefix.size };
        prefixSlice.set_ulen(prefix.size);
        prefixSlice.set_flags(DB_DBT_USERMEM);

        auto dummy_key = Dbt{ nullptr, 0 };
        dummy_key.set_ulen(0);
        dummy_key.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        auto dummy_val = Dbt{ nullptr, 0 };
        dummy_val.set_ulen(0);
        dummy_val.set_flags(DB_DBT_USERMEM|DB_DBT_PARTIAL);

        auto key = Dbt{ nullptr, 0 };
        key.set_ulen(0);
        key.set_flags(DB_DBT_USERMEM);

        auto val = Dbt{ nullptr, 0 };
        val.set_ulen(0);
        val.set_flags(DB_DBT_USERMEM);

        Dbc* cursor = nullptr;
        int status = m_db->cursor(nullptr, &cursor, 0);

        if(fromKey.size == 0) {
            status = cursor->get(&dummy_key, &dummy_val, DB_FIRST);
        } else {
            status = cursor->get(&fromKeySlice, &dummy_val,  DB_SET_RANGE);
            bool start_key_found =
                   (status == 0 || status == DB_BUFFER_SMALL)
                && (fromKeySlice.get_size() == fromKey.size)
                && (std::memcmp(fromKeySlice.get_data(), fromKey.data, fromKey.size) == 0);
            if(start_key_found && !inclusive) {
                // not inclusive, make it point to the next key
                cursor->get(&dummy_key, &dummy_val, DB_NEXT);
            }
        }

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        size_t val_offset = 0;
        bool key_buf_too_small = false;
        bool val_buf_too_small = false;

        while(i < max) {

            if(packed && key_buf_too_small) {
                keySizes[i] = RKV_SIZE_TOO_SMALL;
                i += 1;
                status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                if(status == DB_NOTFOUND) break;
                else continue;
            }

            auto key_ulen = packed ? (keys.size - key_offset) : keySizes[i];
            key.set_data(keys.data + key_offset);
            key.set_ulen(key_ulen);
            auto val_ulen = packed ? (vals.size - val_offset) : valSizes[i];
            val.set_data(vals.data + val_offset);
            val.set_ulen(val_ulen);

            status = cursor->get(&key, &val, DB_CURRENT);
            if(status != 0 && status != DB_BUFFER_SMALL) {
                // We got an unexpected error
                // ...
                // TODO handle status properly
                break;
            }

            // check if we had enough space for the key
            if(key.get_size() > key.get_ulen()) {
                // we did not
                key_buf_too_small = true;
                if(!packed) key_offset += keySizes[i];
                keySizes[i] = RKV_SIZE_TOO_SMALL;
                i += 1;
                status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                if(status == DB_NOTFOUND) break;
                else continue;
            }

            // check if the key starts with the prefix
            if((key.get_size() < prefix.size)
            || (std::memcmp(key.get_data(), prefix.data, prefix.size) != 0)) {
                // key doesn't start with the prefix
                status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
                if(status == DB_NOTFOUND) break;
                else continue;
            }

            // here we know that the key was retrieved correctly
            if(packed) {
                key_offset += key.get_size();
            } else {
                key_offset += keySizes[i];
            }
            keySizes[i] = key.get_size();
            i += 1;
            status = cursor->get(&dummy_key, &dummy_val, DB_NEXT);
            if(status == DB_NOTFOUND) break;
        }
        keys.size = key_offset;
        for(; i < max; i++) {
            keySizes[i] = RKV_NO_MORE_KEYS;
        }
        cursor->close();

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

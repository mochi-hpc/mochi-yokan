/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <leveldb/db.h>
#include <leveldb/comparator.h>
#include <leveldb/env.h>
#include <leveldb/write_batch.h>
#include <string>
#include <cstring>
#include <iostream>
#include <experimental/filesystem>

namespace rkv {

using json = nlohmann::json;

#if 0
struct MapKeyValueStoreCompare {

    // LCOV_EXCL_START
    static bool DefaultMemCmp(const void* lhs, size_t lhsize,
                              const void* rhs, size_t rhsize) {
        auto r = std::memcmp(lhs, rhs, std::min(lhsize, rhsize));
        if(r < 0) return true;
        if(r > 0) return false;
        if(lhsize < rhsize)
            return true;
        return false;
    }
    // LCOV_EXCL_STOP

    using cmp_type = bool (*)(const void*, size_t, const void*, size_t);

    cmp_type cmp = &DefaultMemCmp;

    MapKeyValueStoreCompare() = default;

    MapKeyValueStoreCompare(cmp_type comparator)
    : cmp(comparator) {}

    bool operator()(const std::string& lhs, const std::string& rhs) const {
        return cmp(lhs.data(), lhs.size(), rhs.data(), rhs.size());
    }

    bool operator()(const std::string& lhs, const UserMem& rhs) const {
        return cmp(lhs.data(), lhs.size(), rhs.data, rhs.size);
    }

    bool operator()(const UserMem& lhs, const std::string& rhs) const {
        return cmp(lhs.data, lhs.size, rhs.data(), rhs.size());
    }

    bool operator()(const UserMem& lhs, const UserMem& rhs) const {
        return cmp(lhs.data, lhs.size, rhs.data, rhs.size);
    }

    using is_transparent = int;
};
#endif

class LevelDBKeyValueStore : public KeyValueStoreInterface {

    public:

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;
        try {
            cfg = json::parse(config);
        } catch(...) {
            return Status::InvalidConf;
        }
        // fill options and complete configuration
        leveldb::Options options;

#define SET_AND_COMPLETE(__field__, __value__) \
        options.__field__ = cfg.value(#__field__, __value__); \
        cfg[#__field__] = options.__field__

        SET_AND_COMPLETE(create_if_missing, false);
        SET_AND_COMPLETE(error_if_exists, false);
        SET_AND_COMPLETE(paranoid_checks, false);
        SET_AND_COMPLETE(write_buffer_size, (size_t)(4*1024*1024));
        SET_AND_COMPLETE(max_open_files, 1000);
        SET_AND_COMPLETE(block_size, (size_t)(4*1024));
        SET_AND_COMPLETE(block_restart_interval, 16);
        SET_AND_COMPLETE(max_file_size, (size_t)(2*1024*1024));
        SET_AND_COMPLETE(reuse_logs, false);
        options.compression = cfg.value("compression", true) ?
            leveldb::kSnappyCompression : leveldb::kNoCompression;
        cfg["compression"] = options.compression == leveldb::kSnappyCompression;
        // TODO set logger, env, block_cache, and filter_policy
        std::string path = cfg.value("path", "");
        if(path.empty()) {
            return Status::InvalidConf;
        }
        leveldb::Status status;
        leveldb::DB* db = nullptr;
        status = leveldb::DB::Open(options, path, &db);
        // TODO properly handle status

        *kvs = new LevelDBKeyValueStore(db, std::move(cfg));

        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "leveldb";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual void destroy() override {
        auto path = m_config["path"].get<std::string>();
        std::experimental::filesystem::remove_all(path);
    }

    virtual Status exists(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        std::string value;
        // TODO enable more read options in config
        leveldb::ReadOptions options;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const leveldb::Slice key{ keys.data + offset, ksizes[i] };
            flags[i] = m_db->Get(options, key, &value).ok();
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size > vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        std::string value;
        // TODO enable more read options in config
        leveldb::ReadOptions options;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const leveldb::Slice key{ keys.data + offset, ksizes[i] };
            auto status = m_db->Get(options, key, &value);
            if(status.ok()) {
                vsizes[i] = value.size();
            } else if(status.IsNotFound()) {
                vsizes[i] = KeyNotFound;
            } else {
                // TODO handle other types of status
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

        // TODO I don't know if using a WriteBatch is more efficient than
        // looping over the key/value pairs and call Put directly on the
        // database, so I should add an option in the config to allow this.
        leveldb::WriteBatch wb;

        for(size_t i = 0; i < ksizes.size; i++) {
            wb.Put(leveldb::Slice{ keys.data + key_offset, ksizes[i] },
                   leveldb::Slice{ vals.data + val_offset, vsizes[i] });
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        // TODO add an option in the config to pass sync=true to WriteOptions
        auto status = m_db->Write(leveldb::WriteOptions(), &wb);
        // TODO do something with status
        (void)status;

        return Status::OK;
    }

    virtual Status get(bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t val_offset = 0;

        std::string value;
        // TODO add "verify_checksum and "fill_cache" in configuration
        leveldb::ReadOptions options;

        if(!packed) {

            for(size_t i = 0; i < ksizes.size; i++) {
                const leveldb::Slice key{ keys.data + key_offset, ksizes[i] };
                auto status = m_db->Get(options, key, &value);
                const auto original_vsize = vsizes[i];
                if(status.IsNotFound()) {
                    vsizes[i] = KeyNotFound;
                } else if(status.ok()) {
                    if(value.size() > vsizes[i]) {
                        vsizes[i] = BufTooSmall;
                    } else {
                        std::memcpy(vals.data + val_offset, value.data(), value.size());
                        vsizes[i] = value.size();
                    }
                } else {
                    // TODO handle other leveldb status
                }
                key_offset += ksizes[i];
                val_offset += original_vsize;
            }

        } else { // if packed

            size_t val_remaining_size = vals.size;

            for(size_t i = 0; i < ksizes.size; i++) {
                const leveldb::Slice key{ keys.data + key_offset, ksizes[i] };
                auto status = m_db->Get(options, key, &value);
                if(status.IsNotFound()) {
                    vsizes[i] = KeyNotFound;
                } else if(status.ok()) {
                    if(value.size() > val_remaining_size) {
                        for(; i < ksizes.size; i++) {
                            vsizes[i] = BufTooSmall;
                        }
                    } else {
                        std::memcpy(vals.data + val_offset, value.data(), value.size());
                        vsizes[i] = value.size();
                        val_remaining_size -= vsizes[i];
                        val_offset += vsizes[i];
                    }
                } else {
                    // TODO handle other leveldb status
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
        leveldb::WriteBatch wb;
        for(size_t i = 0; i < ksizes.size; i++) {
            const auto key = leveldb::Slice{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            wb.Delete(key);
            offset += ksizes[i];
        }
        leveldb::WriteOptions options;
        // TODO add options from the configuration
        auto status = m_db->Write(options, &wb);
        // TODO handle status properly
        return Status::OK;
    }

    virtual Status listKeys(bool packed, const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {

        auto fromKeySlice = leveldb::Slice{ fromKey.data, fromKey.size };
        auto prefixSlice = leveldb::Slice { prefix.data, prefix.size };

        leveldb::ReadOptions options; // TODO add options from config
        auto iterator = m_db->NewIterator(options);
        if(fromKey.size == 0) {
            iterator->SeekToFirst();
        } else {
            iterator->Seek(fromKeySlice);
            if(!inclusive) {
                if(iterator->key().compare(fromKeySlice) == 0) {
                    iterator->Next();
                }
            }
        }

        auto max = keySizes.size;
        size_t i = 0;
        size_t offset = 0;
        bool buf_too_small = false;

        while(iterator->Valid() && i < max) {
            auto key = iterator->key();
            if(!key.starts_with(prefixSlice)) {
                iterator->Next();
                continue;
            }
            size_t usize = keySizes[i];
            auto umem = static_cast<char*>(keys.data) + offset;
            if(packed) {
                if(keys.size - offset < key.size() || buf_too_small) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    buf_too_small = true;
                } else {
                    std::memcpy(umem, key.data(), key.size());
                    keySizes[i] = key.size();
                    offset += key.size();
                }
            } else {
                if(usize < key.size()) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    offset += usize;
                } else {
                    std::memcpy(umem, key.data(), key.size());
                    keySizes[i] = key.size();
                    offset += usize;
                }
            }
            i += 1;
            iterator->Next();
        }
        keys.size = offset;
        for(; i < max; i++) {
            keySizes[i] = RKV_NO_MORE_KEYS;
        }
        delete iterator;
        return Status::OK;
    }

    virtual Status listKeyValues(bool packed,
                                 const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {

        auto fromKeySlice = leveldb::Slice{ fromKey.data, fromKey.size };
        auto prefixSlice = leveldb::Slice { prefix.data, prefix.size };

        leveldb::ReadOptions options; // TODO add options from config
        auto iterator = m_db->NewIterator(options);
        if(fromKey.size == 0) {
            iterator->SeekToFirst();
        } else {
            iterator->Seek(fromKeySlice);
            if(!inclusive) {
                if(iterator->key().compare(fromKeySlice) == 0) {
                    iterator->Next();
                }
            }
        }

        auto max = keySizes.size;
        size_t i = 0;
        size_t key_offset = 0;
        size_t val_offset = 0;
        bool key_buf_too_small = false;
        bool val_buf_too_small = false;

        while(iterator->Valid() && i < max) {
            auto key = iterator->key();
            if(!key.starts_with(prefixSlice)) {
                iterator->Next();
                continue;
            }
            auto val = iterator->value();
            size_t key_usize = keySizes[i];
            size_t val_usize = valSizes[i];
            auto key_umem = static_cast<char*>(keys.data) + key_offset;
            auto val_umem = static_cast<char*>(vals.data) + val_offset;
            if(packed) {
                if(keys.size - key_offset < key.size() || key_buf_too_small) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_buf_too_small = true;
                } else {
                    std::memcpy(key_umem, key.data(), key.size());
                    keySizes[i] = key.size();
                    key_offset += key.size();
                }
                if(vals.size - val_offset < val.size() || val_buf_too_small) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                    val_buf_too_small = true;
                } else {
                    std::memcpy(val_umem, val.data(), val.size());
                    valSizes[i] = val.size();
                    val_offset += val.size();
                }
            } else {
                if(key_usize < key.size()) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_offset += key_usize;
                } else {
                    std::memcpy(key_umem, key.data(), key.size());
                    keySizes[i] = key.size();
                    key_offset += key_usize;
                }
                if(val_usize < val.size()) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                    val_offset += val_usize;
                } else {
                    std::memcpy(val_umem, val.data(), val.size());
                    valSizes[i] = val.size();
                    val_offset += val_usize;
                }
            }
            i += 1;
            iterator->Next();
        }
        keys.size = key_offset;
        vals.size = val_offset;
        for(; i < max; i++) {
            keySizes[i] = RKV_NO_MORE_KEYS;
            valSizes[i] = RKV_NO_MORE_KEYS;
        }
        delete iterator;
        return Status::OK;
    }

    ~LevelDBKeyValueStore() {
        delete m_db;
    }

    private:

    LevelDBKeyValueStore(leveldb::DB* db, json&& cfg)
    : m_db(db)
    , m_config(std::move(cfg)) {}

    leveldb::DB* m_db;
    json m_config;
};

}

RKV_REGISTER_BACKEND(leveldb, rkv::LevelDBKeyValueStore);

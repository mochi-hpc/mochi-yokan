/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <rocksdb/db.h>
#include <rocksdb/comparator.h>
#include <rocksdb/env.h>
#include <rocksdb/write_batch.h>
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

class RocksDBKeyValueStore : public KeyValueStoreInterface {

    public:

    static inline Status convertStatus(const rocksdb::Status& s) {
        switch(s.code()) {
        case rocksdb::Status::kOk:
            return Status::OK;
        case rocksdb::Status::kNotFound:
            return Status::NotFound;
        case rocksdb::Status::kCorruption:
            return Status::Corruption;
        case rocksdb::Status::kNotSupported:
            return Status::NotSupported;
        case rocksdb::Status::kInvalidArgument:
            return Status::InvalidArg;
        case rocksdb::Status::kIOError:
            return Status::IOError;
        case rocksdb::Status::kIncomplete:
            return Status::Incomplete;
        case rocksdb::Status::kTimedOut:
            return Status::TimedOut;
        case rocksdb::Status::kAborted:
            return Status::Aborted;
        case rocksdb::Status::kBusy:
            return Status::Busy;
        case rocksdb::Status::kExpired:
            return Status::Expired;
        case rocksdb::Status::kTryAgain:
            return Status::TryAgain;
        default:
            return Status::Other;
        }
    }

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;
        try {
            cfg = json::parse(config);
        } catch(...) {
            return Status::InvalidConf;
        }
        // fill options and complete configuration
        rocksdb::Options options;

#define SET_AND_COMPLETE(__json__, __field__, __value__)               \
        do { try {                                                     \
            options.__field__ = __json__.value(#__field__, __value__); \
            __json__[#__field__] = options.__field__;                  \
        } catch(...) {                                                 \
            return Status::InvalidConf;                                \
        } } while(0)

#define CHECK_AND_ADD_MISSING(__json__, __field__, __type__, __default__) \
        do { if(!__json__.contains(__field__)) {                          \
            __json__[__field__] = __default__;                            \
        } else if(!__json__[__field__].is_##__type__()) {                 \
            return Status::InvalidConf;                                   \
        } } while(0)

        SET_AND_COMPLETE(cfg, create_if_missing, false);
        SET_AND_COMPLETE(cfg, create_missing_column_families, false);
        SET_AND_COMPLETE(cfg, error_if_exists, false);
        SET_AND_COMPLETE(cfg, paranoid_checks, false);
        SET_AND_COMPLETE(cfg, track_and_verify_wals_in_manifest, false);
        SET_AND_COMPLETE(cfg, write_buffer_size, (size_t)(64 << 20));
        SET_AND_COMPLETE(cfg, max_open_files, 1000);
        SET_AND_COMPLETE(cfg, max_file_opening_threads, 16);
        SET_AND_COMPLETE(cfg, max_total_wal_size, (uint64_t)0);
        SET_AND_COMPLETE(cfg, use_fsync, false);
        SET_AND_COMPLETE(cfg, db_log_dir, std::string());
        SET_AND_COMPLETE(cfg, wal_dir, std::string());
        SET_AND_COMPLETE(cfg, delete_obsolete_files_period_micros, 6ULL * 60 * 60 * 1000000);
        SET_AND_COMPLETE(cfg, max_background_jobs, 2);
        SET_AND_COMPLETE(cfg, base_background_compactions, -1);
        SET_AND_COMPLETE(cfg, max_background_compactions, -1);
        SET_AND_COMPLETE(cfg, max_subcompactions, 1);
        SET_AND_COMPLETE(cfg, max_background_flushes, -1);
        SET_AND_COMPLETE(cfg, max_log_file_size, 0);
        SET_AND_COMPLETE(cfg, log_file_time_to_roll, 0);
        SET_AND_COMPLETE(cfg, keep_log_file_num, 1000);
        SET_AND_COMPLETE(cfg, recycle_log_file_num, 0);
        SET_AND_COMPLETE(cfg, max_manifest_file_size, 1024 * 1024 * 1024);
        SET_AND_COMPLETE(cfg, WAL_ttl_seconds, 0);
        SET_AND_COMPLETE(cfg, WAL_size_limit_MB, 0);
        SET_AND_COMPLETE(cfg, manifest_preallocation_size, 4 * 1024 * 1024);
        SET_AND_COMPLETE(cfg, allow_mmap_reads, false);
        SET_AND_COMPLETE(cfg, allow_mmap_writes, false);
        SET_AND_COMPLETE(cfg, use_direct_reads, false);
        SET_AND_COMPLETE(cfg, use_direct_io_for_flush_and_compaction, false);
        SET_AND_COMPLETE(cfg, allow_fallocate, true);
        SET_AND_COMPLETE(cfg, is_fd_close_on_exec, true);
        SET_AND_COMPLETE(cfg, stats_dump_period_sec, 600);
        SET_AND_COMPLETE(cfg, stats_persist_period_sec, 600);
        SET_AND_COMPLETE(cfg, persist_stats_to_disk, false);
        SET_AND_COMPLETE(cfg, stats_history_buffer_size, (size_t)1024*1024);
        SET_AND_COMPLETE(cfg, advise_random_on_open, true);
        SET_AND_COMPLETE(cfg, db_write_buffer_size, (size_t)0);
        // TODO access_hint_on_compaction_start enumerator
        SET_AND_COMPLETE(cfg, new_table_reader_for_compaction_inputs, false);
        SET_AND_COMPLETE(cfg, compaction_readahead_size, (size_t)0);
        // TODO many more options
        SET_AND_COMPLETE(cfg, level0_file_num_compaction_trigger, 4);
        SET_AND_COMPLETE(cfg, max_bytes_for_level_base, 256 * 1048576);
        SET_AND_COMPLETE(cfg, snap_refresh_nanos, 0);
        SET_AND_COMPLETE(cfg, disable_auto_compactions, false);

        // TODO handle compression and compression options

        CHECK_AND_ADD_MISSING(cfg, "read_options", object, json::object());
        CHECK_AND_ADD_MISSING(cfg["read_options"], "readahead_size", number_unsigned, 0);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "max_skippable_internal_keys", number_unsigned, 0);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "verify_checksums", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "fill_cache", boolean, true);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "tailing", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "total_order_seek", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "auto_prefix_mode", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "prefix_same_as_start", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "pin_data", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "background_purge_on_iterator_cleanup", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "ignore_range_deletions", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["read_options"], "value_size_soft_limit", number_unsigned, 0);

        CHECK_AND_ADD_MISSING(cfg, "write_options", object, json::object());
        CHECK_AND_ADD_MISSING(cfg["write_options"], "sync", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["write_options"], "disableWAL", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["write_options"], "ignore_missing_column_families", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["write_options"], "no_slowdown", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["write_options"], "low_pri", boolean, false);
        CHECK_AND_ADD_MISSING(cfg["write_options"], "memtable_insert_hint_per_batch", boolean, false);

        CHECK_AND_ADD_MISSING(cfg["write_options"], "use_write_batch", boolean, false);

        // TODO set logger, env, block_cache, and filter_policy...
        if(cfg.contains("db_paths")) {
            auto& db_paths = cfg["db_paths"];
            if(!db_paths.is_array()) {
                return Status::InvalidConf;
            }
            for(auto& p : db_paths) {
                if(!p.is_object())
                    return Status::InvalidConf;
                if(!p.contains("path") || !p.contains("target_size"))
                    return Status::InvalidConf;
                if(!p["path"].is_string())
                    return Status::InvalidConf;
                if(!p["target_size"].is_number_unsigned())
                    return Status::InvalidConf;
                options.db_paths.emplace_back(
                        p["path"].get<std::string>(),
                        p["target_size"].get<uint64_t>());
            }
        }
        if(!cfg.contains("path"))
            return Status::InvalidConf;
        if(!cfg["path"].is_string())
            return Status::InvalidConf;
        auto path = cfg["path"].get<std::string>();

        rocksdb::Status status;
        rocksdb::DB* db = nullptr;
        status = rocksdb::DB::Open(options, path, &db);
        if(!status.ok())
            return convertStatus(status);

        *kvs = new RocksDBKeyValueStore(db, std::move(cfg));

        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "rocksdb";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual bool supportsMode(int32_t mode) const override {
        return mode == 0 || mode == 1;
    }

    virtual void destroy() override {
        delete m_db;
        m_db = nullptr;
        auto path = m_config["path"].get<std::string>();
        fs::remove_all(path);
    }

    virtual Status exists(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        auto count = ksizes.size;
        size_t offset = 0;
        for(size_t i = 0; i < count; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const rocksdb::Slice key{ keys.data + offset, ksizes[i] };
            rocksdb::PinnableSlice value;
            flags[i] = m_db->Get(m_read_options, m_db->DefaultColumnFamily(), key, &value).ok();
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
        if(ksizes.size > vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            const rocksdb::Slice key{ keys.data + offset, ksizes[i] };
            rocksdb::PinnableSlice value;
            auto status = m_db->Get(m_read_options, m_db->DefaultColumnFamily(), key, &value);
            if(status.ok()) {
                vsizes[i] = value.size();
            } else if(status.IsNotFound()) {
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

        if(m_use_write_batch) {
            rocksdb::WriteBatch wb;

            for(size_t i = 0; i < ksizes.size; i++) {
                wb.Put(rocksdb::Slice{ keys.data + key_offset, ksizes[i] },
                       rocksdb::Slice{ vals.data + val_offset, vsizes[i] });
                key_offset += ksizes[i];
                val_offset += vsizes[i];
            }
            auto status = m_db->Write(m_write_options, &wb);
            return convertStatus(status);

        } else {
            for(size_t i = 0; i < ksizes.size; i++) {
                auto status = m_db->Put(m_write_options,
                          rocksdb::Slice{ keys.data + key_offset, ksizes[i] },
                          rocksdb::Slice{ vals.data + val_offset, vsizes[i] });
                key_offset += ksizes[i];
                val_offset += vsizes[i];
                if(!status.ok())
                    return convertStatus(status);
            }
        }
        return Status::OK;;
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
                const rocksdb::Slice key{ keys.data + key_offset, ksizes[i] };
                rocksdb::PinnableSlice value;
                auto status = m_db->Get(m_read_options, m_db->DefaultColumnFamily(), key, &value);
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
                    return convertStatus(status);
                }
                key_offset += ksizes[i];
                val_offset += original_vsize;
            }

        } else { // if packed

            size_t val_remaining_size = vals.size;

            for(size_t i = 0; i < ksizes.size; i++) {
                const rocksdb::Slice key{ keys.data + key_offset, ksizes[i] };
                rocksdb::PinnableSlice value;
                auto status = m_db->Get(m_read_options, m_db->DefaultColumnFamily(), key, &value);
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
                    convertStatus(status);
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
        rocksdb::WriteBatch wb;
        for(size_t i = 0; i < ksizes.size; i++) {
            const auto key = rocksdb::Slice{ keys.data + offset, ksizes[i] };
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            wb.Delete(key);
            offset += ksizes[i];
        }
        auto status = m_db->Write(m_write_options, &wb);
        return convertStatus(status);
    }

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        auto inclusive = mode & RKV_MODE_INCLUSIVE;
        auto fromKeySlice = rocksdb::Slice{ fromKey.data, fromKey.size };
        auto prefixSlice = rocksdb::Slice { prefix.data, prefix.size };

        auto iterator = m_db->NewIterator(m_read_options);
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

    virtual Status listKeyValues(int32_t mode, bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {

        auto inclusive = mode & RKV_MODE_INCLUSIVE;
        auto fromKeySlice = rocksdb::Slice{ fromKey.data, fromKey.size };
        auto prefixSlice = rocksdb::Slice { prefix.data, prefix.size };

        auto iterator = m_db->NewIterator(m_read_options);
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

    ~RocksDBKeyValueStore() {
        if(m_db)
            delete m_db;
    }

    private:

    RocksDBKeyValueStore(rocksdb::DB* db, json&& cfg)
    : m_db(db)
    , m_config(std::move(cfg)) {

#define GET_OPTION(__opt__, __cfg__, __field__) \
        __opt__.__field__ = __cfg__[#__field__].get<decltype(__opt__.__field__)>()

        GET_OPTION(m_read_options, m_config["read_options"], readahead_size);
        GET_OPTION(m_read_options, m_config["read_options"], max_skippable_internal_keys);
        GET_OPTION(m_read_options, m_config["read_options"], verify_checksums);
        GET_OPTION(m_read_options, m_config["read_options"], fill_cache);
        GET_OPTION(m_read_options, m_config["read_options"], tailing);
        GET_OPTION(m_read_options, m_config["read_options"], total_order_seek);
        GET_OPTION(m_read_options, m_config["read_options"], auto_prefix_mode);
        GET_OPTION(m_read_options, m_config["read_options"], prefix_same_as_start);
        GET_OPTION(m_read_options, m_config["read_options"], pin_data);
        GET_OPTION(m_read_options, m_config["read_options"], background_purge_on_iterator_cleanup);
        GET_OPTION(m_read_options, m_config["read_options"], ignore_range_deletions);
        GET_OPTION(m_read_options, m_config["read_options"], value_size_soft_limit);

        GET_OPTION(m_write_options, m_config["write_options"], sync);
        GET_OPTION(m_write_options, m_config["write_options"], disableWAL);
        GET_OPTION(m_write_options, m_config["write_options"], ignore_missing_column_families);
        GET_OPTION(m_write_options, m_config["write_options"], no_slowdown);
        GET_OPTION(m_write_options, m_config["write_options"], low_pri);
        GET_OPTION(m_write_options, m_config["write_options"], memtable_insert_hint_per_batch);

        m_use_write_batch = m_config["write_options"]["use_write_batch"].get<bool>();
    }

    rocksdb::DB*          m_db;
    json                  m_config;
    rocksdb::ReadOptions  m_read_options;
    rocksdb::WriteOptions m_write_options;
    bool                  m_use_write_batch;
};

}

RKV_REGISTER_BACKEND(rocksdb, rkv::RocksDBKeyValueStore);

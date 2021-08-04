/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include "../common/modes.hpp"
#include <tkrzw_dbm_tree.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_dbm_skip.h>
#include <tkrzw_dbm_tiny.h>
#include <tkrzw_dbm_baby.h>
#include <nlohmann/json.hpp>
#include <abt.h>
#include <string>
#include <cstring>
#include <iostream>
#include <filesystem>

namespace rkv {

using json = nlohmann::json;

static Status convertStatus(const tkrzw::Status& status) {
    switch(status.GetCode()) {
        case tkrzw::Status::SUCCESS:
            return Status::OK;
        case tkrzw::Status::UNKNOWN_ERROR:
            return Status::Other;
        case tkrzw::Status::SYSTEM_ERROR:
            return Status::System;
        case tkrzw::Status::NOT_IMPLEMENTED_ERROR:
            return Status::NotSupported;
        case tkrzw::Status::PRECONDITION_ERROR:
            return Status::Other;
        case tkrzw::Status::INVALID_ARGUMENT_ERROR:
            return Status::InvalidArg;
        case tkrzw::Status::CANCELED_ERROR:
            return Status::Canceled;
        case tkrzw::Status::NOT_FOUND_ERROR:
            return Status::NotFound;
        case tkrzw::Status::PERMISSION_ERROR:
            return Status::Permission;
        case tkrzw::Status::INFEASIBLE_ERROR:
            return Status::Other;
        case tkrzw::Status::DUPLICATION_ERROR:
            return Status::Other;
        case tkrzw::Status::BROKEN_DATA_ERROR:
            return Status::Corruption;
        case tkrzw::Status::APPLICATION_ERROR:
            return Status::Other;
    }
    return Status::OK;
}

class TkrzwKeyValueStore : public KeyValueStoreInterface {

    public:

    static Status create(const std::string& config, KeyValueStoreInterface** kvs) {
        json cfg;

#define CHECK_TYPE_AND_COMPLETE(__cfg__, __field__, __type__, __default__, __required__) \
        do { \
            if(__cfg__.contains(__field__)) { \
                if(!__cfg__[__field__].is_##__type__()) { \
                    return Status::InvalidConf; \
                } \
            } else { \
                if(__required__) { \
                    return Status::InvalidConf; \
                } \
                __cfg__[__field__] = __default__; \
            } \
        } while(0)

#define CHECK_ENUM(__cfg__, ...) \
        do { \
            bool found = false; \
            auto& c = __cfg__.get_ref<const std::string&>(); \
            for(auto& val : { __VA_ARGS__ }) { \
                if(c == val) { \
                    found = true; \
                    break; \
                } \
            } \
            if(!found) { \
                return Status::InvalidConf; \
            } \
        } while(0)

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            if(!cfg.contains("type") || !cfg["type"].is_string())
                return Status::InvalidConf;

            auto type = cfg["type"].get<std::string>();
            if(type == "tree") {
                CHECK_TYPE_AND_COMPLETE(cfg, "max_page_size", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "max_branches", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "max_cached_pages", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "key_comparator", string, "", false);
            }
            if(type == "hash" || type == "tree") {
                CHECK_TYPE_AND_COMPLETE(cfg, "update_mode", string, "default", false);
                CHECK_ENUM(cfg["update_mode"], "default", "in_place", "appending");
                CHECK_TYPE_AND_COMPLETE(cfg, "record_crc_mode", string, "default", false);
                CHECK_ENUM(cfg["record_crc_mode"], "default", "none", "crc8", "crc16", "crc32");
                CHECK_TYPE_AND_COMPLETE(cfg, "record_comp_mode", string, "default", false);
                CHECK_ENUM(cfg["record_comp_mode"], "default", "none", "zlib", "zstd", "lz4", "lzma");
                CHECK_TYPE_AND_COMPLETE(cfg, "offset_width", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "align_pow", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "num_buckets", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "restore_mode", string, "default", false);
                CHECK_ENUM(cfg["restore_mode"], "default", "sync", "read_only", "noop");
                CHECK_TYPE_AND_COMPLETE(cfg, "fbp_capacity", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "min_read_size", number, -1, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "lock_mem_buckets", boolean, false, false);
                CHECK_TYPE_AND_COMPLETE(cfg, "cache_buckets", boolean, false, false);
            } else if(type == "tiny") {
                CHECK_TYPE_AND_COMPLETE(cfg, "num_buckets", number, -1, false);
            } else if(type == "baby") {
                CHECK_TYPE_AND_COMPLETE(cfg, "key_comparator", string, "", false);
            } else {
                return Status::InvalidConf;
            }
            CHECK_TYPE_AND_COMPLETE(cfg, "writable", boolean, true, false);
            CHECK_TYPE_AND_COMPLETE(cfg, "path", string, "", true);
        } catch(const std::exception& ex) {
            return Status::InvalidConf;
        }
        auto path = cfg["path"].get<std::string>();
        auto writable = cfg["writable"].get<bool>();

        tkrzw::DBM* db = nullptr;

        auto& type = cfg["type"].get_ref<const std::string&>();

#define SET_TUNABLE(__tun__, __field__, __cfg__, __type__) \
            do { if(!__cfg__.contains(#__field__)) \
                __tun__.__field__ = __cfg__[#__field__].get<__type__>(); \
            } while(0)

#define CONVERT_ENUM(__tun__, __field__, __cfg__, __strings__, __enums__) \
        do { \
            auto vs = std::vector<std::string> __strings__; \
            auto ve = std::vector<int> __enums__; \
            for(unsigned i = 0; i < vs.size(); i++) { \
                if(vs[i] == __cfg__[#__field__].get_ref<const std::string&>()) { \
                    __tun__.__field__ = (decltype(__tun__.__field__))ve[i]; \
                    break; \
                } \
            } \
        } while(0)

        tkrzw::Status status;
        if(type == "hash") {
            auto tmp = new tkrzw::HashDBM{};
            tkrzw::HashDBM::TuningParameters params;
            CONVERT_ENUM(params, update_mode, cfg,
                ({"default", "in_place", "appending"}),
                ({tkrzw::HashDBM::UPDATE_DEFAULT,
                  tkrzw::HashDBM::UPDATE_IN_PLACE,
                  tkrzw::HashDBM::UPDATE_APPENDING}));
            // Those appeared in more recent tkrzw
            /*
            CONVERT_ENUM(params, record_crc_mode, cfg,
                ({"default", "none", "crc8", "crc16", "crc32"}),
                ({tkrzw::HashDBM::RECORD_CRC_DEFAULT,
                  tkrzw::HashDBM::RECORD_CRC_NONE,
                  tkrzw::HashDBM::RECORD_CRC_8,
                  tkrzw::HashDBM::RECORD_CRC_16,
                  tkrzw::HashDBM::RECORD_CRC_32}));
            CONVERT_ENUM(params, record_comp_mode, cfg,
                ({"default", "none", "zlib", "zstd", "lz4", "lzma"}),
                ({tkrzw::HashDBM::RECORD_COMP_DEFAULT,
                  tkrzw::HashDBM::RECORD_COMP_NONE,
                  tkrzw::HashDBM::RECORD_COMP_ZLIB,
                  tkrzw::HashDBM::RECORD_COMP_ZSTD,
                  tkrzw::HashDBM::RECORD_COMP_LZ4,
                  tkrzw::HashDBM::RECORD_COMP_LZMA}));
            CONVERT_ENUM(params, restore_mode, cfg,
                ({"default", "sync", "read_only", "noop"}),
                ({tkrzw::HashDBM::RESTORE_DEFAULT,
                  tkrzw::HashDBM::RESTORE_SYNC,
                  tkrzw::HashDBM::RESTORE_READ_ONLY,
                  tkrzw::HashDBM::RESTORE_NOOP}));
            */
            SET_TUNABLE(params, offset_width, cfg, int32_t);
            SET_TUNABLE(params, align_pow, cfg, int32_t);
            SET_TUNABLE(params, num_buckets, cfg, int32_t);
            SET_TUNABLE(params, fbp_capacity, cfg, int32_t);
            SET_TUNABLE(params, min_read_size, cfg, int32_t);
            params.lock_mem_buckets = cfg["lock_mem_buckets"].get<bool>() ? 1 : -1;
            params.cache_buckets = cfg["cache_buckets"].get<bool>() ? 1 : -1;
            status = tmp->OpenAdvanced(path, writable, tkrzw::File::OPEN_DEFAULT, params);
            db = tmp;
        } else if(type == "tree") {
            auto tmp = new tkrzw::TreeDBM{};
            tkrzw::TreeDBM::TuningParameters params;
            CONVERT_ENUM(params, update_mode, cfg,
                ({"default", "in_place", "appending"}),
                ({tkrzw::HashDBM::UPDATE_DEFAULT,
                  tkrzw::HashDBM::UPDATE_IN_PLACE,
                  tkrzw::HashDBM::UPDATE_APPENDING}));
            // Those appeared in more recent tkrzw
            /*
            CONVERT_ENUM(params, record_crc_mode, cfg,
                ({"default", "none", "crc8", "crc16", "crc32"}),
                ({tkrzw::HashDBM::RECORD_CRC_DEFAULT,
                  tkrzw::HashDBM::RECORD_CRC_NONE,
                  tkrzw::HashDBM::RECORD_CRC_8,
                  tkrzw::HashDBM::RECORD_CRC_16,
                  tkrzw::HashDBM::RECORD_CRC_32}));
            CONVERT_ENUM(params, record_comp_mode, cfg,
                ({"default", "none", "zlib", "zstd", "lz4", "lzma"}),
                ({tkrzw::HashDBM::RECORD_COMP_DEFAULT,
                  tkrzw::HashDBM::RECORD_COMP_NONE,
                  tkrzw::HashDBM::RECORD_COMP_ZLIB,
                  tkrzw::HashDBM::RECORD_COMP_ZSTD,
                  tkrzw::HashDBM::RECORD_COMP_LZ4,
                  tkrzw::HashDBM::RECORD_COMP_LZMA}));
            CONVERT_ENUM(params, restore_mode, cfg,
                ({"default", "sync", "read_only", "noop"}),
                ({tkrzw::HashDBM::RESTORE_DEFAULT,
                  tkrzw::HashDBM::RESTORE_SYNC,
                  tkrzw::HashDBM::RESTORE_READ_ONLY,
                  tkrzw::HashDBM::RESTORE_NOOP}));
            */
            SET_TUNABLE(params, offset_width, cfg, int32_t);
            SET_TUNABLE(params, align_pow, cfg, int32_t);
            SET_TUNABLE(params, num_buckets, cfg, int32_t);
            SET_TUNABLE(params, fbp_capacity, cfg, int32_t);
            SET_TUNABLE(params, min_read_size, cfg, int32_t);
            SET_TUNABLE(params, max_page_size, cfg, int32_t);
            SET_TUNABLE(params, max_branches, cfg, int32_t);
            SET_TUNABLE(params, max_cached_pages, cfg, int32_t);
            auto key_comparator_name = cfg["key_comparator"].get<std::string>();
            // TODO add support for key_comparator argument
            params.lock_mem_buckets = cfg["lock_mem_buckets"].get<bool>() ? 1 : -1;
            params.cache_buckets = cfg["cache_buckets"].get<bool>() ? 1 : -1;
            status = tmp->OpenAdvanced(path, writable, tkrzw::File::OPEN_DEFAULT, params);
            db = tmp;
        } else if(type == "tiny") {
            auto num_buckets = cfg["num_buckets"].get<int64_t>();
            auto tmp = new tkrzw::TinyDBM{num_buckets};
            status = tmp->Open(path, writable);
            db = tmp;
        } else if(type == "baby") {
            auto key_comparator_name = cfg["key_comparator"].get<std::string>();
            // TODO add support for key_comparator
            auto tmp = new tkrzw::BabyDBM{};
            status = tmp->Open(path, writable);
            db = tmp;
        }

        if(!status.IsOK()) {
            delete db;
            return convertStatus(status);
        }
        *kvs = new TkrzwKeyValueStore(std::move(cfg), db);
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string name() const override {
        return "tkrzw";
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
                    |RKV_MODE_CONSUME
        //            |RKV_MODE_WAIT
                    |RKV_MODE_NEW_ONLY
        //            |RKV_MODE_EXIST_ONLY
        //            |RKV_MODE_NO_PREFIX
        //            |RKV_MODE_IGNORE_KEYS
        //            |RKV_MODE_KEEP_LAST
                    |RKV_MODE_SUFFIX
#ifdef HAS_LUA
                    |RKV_MODE_LUA_FILTER
#endif
                    )
            );
    }

    virtual void destroy() override {
        auto path = m_config["path"].get<std::string>();
        auto type = m_config["type"].get<std::string>();
        m_db->Close();
        delete m_db;
        m_db = nullptr;
        std::filesystem::remove(path);
    }

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto status = m_db->Get({keys.data + offset, ksizes[i]}, nullptr);
            flags[i] = status.IsOK();
            offset += ksizes[i];
        }
        return Status::OK;
    }

    struct GetLength : public tkrzw::DBM::RecordProcessor {

        size_t& m_index;
        BasicUserMem<size_t>& m_vsizes;

        GetLength(size_t& i, BasicUserMem<size_t>& vsizes)
        : m_index(i), m_vsizes(vsizes) {}

        std::string_view ProcessFull(std::string_view key,
                                     std::string_view value) override {
            (void)key;
            m_vsizes[m_index] = value.size();
            return tkrzw::DBM::RecordProcessor::NOOP;
        }

        std::string_view ProcessEmpty(std::string_view key) override {
            (void)key;
            m_vsizes[m_index] = KeyNotFound;
            return tkrzw::DBM::RecordProcessor::NOOP;
        }
    };

    virtual Status length(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t i = 0;

        GetLength get_length(i, vsizes);

        size_t offset = 0;
        for(; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto status = m_db->Process({keys.data + offset, ksizes[i]}, &get_length, false);
            if(!status.IsOK())
                return convertStatus(status);
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
                                              0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        size_t total_vsizes = std::accumulate(vsizes.data,
                                              vsizes.data + vsizes.size,
                                              0);
        if(total_vsizes > vals.size) return Status::InvalidArg;

        bool overwrite = !(mode & RKV_MODE_NEW_ONLY);

        for(size_t i = 0; i < ksizes.size; i++) {
            auto status = m_db->Set({ keys.data + key_offset, ksizes[i] },
                                    { vals.data + val_offset, vsizes[i] },
                                    overwrite);
            if(!status.IsOK() && (status != tkrzw::Status::DUPLICATION_ERROR)) {
                return convertStatus(status);
            }
            key_offset += ksizes[i];
            val_offset += vsizes[i];
        }
        return Status::OK;
    }

    struct GetValue : public tkrzw::DBM::RecordProcessor {

        size_t&               m_index;
        BasicUserMem<size_t>& m_vsizes;
        UserMem&              m_values;
        bool                  m_packed;
        size_t                m_offset = 0;

        GetValue(size_t& i, BasicUserMem<size_t>& vsizes,
                 UserMem& values, bool packed)
        : m_index(i)
        , m_vsizes(vsizes)
        , m_values(values)
        , m_packed(packed) {}

        std::string_view ProcessFull(std::string_view key,
                                     std::string_view value) override {
            (void)key;
            if(m_packed) {
                if(m_values.size - m_offset < value.size()) {
                    m_vsizes[m_index] = BufTooSmall;
                } else {
                    std::memcpy(m_values.data + m_offset, value.data(), value.size());
                    m_vsizes[m_index] = value.size();
                    m_offset += value.size();
                }
            } else {
                if(m_vsizes[m_index] < value.size()) {
                    m_offset += m_vsizes[m_index];
                    m_vsizes[m_index] = BufTooSmall;
                } else {
                    std::memcpy(m_values.data + m_offset, value.data(), value.size());
                    m_offset += m_vsizes[m_index];
                    m_vsizes[m_index] = value.size();
                }
            }
            return tkrzw::DBM::RecordProcessor::NOOP;
        }

        std::string_view ProcessEmpty(std::string_view key) override {
            (void)key;
            if(!m_packed)
                m_offset += m_vsizes[m_index];
            m_vsizes[m_index] = KeyNotFound;
            return tkrzw::DBM::RecordProcessor::NOOP;
        }
    };

    virtual Status get(int32_t mode, bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t i = 0;
        auto get_value = GetValue(i, vsizes, vals, packed);

        for(; i < ksizes.size; i++) {
            auto status = m_db->Process({ keys.data + key_offset, ksizes[i] },
                                        &get_value, false);
            if(!status.IsOK()) {
                return convertStatus(status);
            }
            if(packed && (vsizes[i] == BufTooSmall)) {
                for(; i < ksizes.size; i++)
                    vsizes[i] = BufTooSmall;
            } else {
                key_offset += ksizes[i];
            }
        }

        vals.size = key_offset;
        if(mode & RKV_MODE_CONSUME) {
            return erase(mode, keys, ksizes);
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto status = m_db->Remove({ keys.data + offset, ksizes[i] });
            if(!status.IsOK() && status != tkrzw::Status::NOT_FOUND_ERROR)
                return convertStatus(status);
            offset += ksizes[i];
        }
        return Status::OK;
    }

    struct ListKeys : public tkrzw::DBM::RecordProcessor {

        ssize_t&              m_index;
        BasicUserMem<size_t>& m_ksizes;
        UserMem&              m_keys;
        bool                  m_packed;
        bool                  m_key_buf_too_small = false;
        size_t                m_key_offset = 0;
        Filter         m_filter_checker;

        ListKeys(ssize_t& i,
                 int32_t mode,
                 const UserMem& filter,
                 BasicUserMem<size_t>& ksizes,
                 UserMem& keys,
                 bool packed)
        : m_index(i)
        , m_ksizes(ksizes)
        , m_keys(keys)
        , m_packed(packed)
        , m_filter_checker(mode, filter.data, filter.size) {}

        std::string_view ProcessFull(std::string_view key,
                                     std::string_view value) override {
            (void)value;

            if(!m_filter_checker.check(key.data(), key.size())) {
                m_index -= 1;
                return tkrzw::DBM::RecordProcessor::NOOP;
            }

            if(m_packed) {

                if(m_key_buf_too_small) {
                    m_ksizes[m_index] = BufTooSmall;
                } else if(m_keys.size - m_key_offset < key.size()) {
                    m_ksizes[m_index] = BufTooSmall;
                    m_key_buf_too_small = true;
                } else {
                    std::memcpy(m_keys.data + m_key_offset, key.data(), key.size());
                    m_ksizes[m_index] = key.size();
                    m_key_offset += key.size();
                }

            } else {

                if(m_ksizes[m_index] < key.size()) {
                    m_key_offset += m_ksizes[m_index];
                    m_ksizes[m_index] = BufTooSmall;
                } else {
                    std::memcpy(m_keys.data + m_key_offset, key.data(), key.size());
                    m_key_offset += m_ksizes[m_index];
                    m_ksizes[m_index] = key.size();
                }

            }
            return tkrzw::DBM::RecordProcessor::NOOP;
        }

        std::string_view ProcessEmpty(std::string_view key) override {
            (void)key;
            return tkrzw::DBM::RecordProcessor::NOOP;
        }
    };

    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const UserMem& filter,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        if(!m_db->IsOrdered())
            return Status::NotSupported;

        auto inclusive = mode & RKV_MODE_INCLUSIVE;

        tkrzw::Status status;

        auto iterator = m_db->MakeIterator();
        if(fromKey.size == 0) {
            status = iterator->First();
        } else {
            status = iterator->JumpUpper(
                std::string_view{ fromKey.data, fromKey.size },
                inclusive);
        }
        if(!status.IsOK()) return convertStatus(status);

        const auto max = keySizes.size;
        ssize_t i = 0;

        auto list_keys = ListKeys{i, mode, filter, keySizes, keys, packed};

        for(; (i < (ssize_t)max); i++) {
            status = iterator->Process(&list_keys, false);
            if(!status.IsOK()) {
                if(status == tkrzw::Status::NOT_FOUND_ERROR)
                    break;
                else return convertStatus(status);
            }
            status = iterator->Next();
            if(!status.IsOK()) return convertStatus(status);
        }

        keys.size = list_keys.m_key_offset;
        for(; i < (ssize_t)max; i++) {
            keySizes[i] = RKV_NO_MORE_KEYS;
        }
        return Status::OK;
    }

    struct ListKeyVals : public tkrzw::DBM::RecordProcessor {

        ssize_t&              m_index;
        BasicUserMem<size_t>& m_ksizes;
        UserMem&              m_keys;
        BasicUserMem<size_t>& m_vsizes;
        UserMem&              m_vals;
        bool                  m_packed;
        bool                  m_key_buf_too_small = false;
        bool                  m_val_buf_too_small = false;
        size_t                m_key_offset = 0;
        size_t                m_val_offset = 0;
        Filter         m_filter_checker;

        ListKeyVals(ssize_t& i,
                    int32_t mode,
                    const UserMem& filter,
                    BasicUserMem<size_t>& ksizes,
                    UserMem& keys,
                    BasicUserMem<size_t>& vsizes,
                    UserMem& vals,
                    bool packed)
        : m_index(i)
        , m_ksizes(ksizes)
        , m_keys(keys)
        , m_vsizes(vsizes)
        , m_vals(vals)
        , m_packed(packed)
        , m_filter_checker(mode, filter.data, filter.size) {}

        std::string_view ProcessFull(std::string_view key,
                                     std::string_view val) override {
            if(!m_filter_checker.check(key.data(), key.size())) {
                m_index -= 1;
                return tkrzw::DBM::RecordProcessor::NOOP;
            }

            if(m_packed) {

                if(m_key_buf_too_small) {
                    m_ksizes[m_index] = BufTooSmall;
                } else if(m_keys.size - m_key_offset < key.size()) {
                    m_ksizes[m_index] = BufTooSmall;
                    m_key_buf_too_small = true;
                } else {
                    std::memcpy(m_keys.data + m_key_offset, key.data(), key.size());
                    m_ksizes[m_index] = key.size();
                    m_key_offset += key.size();
                }

                if(m_val_buf_too_small) {
                    m_vsizes[m_index] = BufTooSmall;
                } else if(m_vals.size - m_val_offset < val.size()) {
                    m_vsizes[m_index] = BufTooSmall;
                    m_val_buf_too_small = true;
                } else {
                    std::memcpy(m_vals.data + m_val_offset, val.data(), val.size());
                    m_vsizes[m_index] = val.size();
                    m_val_offset += val.size();
                }

            } else {

                if(m_ksizes[m_index] < key.size()) {
                    m_key_offset += m_ksizes[m_index];
                    m_ksizes[m_index] = BufTooSmall;
                } else {
                    std::memcpy(m_keys.data + m_key_offset, key.data(), key.size());
                    m_key_offset += m_ksizes[m_index];
                    m_ksizes[m_index] = key.size();
                }

                if(m_vsizes[m_index] < val.size()) {
                    m_val_offset += m_vsizes[m_index];
                    m_vsizes[m_index] = BufTooSmall;
                } else {
                    std::memcpy(m_vals.data + m_val_offset, val.data(), val.size());
                    m_val_offset += m_vsizes[m_index];
                    m_vsizes[m_index] = val.size();
                }

            }
            return tkrzw::DBM::RecordProcessor::NOOP;
        }

        std::string_view ProcessEmpty(std::string_view key) override {
            (void)key;
            return tkrzw::DBM::RecordProcessor::NOOP;
        }
    };

    virtual Status listKeyValues(int32_t mode,
                                 bool packed,
                                 const UserMem& fromKey,
                                 const UserMem& filter,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const override {
        if(!m_db->IsOrdered())
            return Status::NotSupported;

        bool inclusive = mode & RKV_MODE_INCLUSIVE;

        tkrzw::Status status;

        auto iterator = m_db->MakeIterator();
        if(fromKey.size == 0) {
            status = iterator->First();
        } else {
            status = iterator->JumpUpper(
                std::string_view{ fromKey.data, fromKey.size },
                inclusive);
        }
        if(!status.IsOK()) return convertStatus(status);

        const auto max = keySizes.size;
        ssize_t i = 0;

        auto list_keyvals = ListKeyVals{i, mode, filter, keySizes, keys, valSizes, vals, packed};

        for(; (i < (ssize_t)max); i++) {
            status = iterator->Process(&list_keyvals, false);
            if(!status.IsOK()) {
                if(status == tkrzw::Status::NOT_FOUND_ERROR)
                    break;
                else return convertStatus(status);
            }
            status = iterator->Next();
            if(!status.IsOK()) return convertStatus(status);
        }

        keys.size = list_keyvals.m_key_offset;
        vals.size = list_keyvals.m_val_offset;
        for(; i < (ssize_t)max; i++) {
            keySizes[i] = RKV_NO_MORE_KEYS;
            valSizes[i] = RKV_NO_MORE_KEYS;
        }
        return Status::OK;
    }

    ~TkrzwKeyValueStore() {
        if(m_db) {
            m_db->Close();
            delete m_db;
        }
    }

    private:

    json        m_config;
    tkrzw::DBM* m_db = nullptr;

    TkrzwKeyValueStore(json config, tkrzw::DBM* db)
    : m_config(std::move(config))
    , m_db(db)
    {}
};

}

RKV_REGISTER_BACKEND(tkrzw, rkv::TkrzwKeyValueStore);

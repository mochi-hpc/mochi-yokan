/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include <tkrzw_dbm_tree.h>
#include <nlohmann/json.hpp>
#include <abt.h>
#include <string>
#include <cstring>
#include <iostream>
#include <filesystem>

namespace rkv {

using json = nlohmann::json;

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

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            CHECK_TYPE_AND_COMPLETE(cfg, "path", string, "", true);
            CHECK_TYPE_AND_COMPLETE(cfg, "writable", boolean, true, false);
        } catch(...) {
            return Status::InvalidConf;
        }
        auto path = cfg["path"].get<std::string>();
        auto writable = cfg["writable"].get<bool>();

        auto db = new tkrzw::TreeDBM{};
        auto status = db->Open(path, writable);
        // TODO check status and convert
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
        return "{}";
    }
    // LCOV_EXCL_STOP

    virtual void destroy() override {
        auto path = m_config["path"].get<std::string>();
        m_db->Close();
        delete m_db;
        m_db = nullptr;
        std::filesystem::remove(path);
    }

    virtual Status exists(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
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

    virtual Status length(const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t i = 0;

        GetLength get_length(i, vsizes);

        size_t offset = 0;
        for(; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto status = m_db->Process({keys.data + offset, ksizes[i]}, &get_length, false);
            // TODO do something with status
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

        for(size_t i = 0; i < ksizes.size; i++) {
            // TODO we can give the option to use SetMulti instead
            auto status = m_db->Set({ keys.data + key_offset, ksizes[i] },
                                    { vals.data + val_offset, vsizes[i] });
            if(!status.IsOK()) {
                // TODO convert status
                return Status::Other;
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

    virtual Status get(bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) const override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;

        size_t key_offset = 0;
        size_t i = 0;
        auto get_value = GetValue(i, vsizes, vals, packed);

        for(; i < ksizes.size; i++) {
            //std::cerr << "ksize = " << ksizes[i] << std::endl;
            auto status = m_db->Process({ keys.data + key_offset, ksizes[i] },
                                        &get_value, false);
            // TODO do something with status
            if(!status.IsOK()) {
                return Status::Other;
            }
            if(packed && (vsizes[i] == BufTooSmall)) {
                for(; i < ksizes.size; i++)
                    vsizes[i] = BufTooSmall;
            } else {
                key_offset += ksizes[i];
            }
        }

        vals.size = key_offset;

        return Status::OK;
    }

    virtual Status erase(const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            auto status = m_db->Remove({ keys.data + offset, ksizes[i] });
            // TODO handle status
            offset += ksizes[i];
        }
        return Status::OK;
    }

    struct ListKeys : public tkrzw::DBM::RecordProcessor {

        ssize_t&               m_index;
        const UserMem&        m_prefix;
        BasicUserMem<size_t>& m_ksizes;
        UserMem&              m_keys;
        bool                  m_packed;
        bool                  m_key_buf_too_small = false;
        size_t                m_key_offset = 0;

        ListKeys(ssize_t& i,
                 const UserMem& prefix,
                 BasicUserMem<size_t>& ksizes,
                 UserMem& keys,
                 bool packed)
        : m_index(i)
        , m_prefix(prefix)
        , m_ksizes(ksizes)
        , m_keys(keys)
        , m_packed(packed) {}

        std::string_view ProcessFull(std::string_view key,
                                     std::string_view value) override {
            (void)value;

            if((key.size() < m_prefix.size)
            || std::memcmp(key.data(), m_prefix.data, m_prefix.size) != 0) {
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
    virtual Status listKeys(bool packed, const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const override {
        if(!m_db->IsOrdered())
            return Status::NotSupported;

        tkrzw::Status status;

        auto iterator = m_db->MakeIterator();
        if(fromKey.size == 0) {
            status = iterator->First();
        } else {
            status = iterator->JumpUpper(
                std::string_view{ fromKey.data, fromKey.size },
                inclusive);
        }
        // TODO handle status

        const auto max = keySizes.size;
        ssize_t i = 0;

        auto list_keys = ListKeys{i, prefix, keySizes, keys, packed};

        for(; (i < (ssize_t)max); i++) {
            status = iterator->Process(&list_keys, false);
            if(!status.IsOK())
                break;
            status = iterator->Next();
            // TODO do something with status
        }

        keys.size = list_keys.m_key_offset;
        for(; i < (ssize_t)max; i++) {
            keySizes[i] = RKV_NO_MORE_KEYS;
        }
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
        ScopedReadLock lock(m_lock);

        if(!packed) {

            using iterator = decltype(m_db->begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db->begin();
            } else {
                fromKeyIt = inclusive ? m_db->lower_bound(fromKey) : m_db->upper_bound(fromKey);
            }
            const auto end = m_db->end();
            auto max = keySizes.size;
            size_t i = 0;
            size_t key_offset = 0;
            size_t val_offset = 0;
            for(auto it = fromKeyIt; it != end && i < max; it++) {
                auto& key = it->first;
                auto& val = it->second;
                if(prefix.size != 0) {
                    if(prefix.size > key.size()) continue;
                    if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                        continue;
                }
                size_t key_usize = keySizes[i];
                size_t val_usize = valSizes[i];
                auto key_umem = static_cast<char*>(keys.data) + key_offset;
                auto val_umem = static_cast<char*>(vals.data) + val_offset;
                if(key_usize < key.size()) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                } else {
                    std::memcpy(key_umem, key.data(), key.size());
                    keySizes[i] = key.size();
                }
                if(val_usize < val.size()) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                } else {
                    std::memcpy(val_umem, val.data(), val.size());
                    valSizes[i] = val.size();
                }
                key_offset += key_usize;
                val_offset += val_usize;
                i += 1;
            }
            keys.size = key_offset;
            vals.size = val_offset;
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
                valSizes[i] = RKV_NO_MORE_KEYS;
            }

        } else { // if packed

            using iterator = decltype(m_db->begin());
            iterator fromKeyIt;
            if(fromKey.size == 0) {
                fromKeyIt = m_db->begin();
            } else {
                fromKeyIt = inclusive ? m_db->lower_bound(fromKey) : m_db->upper_bound(fromKey);
            }
            const auto end = m_db->end();
            auto max = keySizes.size;
            size_t i = 0;
            size_t key_offset = 0;
            size_t val_offset = 0;
            bool key_buf_too_small = false;
            bool val_buf_too_small = false;
            for(auto it = fromKeyIt; it != end && i < max; it++) {
                auto& key = it->first;
                auto& val = it->second;
                if(prefix.size != 0) {
                    if(prefix.size > key.size()) continue;
                    if(std::memcmp(key.data(), prefix.data, prefix.size) != 0)
                        continue;
                }
                auto key_umem = static_cast<char*>(keys.data) + key_offset;
                auto val_umem = static_cast<char*>(vals.data) + val_offset;
                if(key_buf_too_small
                || keys.size - key_offset < key.size()) {
                    keySizes[i] = RKV_SIZE_TOO_SMALL;
                    key_buf_too_small = true;
                } else {
                    std::memcpy(key_umem, key.data(), key.size());
                    keySizes[i] = key.size();
                    key_offset += key.size();
                }
                if(val_buf_too_small
                || vals.size - val_offset < val.size()) {
                    valSizes[i] = RKV_SIZE_TOO_SMALL;
                    val_buf_too_small = true;
                } else {
                    std::memcpy(val_umem, val.data(), val.size());
                    valSizes[i] = val.size();
                    val_offset += val.size();
                }
                i += 1;
            }
            keys.size = key_offset;
            vals.size = val_offset;
            for(; i < max; i++) {
                keySizes[i] = RKV_NO_MORE_KEYS;
                valSizes[i] = RKV_NO_MORE_KEYS;
            }

        }
        return Status::OK;
#endif
        return Status::NotSupported;
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

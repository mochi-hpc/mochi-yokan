/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/watcher.hpp"
#include "yokan/util/locks.hpp"
#include "yokan/doc-mixin.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include "../common/modes.hpp"
#include <nlohmann/json.hpp>
#include <abt.h>
#include <unordered_map>
#include <string>
#include <cstring>
#include <iostream>

namespace yokan {

using json = nlohmann::json;

class NullDatabase : public DocumentStoreMixin<DatabaseInterface> {

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
        *kvs = new NullDatabase(json::parse(config));
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string type() const override {
        return "null";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual bool supportsMode(int32_t mode) const override {
        return mode == YOKAN_MODE_DEFAULT;
    }

    bool isSorted() const override {
        return false;
    }

    virtual void destroy() override {}

    virtual Status count(int32_t mode, uint64_t* c) const override {
        (void)mode;
        *c = 0;
        return Status::OK;
    }

    virtual Status exists(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            flags[i] = false;
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status length(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const override {
        (void)mode;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            vsizes[i] = KeyNotFound;
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

        size_t total_ksizes = std::accumulate(ksizes.data,
                                              ksizes.data + ksizes.size,
                                              (size_t)0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        size_t total_vsizes = std::accumulate(vsizes.data,
                                              vsizes.data + vsizes.size,
                                              (size_t)0);
        if(total_vsizes > vals.size) return Status::InvalidArg;

        return Status::OK;
    }

    virtual Status get(int32_t mode,
                       bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) override {
        (void)mode;
        (void)keys;
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        if(packed) {
            for(size_t i = 0; i < ksizes.size; i++)
                vsizes[i] = 0;
            vsizes[0] = vals.size;
        }
        return Status::OK;
    }

    Status fetch(int32_t mode, const UserMem& keys,
                 const BasicUserMem<size_t>& ksizes,
                 const FetchCallback& func) override {
        (void)mode;
        size_t key_offset = 0;
        auto val_umem = UserMem{nullptr, 0};
        for(unsigned i=0 ; i < ksizes.size; i++) {
            auto key_umem = UserMem{keys.data + key_offset, ksizes[i]};
            auto status = func(key_umem, val_umem);
            if(status != Status::OK)
                return status;
            key_offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            offset += ksizes[i];
        }
        return Status::OK;
    }

    ~NullDatabase() {}

    private:

    NullDatabase(json cfg)
    : m_config(std::move(cfg))
    {
        auto disable_doc_mixin_lock = m_config.value("disable_doc_mixin_lock", false);
        if(disable_doc_mixin_lock) disableDocMixinLock();
    }

    json        m_config;
};

}

YOKAN_REGISTER_BACKEND(null, yokan::NullDatabase);

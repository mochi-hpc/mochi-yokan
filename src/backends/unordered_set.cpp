/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/util/locks.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <abt.h>
#include <unordered_set>
#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#ifdef YOKAN_USE_STD_STRING_VIEW
#include <string_view>
#else
#include <experimental/string_view>
#endif

namespace yokan {

using json = nlohmann::json;

template<typename KeyType>
struct UnorderedSetDatabaseHash {

#ifdef YOKAN_USE_STD_STRING_VIEW
    using sv = std::string_view;
#else
    using sv = std::experimental::string_view;
#endif

    std::size_t operator()(KeyType const& key) const noexcept
    {
        return std::hash<sv>{}({ key.data(), key.size() });
    }
};

class UnorderedSetDatabase : public DatabaseInterface {

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
        json cfg;
        yk_allocator_init_fn key_alloc_init, node_alloc_init;
        yk_allocator_t key_alloc, node_alloc;
        std::string key_alloc_conf, node_alloc_conf;

        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            // check use_lock
            bool use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;

            // bucket number
            if(!cfg.contains("initial_bucket_count")) {
                cfg["initial_bucket_count"] = 23;
            } else {
                if(!cfg["initial_bucket_count"].is_number_unsigned())
                    return Status::InvalidConf;
            }

            // check allocators
            if(!cfg.contains("allocators")) {
                cfg["allocators"]["key_allocator"] = "default";
                cfg["allocators"]["node_allocator"] = "default";
            } else if(!cfg["allocators"].is_object()) {
                return Status::InvalidConf;
            }

            auto& alloc_cfg = cfg["allocators"];

            // key allocator
            auto key_allocator_name = alloc_cfg.value("key_allocator", "default");
            auto key_allocator_config = alloc_cfg.value("key_allocator_config", json::object());
            alloc_cfg["key_allocator"] = key_allocator_name;
            alloc_cfg["key_allocator_config"] = key_allocator_config;
            if(key_allocator_name == "default")
                key_alloc_init = default_allocator_init;
            else
                key_alloc_init = Linker::load<decltype(key_alloc_init)>(key_allocator_name);
            if(key_alloc_init == nullptr) return Status::InvalidConf;
            key_alloc_init(&key_alloc, key_allocator_config.dump().c_str());

            // node allocator
            auto node_allocator_name = alloc_cfg.value("node_allocator", "default");
            auto node_allocator_config = alloc_cfg.value("node_allocator_config", json::object());
            alloc_cfg["node_allocator"] = node_allocator_name;
            alloc_cfg["node_allocator_config"] = node_allocator_config;
            if(node_allocator_name == "default")
                node_alloc_init = default_allocator_init;
            else
                node_alloc_init = Linker::load<decltype(node_alloc_init)>(node_allocator_name);
            if(node_alloc_init == nullptr) {
                key_alloc.finalize(key_alloc.context);
                return Status::InvalidConf;
            }
            node_alloc_init(&node_alloc, node_allocator_config.dump().c_str());

        } catch(...) {
            return Status::InvalidConf;
        }
        *kvs = new UnorderedSetDatabase(std::move(cfg), node_alloc, key_alloc);
        return Status::OK;
    }

    static Status recover(
            const std::string& config,
            const std::string& migrationConfig,
            const std::string& root,
            const std::list<std::string>& files, DatabaseInterface** kvs) {
        (void)migrationConfig;
        if(files.size() != 1) return Status::InvalidArg;
        auto filename = root + "/" + files.front();
        std::ifstream ifs(filename.c_str(), std::ios::binary);
        if(!ifs.good()) {
            return Status::IOError;
        }
        auto remove_file = [&ifs,&filename]() {
            ifs.close();
            remove(filename.c_str());
        };
        auto status = create(config, kvs);
        if(status != Status::OK) {
            remove_file();
            return status;
        }
        auto db = dynamic_cast<UnorderedSetDatabase*>(*kvs);
        ifs.seekg(0, std::ios::end);
        size_t total_size = ifs.tellg();
        ifs.clear();
        ifs.seekg(0);
        size_t size_read = 0;
        std::vector<char> key;
        while(size_read < total_size) {
            size_t ksize;
            ifs.read(reinterpret_cast<char*>(&ksize), sizeof(ksize));
            key.resize(ksize);
            ifs.read(key.data(), ksize);
            if(ifs.fail()) {
                remove_file();
                return Status::IOError;
            }
            db->m_db->emplace(key.data(), ksize, db->m_key_allocator);
            size_read += sizeof(ksize) + ksize;
        }
        remove_file();
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string type() const override {
        return "unordered_set";
    }
    // LCOV_EXCL_STOP

    // LCOV_EXCL_START
    virtual std::string config() const override {
        return m_config.dump();
    }
    // LCOV_EXCL_STOP

    virtual bool supportsMode(int32_t mode) const override {
        // note: YOKAN_MODE_APPEND, NEW_ONLY, and EXIST_ONLY
        // are marked as supported but they don't really do
        // anything because this backend doesn't store values.
        // YOKAN_MODE_IGNORE_KEYS, KEEP_LAST, and SUFFIX are
        // marked as supported but listKeys and listKeyvals
        // are not supported anyway.
        return mode ==
            (mode & (
                     YOKAN_MODE_INCLUSIVE
                    |YOKAN_MODE_APPEND
                    |YOKAN_MODE_CONSUME
        //            |YOKAN_MODE_WAIT
        //            |YOKAN_MODE_NOTIFY
                    |YOKAN_MODE_NEW_ONLY
                    |YOKAN_MODE_EXIST_ONLY
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
        return false;
    }

    virtual void destroy() override {
        ScopedWriteLock lock(m_lock);
        m_db->clear();
    }

    virtual Status count(int32_t mode, uint64_t* c) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        if(m_migrated) return Status::Migrated;
        *c = m_db->size();
        return Status::OK;
    }

    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& flags) const override {
        (void)mode;
        if(ksizes.size > flags.size) return Status::InvalidArg;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        if(m_migrated) return Status::Migrated;
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            flags[i] = m_db->count(key) > 0;
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
        if(m_migrated) return Status::Migrated;
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            auto it = m_db->find(key);
            if(it == m_db->end()) vsizes[i] = KeyNotFound;
            else vsizes[i] = 0;
            offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status put(int32_t mode, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        (void)vals;
        size_t key_offset = 0;

        size_t total_ksizes = std::accumulate(ksizes.data,
                                              ksizes.data + ksizes.size,
                                              (size_t)0);
        if(total_ksizes > keys.size) return Status::InvalidArg;

        size_t total_vsizes = std::accumulate(vsizes.data,
                                              vsizes.data + vsizes.size,
                                              (size_t)0);
        if(total_vsizes > 0) return Status::InvalidArg;

        ScopedWriteLock lock(m_lock);
        if(m_migrated) return Status::Migrated;

        if(mode & YOKAN_MODE_EXIST_ONLY) {
            if(ksizes.size == 1) {
                if(m_db->find(key_type{keys.data, ksizes[0], m_key_allocator}) == m_db->end())
                    return Status::NotFound;
            }
            return Status::OK;
        }

        if(mode & YOKAN_MODE_NEW_ONLY) {
            if(ksizes.size == 1) {
                if(m_db->find(key_type{keys.data, ksizes[0], m_key_allocator}) != m_db->end())
                    return Status::KeyExists;
            }
        }

        for(size_t i = 0; i < ksizes.size; i++) {
            m_db->emplace(keys.data + key_offset,
                          ksizes[i], m_key_allocator);
            key_offset += ksizes[i];
        }
        return Status::OK;
    }

    virtual Status get(int32_t mode, bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) override {
        if(ksizes.size != vsizes.size) return Status::InvalidArg;
        (void)packed;
        size_t key_offset = 0;
        ScopedReadLock lock(m_lock);
        if(m_migrated) return Status::Migrated;

        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            key.assign(keys.data + key_offset, ksizes[i]);
            auto it = m_db->find(key);
            if(it == m_db->end()) {
                vsizes[i] = KeyNotFound;
            } else {
                vsizes[i] = 0;
            }
            key_offset += ksizes[i];
        }
        vals.size = 0;
        if(mode & YOKAN_MODE_CONSUME) {
            lock.unlock();
            return erase(mode, keys, ksizes);
        }
        return Status::OK;
    }

    Status fetch(int32_t mode, const UserMem& keys,
                 const BasicUserMem<size_t>& ksizes,
                 const FetchCallback& func) override {
        size_t key_offset = 0;
        ScopedReadLock lock(m_lock);
        if(m_migrated) return Status::Migrated;

        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            key.assign(keys.data + key_offset, ksizes[i]);
            auto key_umem = UserMem{keys.data + key_offset, ksizes[i]};
            auto val_umem = UserMem{nullptr, 0};
            auto it = m_db->find(key);
            if(it == m_db->end()) {
                val_umem.size = KeyNotFound;
            }
            auto status = func(key_umem, val_umem);
            if(status != Status::OK)
                return status;
            key_offset += ksizes[i];
        }

        if(mode & YOKAN_MODE_CONSUME) {
            lock.unlock();
            return erase(mode, keys, ksizes);
        }
        return Status::OK;
    }

    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) override {
        (void)mode;
        size_t offset = 0;
        ScopedReadLock lock(m_lock);
        if(m_migrated) return Status::Migrated;
        auto key = key_type(m_key_allocator);
        for(size_t i = 0; i < ksizes.size; i++) {
            if(offset + ksizes[i] > keys.size) return Status::InvalidArg;
            key.assign(keys.data + offset, ksizes[i]);
            auto it = m_db->find(key);
            if(it != m_db->end()) {
                m_db->erase(it);
            }
            offset += ksizes[i];
        }
        return Status::OK;
    }

    struct UnorderedSetMigrationHandle : public MigrationHandle {

        UnorderedSetDatabase&   m_db;
        ScopedReadLock m_db_lock;
        std::string    m_filename;
        int            m_fd;
        FILE*          m_file;
        bool           m_cancel = false;

        UnorderedSetMigrationHandle(UnorderedSetDatabase& db)
            : m_db(db)
              , m_db_lock(db.m_lock) {
                  // create temporary file name
                  char template_filename[] = "/tmp/yokan-unordered-set-snapshot-XXXXXX";
                  m_fd = mkstemp(template_filename);
                  m_filename = template_filename;
                  // create temporary file
                  std::ofstream ofs(m_filename.c_str(), std::ofstream::out | std::ofstream::binary);
                  // write the map to it
                  if(m_db.m_db) {
                      for(const auto& key : *m_db.m_db) {
                          size_t ksize = key.size();
                          const char* kdata = key.data();
                          ofs.write(reinterpret_cast<const char*>(&ksize), sizeof(ksize));
                          ofs.write(kdata, ksize);
                      }
                  }
              }

        ~UnorderedSetMigrationHandle() {
            close(m_fd);
            remove(m_filename.c_str());
            if(!m_cancel) {
                m_db.m_migrated = true;
                m_db.m_db->clear();
            }
        }

        std::string getRoot() const override {
            return "/tmp";
        }

        std::list<std::string> getFiles() const override {
            return {m_filename.substr(5)}; // remove /tmp/ from the name
        }

        void cancel() override {
            m_cancel = true;
        }
    };

    Status startMigration(std::unique_ptr<MigrationHandle>& mh) override {
        if(m_migrated) return Status::Migrated;
        try {
            mh.reset(new UnorderedSetMigrationHandle(*this));
        } catch(...) {
            return Status::IOError;
        }
        return Status::OK;
    }

    ~UnorderedSetDatabase() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
        delete m_db;
        m_key_allocator.finalize(m_key_allocator.context);
        m_node_allocator.finalize(m_node_allocator.context);
    }

    private:

    using key_type = std::basic_string<char, std::char_traits<char>,
                                       Allocator<char>>;
    using equal_type = std::equal_to<key_type>;
    using allocator = Allocator<key_type>;
    using hash_type = UnorderedSetDatabaseHash<key_type>;
    using unordered_set_type = std::unordered_set<key_type, hash_type, equal_type, allocator>;

    UnorderedSetDatabase(json cfg,
                         const yk_allocator_t& node_allocator,
                         const yk_allocator_t& key_allocator)
    : m_config(std::move(cfg))
    , m_node_allocator(node_allocator)
    , m_key_allocator(key_allocator)
    {
        if(m_config["use_lock"].get<bool>())
            ABT_rwlock_create(&m_lock);
        m_db = new unordered_set_type(
                m_config["initial_bucket_count"].get<size_t>(),
                hash_type(),
                equal_type(),
                allocator(m_node_allocator));
    }

    unordered_set_type*    m_db;
    json                   m_config;
    ABT_rwlock             m_lock = ABT_RWLOCK_NULL;
    mutable yk_allocator_t m_node_allocator;
    mutable yk_allocator_t m_key_allocator;
    bool                   m_migrated = false;
};

}

YOKAN_REGISTER_BACKEND(unordered_set, yokan::UnorderedSetDatabase);

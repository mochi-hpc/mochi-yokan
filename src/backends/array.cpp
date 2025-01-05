/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/watcher.hpp"
#include "yokan/doc-mixin.hpp"
#include "yokan/util/locks.hpp"
#include "../common/linker.hpp"
#include "../common/allocator.hpp"
#include "../common/modes.hpp"
#include "util/key-copy.hpp"
#include <nlohmann/json.hpp>
#include <unistd.h>
#include <abt.h>
#include <atomic>
#include <fstream>
#include <unordered_map>
#include <string>
#include <cstring>
#include <iostream>
#include <vector>

namespace yokan {

using json = nlohmann::json;

class ArrayDatabase : public DatabaseInterface {

    struct Collection {

        std::vector<char>   m_data;
        size_t              m_count = 0;
        std::vector<size_t> m_offsets;
        std::vector<size_t> m_sizes;
        ABT_rwlock          m_lock = ABT_RWLOCK_NULL;

        Collection(bool use_lock) {
            if(use_lock)
                ABT_rwlock_create(&m_lock);
        }

        ~Collection() {
            if(m_lock != ABT_RWLOCK_NULL)
                ABT_rwlock_free(&m_lock);
        }
    };

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
        json cfg;
        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            // check use_lock
            auto use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;
        } catch(...) {
            return Status::InvalidConf;
        }
        *kvs = new ArrayDatabase(std::move(cfg));
        return Status::OK;
    }

    static Status recover(
            const std::string& config,
            const std::string& migrationConfig,
            const std::list<std::string>& files, DatabaseInterface** kvs) {
        (void)migrationConfig;
        if(files.size() != 1) return Status::InvalidArg;
        auto filename = files.front();
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
        auto db = dynamic_cast<ArrayDatabase*>(*kvs);

        // get the number of collections
        size_t num_collections;
        ifs.read(reinterpret_cast<char*>(&num_collections), sizeof(num_collections));
        for(size_t i = 0; i < num_collections; ++i) {
            // read the size of the collection name
            size_t name_size;
            ifs.read(reinterpret_cast<char*>(&name_size), sizeof(name_size));
            // read the name of the collection
            std::string name;
            name.resize(name_size);
            ifs.read(const_cast<char*>(name.data()), name_size);
            // read the number of documents
            size_t coll_size;
            ifs.read(reinterpret_cast<char*>(&coll_size), sizeof(coll_size));
            // create collection
            auto p = db->m_collections.emplace(name, db->m_lock != ABT_RWLOCK_NULL);
            auto& coll = p.first->second;
            size_t doc_offset = 0;
            // read the documents
            for(size_t j = 0; j < coll_size; ++j) {
                // read document size
                size_t doc_size;
                ifs.read(reinterpret_cast<char*>(&doc_size), sizeof(doc_size));
                coll.m_sizes.push_back(doc_size);
                coll.m_offsets.push_back(doc_offset);
                if(doc_size == YOKAN_KEY_NOT_FOUND)
                    continue;
                // read the document
                coll.m_data.resize(doc_offset + doc_size);
                ifs.read(coll.m_data.data() + doc_offset, doc_size);
                doc_offset += doc_size;
            }
        }
        remove_file();
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string type() const override {
        return "array";
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
                    |YOKAN_MODE_CONSUME
                    |YOKAN_MODE_WAIT
#ifdef YOKAN_HAS_LUA
                    |YOKAN_MODE_LUA_FILTER
#endif
                    |YOKAN_MODE_IGNORE_DOCS
                    |YOKAN_MODE_FILTER_VALUE
                    |YOKAN_MODE_LIB_FILTER
                    |YOKAN_MODE_NO_RDMA
                    |YOKAN_MODE_UPDATE_NEW
                    )
            );
    }

    bool isSorted() const override {
        return true;
    }

    Status collCreate(int32_t mode, const char* name) override {
        (void)mode;
        ScopedWriteLock lock(m_lock);
        if(m_collections.count(name))
            return Status::KeyExists;
        m_collections.emplace(name, m_lock != ABT_RWLOCK_NULL);
        return Status::OK;
    }

    Status collDrop(int32_t mode, const char* name) override {
        (void)mode;
        ScopedWriteLock lock(m_lock);
        if(!m_collections.count(name))
            return Status::NotFound;
        m_collections.erase(name);
        return Status::OK;
    }

    Status collExists(int32_t mode, const char* name, bool* flag) const override {
        (void)mode;
        ScopedWriteLock lock(m_lock);
        *flag = m_collections.count(name) != 0;
        return Status::OK;
    }

    Status collLastID(int32_t mode, const char* name, yk_id_t* id) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        auto p = m_collections.find(name);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;
        *id = coll.m_sizes.size()-1;
        return Status::OK;
    }

    Status collSize(int32_t mode, const char* name, size_t* size) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        auto p = m_collections.find(name);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;
        *size = coll.m_count;
        return Status::OK;
    }

    Status docSize(const char* collection,
                   int32_t mode,
                   const BasicUserMem<yk_id_t>& ids,
                   BasicUserMem<size_t>& sizes) const override {
        (void)mode;

        ScopedReadLock db_lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end()) {
            for(size_t i = 0; i < ids.size; ++i)
                sizes[i] = YOKAN_KEY_NOT_FOUND;
            return Status::OK;
        }
        auto& coll = p->second;

        ScopedReadLock coll_lock(coll.m_lock);
        for(size_t i = 0; i < ids.size; ++i) {
            const auto id = ids[i];
            sizes[i] = coll.m_sizes.size() > id ? coll.m_sizes[id] : YOKAN_KEY_NOT_FOUND;
        }

        return Status::OK;
    }

    Status docStore(const char* collection,
            int32_t mode,
            const UserMem& documents,
            const BasicUserMem<size_t>& sizes,
            BasicUserMem<yk_id_t>& ids) override {
        (void)mode;

        ScopedReadLock db_lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;

        ScopedWriteLock coll_lock(coll.m_lock);
        const auto count = sizes.size;
        auto id = coll.m_sizes.size();
        const char* doc_ptr = documents.data;
        size_t offset = coll.m_data.size();
        coll.m_data.reserve(std::max(2 * coll.m_data.size(), coll.m_data.size() + documents.size));
        coll.m_data.resize(coll.m_data.size() + documents.size);
        for(size_t i = 0; i < count; ++i, ++id) {
            const auto doc_size = sizes[i];
            std::memcpy(coll.m_data.data() + offset, doc_ptr, doc_size);
            coll.m_sizes.push_back(doc_size);
            coll.m_offsets.push_back(offset);
            doc_ptr += doc_size;
            offset += doc_size;
            ids[i] = id;
            coll.m_count += 1;
        }

        return Status::OK;
    }

    Status docUpdate(const char* collection,
                     int32_t mode,
                     const BasicUserMem<yk_id_t>& ids,
                     const UserMem& documents,
                     const BasicUserMem<size_t>& sizes) override {

        ScopedReadLock db_lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;

        ScopedWriteLock coll_lock(coll.m_lock);
        if(!(mode & YOKAN_MODE_UPDATE_NEW)) {
            for(size_t i = 0; i < ids.size; ++i) {
                if(ids[i] >= coll.m_sizes.size())
                    return Status::InvalidID;
            }
        }
        const auto count = ids.size;
        const char* doc_ptr = documents.data;
        size_t offset = coll.m_data.size();
        coll.m_data.reserve(std::max(2 * coll.m_data.size(), coll.m_data.size() + documents.size));
        coll.m_data.resize(coll.m_data.size() + documents.size);
        for(size_t i = 0; i < count; ++i) {
            const auto id = ids[i];
            const auto doc_size = sizes[i];

            if(id >= coll.m_sizes.size()) {
                // updating a document that does not exists, create missing ids
                for(size_t j = coll.m_sizes.size(); j <= id; ++j) {
                    coll.m_sizes.push_back(YOKAN_KEY_NOT_FOUND);
                    coll.m_offsets.push_back(YOKAN_KEY_NOT_FOUND);
                }
            }

            if(coll.m_sizes[id] == YOKAN_KEY_NOT_FOUND)
                coll.m_count += 1;

            std::memcpy(coll.m_data.data() + offset, doc_ptr, doc_size);
            coll.m_sizes[id] = doc_size;
            coll.m_offsets[id] = offset;
            doc_ptr += doc_size;
            offset += doc_size;
        }

        // TODO: the above could be optimized by reusing space if the updated document
        // has a smaller size than the original. We could also keep track of the available
        // size separately from the document size, so that subsequent updates can still
        // benefit from the size of the original document.

        return Status::OK;
    }

    Status docLoad(const char* collection,
                   int32_t mode, bool packed,
                   const BasicUserMem<yk_id_t>& ids,
                   UserMem& documents,
                   BasicUserMem<size_t>& sizes) override {
        (void)mode;

        ScopedReadLock db_lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;

        ScopedReadLock coll_lock(coll.m_lock);
        const auto count = ids.size;
        char* doc_ptr = documents.data;

        if(packed) {

            size_t remaining = documents.size;
            for(size_t i = 0; i < count; ++i) {
                const auto id = ids[i];
                if(id >= coll.m_sizes.size() || coll.m_sizes[id] == YOKAN_KEY_NOT_FOUND) {
                    sizes[i] = YOKAN_KEY_NOT_FOUND;
                    continue;
                }
                auto record = coll.m_data.data() + coll.m_offsets[id];
                auto size = coll.m_sizes[id];
                if(size > remaining) {
                    for(; i < count; ++i) {
                        sizes[i] = YOKAN_SIZE_TOO_SMALL;
                    }
                    continue;
                }
                std::memcpy(doc_ptr, record, size);
                sizes[i] = size;
                doc_ptr += size;
                remaining -= size;
            }

        } else {

            for(size_t i = 0; i < count; ++i) {
                const auto id = ids[i];
                auto buffer_size = sizes[i];
                if(id >= coll.m_sizes.size() || coll.m_sizes[id] == YOKAN_KEY_NOT_FOUND) {
                    doc_ptr += buffer_size;
                    sizes[i] = YOKAN_KEY_NOT_FOUND;
                    continue;
                }
                auto record = coll.m_data.data() + coll.m_offsets[id];
                auto size = coll.m_sizes[id];

                if(size > buffer_size) {
                    sizes[i] = YOKAN_SIZE_TOO_SMALL;
                    continue;
                }
                std::memcpy(doc_ptr, record, size);
                sizes[i] = size;
                doc_ptr += buffer_size;
            }

        }
        return Status::OK;
    }

    Status docFetch(const char* collection,
                    int32_t mode,
                    const BasicUserMem<yk_id_t>& ids,
                    const DocFetchCallback& func) override {
        (void)mode;
        ScopedReadLock db_lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;

        ScopedReadLock coll_lock(coll.m_lock);
        const auto count = ids.size;

        for(size_t i = 0; i < count; ++i) {
            const auto id = ids[i];
            if(id >= coll.m_sizes.size() || coll.m_sizes[id] == YOKAN_KEY_NOT_FOUND) {
                auto doc = UserMem{nullptr, KeyNotFound};
                func(id, doc);
                continue;
            }
            const auto doc = UserMem{
                coll.m_data.data() + coll.m_offsets[id],
                coll.m_sizes[id]
            };
            func(id, doc);
        }

        return Status::OK;
    }

    Status docErase(const char* collection,
                    int32_t mode,
                    const BasicUserMem<yk_id_t>& ids) override {
        (void)mode;
        ScopedReadLock db_lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;

        ScopedWriteLock coll_lock(coll.m_lock);
        const auto count = ids.size;

        for(size_t i = 0; i < count; ++i) {
            const auto id = ids[i];
            if(id >= coll.m_sizes.size())
                continue;
            if(coll.m_sizes[id] != YOKAN_KEY_NOT_FOUND)
                coll.m_count -= 1;
            coll.m_sizes[id]   = YOKAN_KEY_NOT_FOUND;
            coll.m_offsets[id] = YOKAN_KEY_NOT_FOUND;
            // TODO this does not actually erase the data. Ideally we would want
            // a compaction operation to happen or to have a record of the "holes"
            // so we can reuse their space.
        }
        return Status::OK;
    }

    Status docList(const char* collection,
            int32_t mode, bool packed,
            yk_id_t from_id,
            const std::shared_ptr<DocFilter>& filter,
            BasicUserMem<yk_id_t>& ids,
            UserMem& documents,
            BasicUserMem<size_t>& doc_sizes) const override {

        size_t offset = 0;
        size_t i = 0;
        DocIterCallback callback = [&](yk_id_t id, const UserMem& doc) mutable {
            if(packed) {
                if(offset + doc.size > documents.size) return Status::StopIteration;
                std::memcpy(documents.data + offset, doc.data, doc.size);
                offset += doc.size;
            } else {
                if(doc.size > doc_sizes[i]) {
                    doc_sizes[i] = YOKAN_SIZE_TOO_SMALL;
                    i++;
                    return Status::OK;
                }
                std::memcpy(documents.data + offset, doc.data, doc.size);
                offset += doc_sizes[i];
            }
            ids[i] = id;
            doc_sizes[i] = doc.size;
            i++;
            return Status::OK;
        };

        auto status = docIter(collection, mode, ids.size, from_id, filter, callback);

        for(; i < ids.size; ++i) {
            ids[i] = YOKAN_NO_MORE_DOCS;
            doc_sizes[i] = YOKAN_NO_MORE_DOCS;
        }

        return status;
    }

    virtual Status docIter(const char* collection,
            int32_t mode, uint64_t max, yk_id_t from_id,
            const std::shared_ptr<DocFilter>& filter,
            const DocIterCallback& func) const override {
        (void)mode;

        ScopedReadLock db_lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end())
            return Status::NotFound;
        auto& coll = p->second;

        ScopedReadLock coll_lock(coll.m_lock);
        yk_id_t id = from_id;
        size_t i = 0;
        std::vector<char> doc_buffer;
        while(i < max && id < coll.m_sizes.size()) {
            if(coll.m_sizes[id] == YOKAN_KEY_NOT_FOUND) {
                ++id;
                continue;
            }
            auto doc_size = coll.m_sizes[id];
            auto doc_offset = coll.m_offsets[id];
            auto doc_ptr = coll.m_data.data() + doc_offset;
            if(!filter->check(collection, id, doc_ptr, doc_size)) {
                if(filter->shouldStop(collection, doc_ptr, doc_size))
                    break;
                ++id;
                continue;
            }
            auto filtered_size = filter->docSizeFrom(collection, doc_ptr, doc_size);
            doc_buffer.resize(filtered_size);
            filtered_size = filter->docCopy(
                collection, doc_buffer.data(), filtered_size, doc_ptr, doc_size);
            auto status = func(id, UserMem{doc_buffer.data(), filtered_size});
            if(status != Status::OK)
                return status;
            ++id;
            ++i;
        }

        return Status::OK;
    }

    void destroy() override {
        ScopedWriteLock lock(m_lock);
        m_collections.clear();
    }

    struct ArrayMigrationHandle : public MigrationHandle {

        ArrayDatabase&  m_db;
        ScopedWriteLock m_db_lock;
        std::string     m_filename;
        int             m_fd;
        FILE*           m_file;
        bool            m_cancel = false;

        ArrayMigrationHandle(ArrayDatabase& db)
        : m_db(db)
        , m_db_lock(db.m_lock) {
            // create temporary file name
            char template_filename[] = "/tmp/yokan-array-snapshot-XXXXXX";
            m_fd = mkstemp(template_filename);
            m_filename = template_filename;
            // create temporary file
            std::ofstream ofs(m_filename.c_str(), std::ofstream::out | std::ofstream::binary);
            // write the number of collections
            size_t num_collections = m_db.m_collections.size();
            ofs.write(reinterpret_cast<const char*>(&num_collections), sizeof(num_collections));
            // write the collections
            for(auto& p : m_db.m_collections) {
                auto& name = p.first;
                auto& collection = p.second;
                // write the size of the collection name
                size_t name_size = name.size();
                ofs.write(reinterpret_cast<const char*>(&name_size), sizeof(name_size));
                // write the collection name
                ofs.write(name.c_str(), name.size());
                // write the collection size
                size_t coll_size = collection.m_sizes.size();
                ofs.write(reinterpret_cast<const char*>(&coll_size), sizeof(coll_size));
                // write the content of the collection
                for(size_t i = 0; i < coll_size; ++i) {
                    auto doc_size   = collection.m_sizes[i];
                    auto doc_offset = collection.m_offsets[i];
                    ofs.write(reinterpret_cast<const char*>(&doc_size), sizeof(doc_size));
                    if(doc_size == YOKAN_KEY_NOT_FOUND) continue;
                    auto doc = collection.m_data.data() + doc_offset;
                    ofs.write(doc, doc_size);
                }
            }
        }

        ~ArrayMigrationHandle() {
            close(m_fd);
            remove(m_filename.c_str());
            if(!m_cancel) {
                m_db.m_migrated = true;
                m_db.m_collections.clear();
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
            mh.reset(new ArrayMigrationHandle(*this));
        } catch(...) {
            return Status::IOError;
        }
        return Status::OK;
    }

    ~ArrayDatabase() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
    }

    private:

    ArrayDatabase(json cfg)
    : m_config(std::move(cfg))
    {
        if(m_config["use_lock"].get<bool>())
            ABT_rwlock_create(&m_lock);
    }

    std::unordered_map<std::string, Collection> m_collections;
    json                                        m_config;
    ABT_rwlock                                  m_lock = ABT_RWLOCK_NULL;
    mutable KeyWatcher                          m_watcher;
    std::atomic<bool>                           m_migrated{false};
};

}

YOKAN_REGISTER_BACKEND(array, yokan::ArrayDatabase);

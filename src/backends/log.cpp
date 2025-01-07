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
#include <stdexcept>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#ifdef YOKAN_USE_STD_FILESYSTEM
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

namespace yokan {

#ifdef YOKAN_USE_STD_FILESYSTEM
    namespace fs = std::filesystem;
#else
    namespace fs = std::experimental::filesystem;
#endif

using json = nlohmann::json;


class LogDatabase : public DatabaseInterface {

    class MemoryMappedFile {

        int syncMemory(void *addr, size_t size) {
            // Get the system's page size
            long page_size = sysconf(_SC_PAGESIZE);
            if (page_size == -1) {
                perror("sysconf failed");
                return -1;
            }

            // Calculate the start of the page-aligned address
            uintptr_t addr_start = (uintptr_t)addr;
            uintptr_t page_start = addr_start & ~(page_size - 1);

            // Calculate the adjusted size
            size_t offset = addr_start - page_start;
            size_t aligned_size = size + offset;
            if (aligned_size % page_size != 0) {
                aligned_size = ((aligned_size / page_size) + 1) * page_size;
            }

            // Call msync on the page-aligned address and adjusted size
            if (msync((void *)page_start, aligned_size, MS_SYNC) == -1) {
                perror("msync failed");
                return -1;
            }

            return 0;
        }

        void openFile() {
            // Open or create the file
            m_fd = open(m_filename.c_str(), O_RDWR | O_CREAT, 0644);
            if (m_fd < 0) {
                throw std::runtime_error("Failed to open chunk file: " + m_filename);
            }

            // Get size of the file
            auto size = lseek(m_fd, 0L, SEEK_END);
            if(size < 0) {
                close(m_fd);
                throw std::runtime_error("Failed to lseek in file: " + m_filename);
            }
            lseek(m_fd, 0L, SEEK_SET);

            // Resize the file to the chunk size if needed
            if ((size_t)size < m_size) {
                if(ftruncate(m_fd, m_size) < 0) {
                    close(m_fd);
                    throw std::runtime_error("Failed to resize chunk file: " + m_filename);
                }
            }

            // Memory-map the file
            m_data = mmap(nullptr, m_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
            if (m_data == MAP_FAILED) {
                close(m_fd);
                throw std::runtime_error("Failed to mmap chunk file: " + m_filename);
            }
        }

        public:

        MemoryMappedFile(const std::string& filename, size_t size)
            : m_filename(filename)
              , m_size(size) {
                  openFile();
              }

        ~MemoryMappedFile() {
            if (m_data) munmap(m_data, m_size);
            if (m_fd >= 0) close(m_fd);
        }

        // Disable copy and move semantics
        MemoryMappedFile(const MemoryMappedFile&) = delete;
        MemoryMappedFile& operator=(const MemoryMappedFile&) = delete;
        MemoryMappedFile(MemoryMappedFile&&) = delete;
        MemoryMappedFile& operator=(MemoryMappedFile&&) = delete;

        void write(size_t offset, const void* buffer, size_t size) {
            if (offset + size > m_size) {
                throw std::runtime_error("Write exceeds chunk size");
            }
            std::memcpy(static_cast<char*>(m_data) + offset, buffer, size);
        }

        void read(size_t offset, void* buffer, size_t size) const {
            if (offset + size > m_size) {
                throw std::runtime_error("Read exceeds chunk size");
            }
            std::memcpy(buffer, static_cast<const char*>(m_data) + offset, size);
        }

        template<typename Function>
        void fetch(size_t offset, size_t size, Function&& func) {
            if (offset + size > m_size) {
                throw std::runtime_error("Fetch exceeds chunk size");
            }
            func(UserMem{static_cast<char*>(m_data) + offset, size});
        }

        void flush(size_t offset, size_t size) {
            if (offset + size > m_size) {
                throw std::runtime_error("Flush exceeds chunk size");
            }
            if (size == 0) return;
            if (syncMemory(static_cast<char*>(m_data) + offset, size) != 0) {
                throw std::runtime_error(
                    "Failed to sync data (" + std::to_string(offset) + ", "
                    + std::to_string(size) + ") to disk for file  "
                    + m_filename + ": " + strerror(errno));
            }
        }

        void extend(size_t new_size) {
            if(new_size <= m_size) return;
            if (m_data) munmap(m_data, m_size);
            if (m_fd >= 0) close(m_fd);
            m_size = new_size;
            openFile();
        }

        size_t size() const {
            return m_size;
        }

        private:

        std::string m_filename;
        size_t      m_size;
        int         m_fd = -1;
        void*       m_data = nullptr;
    };

    using Chunk = MemoryMappedFile;
    using MetaFile = MemoryMappedFile;

    class Collection {

        struct EntryMetadata {
            uint64_t chunk  = 0;
            uint64_t offset = 0;
            uint64_t size   = 0;
        };

        public:

        Collection(const std::string& name,
                   const std::string& path_prefix,
                   size_t chunk_size)
        : m_name{name}
        , m_path_prefix{path_prefix}
        , m_chunk_size{chunk_size} {
            m_meta = std::make_shared<MetaFile>(
                    m_path_prefix + "/" + name + ".meta",
                    3*8*4096);

            m_meta->read(0, &m_coll_size, sizeof(m_coll_size));
            m_meta->read(8, &m_next_id, sizeof(m_next_id));
            m_meta->read(16, &m_last_chunk_id, sizeof(m_last_chunk_id));

            m_last_chunk = std::make_shared<Chunk>(
                    m_path_prefix + "/" + name + "." + std::to_string(m_last_chunk_id),
                    chunk_size);
        }

        uint64_t append(const void* data, size_t size) {
            if(size > m_chunk_size)
                throw std::runtime_error{
                    "Trying to append a document with a size larger than the chunk size"};
            uint64_t offset;
            m_last_chunk->read(0, &offset, sizeof(offset));
            if(size > m_chunk_size - offset) {
                m_last_chunk_id += 1;
                m_last_chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(m_last_chunk_id),
                        m_chunk_size);
                offset = 0;
            }
            // first 8 bytes of a chunk represent the next available offset - 8
            if(offset == 0)
                offset = 8;
            // write the data
            m_last_chunk->write(offset, data, size);
            m_last_chunk->flush(offset, size);
            auto new_offset = offset + size;
            // write the new offset
            m_last_chunk->write(0, &new_offset, sizeof(new_offset));
            m_last_chunk->flush(0, sizeof(new_offset));
            // write the metadata for the entry
            EntryMetadata entry{m_last_chunk_id, offset, size};
            size_t meta_offset = 24 + sizeof(EntryMetadata)*m_next_id;
            m_meta->write(meta_offset, &entry, sizeof(entry));
            m_meta->flush(meta_offset, sizeof(entry));
            // update the header of the metadata file
            m_next_id += 1;
            m_coll_size += 1;
            m_meta->write(0, &m_coll_size, sizeof(m_coll_size));
            m_meta->write(8, &m_next_id, sizeof(m_next_id));
            m_meta->write(16, &m_last_chunk_id, sizeof(m_last_chunk_id));
            m_meta->flush(0, 24);
            // return the ID of the document
            return m_next_id - 1;
        }

        void update(yk_id_t id, const void* data, size_t size) {
            if(size > m_chunk_size)
                throw std::runtime_error{
                    "Trying to append a document with a size larger than the chunk size"};
            uint64_t offset;
            m_last_chunk->read(0, &offset, sizeof(offset));
            if(size > m_chunk_size - offset) {
                m_last_chunk_id += 1;
                m_last_chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(m_last_chunk_id),
                        m_chunk_size);
                offset = 0;
            }
            // first 8 bytes of a chunk represent the next available offset - 8
            if(offset == 0)
                offset = 8;
            // read the current metadata for the entry
            EntryMetadata entry;
            auto meta_offset = 24 + sizeof(EntryMetadata)*id;
            m_meta->read(meta_offset, &entry, sizeof(entry));
            bool incr_coll_size = entry.size == YOKAN_KEY_NOT_FOUND;
            // write the new data
            m_last_chunk->write(offset, data, size);
            m_last_chunk->flush(offset, size);
            auto new_offset = offset + size;
            // write the new offset
            m_last_chunk->write(0, &new_offset, sizeof(new_offset));
            m_last_chunk->flush(0, sizeof(new_offset));
            // write the metadata for the entry
            entry.chunk  = m_last_chunk_id;
            entry.offset = offset;
            entry.size   = size;
            m_meta->write(meta_offset, &entry, sizeof(entry));
            m_meta->flush(meta_offset, sizeof(entry));
            // update the header of the metadata file
            if(incr_coll_size)
                m_coll_size += 1;
            m_meta->write(0, &m_coll_size, sizeof(m_coll_size));
            m_meta->write(16, &m_last_chunk_id, sizeof(m_last_chunk_id));
            m_meta->flush(0, 24);
        }

        Status erase(uint64_t id) {
            if(id >= m_next_id)
                return Status::InvalidID;
            // read the metadata entry
            EntryMetadata entry;
            size_t meta_offset = 24 + sizeof(EntryMetadata)*id;
            m_meta->read(meta_offset, &entry, sizeof(entry));
            if(entry.size == YOKAN_KEY_NOT_FOUND)
                return Status::OK;
            // update and write the metadata for the entry
            entry.chunk  = YOKAN_KEY_NOT_FOUND;
            entry.size   = YOKAN_KEY_NOT_FOUND;
            entry.offset = YOKAN_KEY_NOT_FOUND;
            m_meta->write(meta_offset, &entry, sizeof(entry));
            m_meta->flush(meta_offset, sizeof(entry));
            // update the header of the metadata file
            m_coll_size -= 1;
            m_meta->write(0, &m_coll_size, sizeof(m_coll_size));
            m_meta->flush(0, 8);
            return Status::OK;
        }

        uint64_t appendMulti(size_t count, const void* data, const size_t* sizes) {
            // TODO optimize this function to make it update the metadata only when
            // all the documents have been stored
            uint64_t first_id = m_next_id;
            size_t offset = 0;
            for(size_t i = 0; i < count; ++i) {
                append((const char*)data + offset, sizes[i]);
                offset += sizes[i];
            }
            return first_id;
        }

        size_t read(size_t id, void* buffer, size_t size) {
            if(id >= m_next_id)
                return YOKAN_KEY_NOT_FOUND;
            EntryMetadata entry;
            size_t meta_offset = 24 + sizeof(EntryMetadata)*id;
            m_meta->read(meta_offset, &entry, sizeof(entry));
            if(entry.size == YOKAN_KEY_NOT_FOUND)
                return YOKAN_KEY_NOT_FOUND;
            if(size < entry.size)
                return YOKAN_SIZE_TOO_SMALL;
            std::shared_ptr<Chunk> chunk;
            if(entry.chunk == m_last_chunk_id) {
                chunk = m_last_chunk;
            } else {
                chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(entry.chunk),
                        m_chunk_size);
            }
            chunk->read(entry.offset, buffer, entry.size);
            return entry.size;
        }

        void fetch(size_t entryNumber, DocFetchCallback cb) {
            if(entryNumber >= m_next_id) {
                cb(entryNumber, UserMem{nullptr, YOKAN_KEY_NOT_FOUND});
                return;
            }
            EntryMetadata entry;
            m_meta->read(24 + sizeof(EntryMetadata)*entryNumber, &entry, sizeof(entry));
            if(entry.size == YOKAN_KEY_NOT_FOUND) {
                cb(entryNumber, UserMem{nullptr, YOKAN_KEY_NOT_FOUND});
                return;
            }
            std::shared_ptr<Chunk> chunk;
            if(entry.chunk == m_last_chunk_id) {
                chunk = m_last_chunk;
            } else {
                chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(entry.chunk),
                        m_chunk_size);
            }
            auto func = [entryNumber, &cb](const UserMem& doc) {
                cb(entryNumber, doc);
            };
            chunk->fetch(entry.offset, entry.size, func);
        }

        size_t entrySize(size_t entryNumber) {
            if(entryNumber >= m_next_id)
                return YOKAN_KEY_NOT_FOUND;
            EntryMetadata entry;
            m_meta->read(24 + sizeof(EntryMetadata)*entryNumber, &entry, sizeof(entry));
            return entry.size;
        }

        auto last_id() const {
            return m_next_id-1;
        }

        auto size() const {
            return m_coll_size;
        }

        private:

        std::string               m_name;
        std::string               m_path_prefix;
        size_t                    m_chunk_size;
        std::shared_ptr<MetaFile> m_meta;
        std::shared_ptr<Chunk>    m_last_chunk;

        uint64_t m_coll_size     = 0;
        uint64_t m_next_id       = 0;
        uint64_t m_last_chunk_id = 0;
    };

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
        json cfg;
        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            if(!(cfg.contains("path") && cfg["path"].is_string())) {
                return Status::InvalidConf;
            }
            if(cfg.contains("chunk_size") && !cfg["chunk_size"].is_number_unsigned()) {
                return Status::InvalidConf;
            }
            // check use_lock
            auto use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;
            auto chunk_size = cfg.value("chunk_size", 10*1024*1024);
            cfg["chunk_size"] = chunk_size;
        } catch(...) {
            return Status::InvalidConf;
        }

        auto path = cfg["path"].get<std::string>();
        std::error_code ec;
        fs::create_directories(path, ec);

        *kvs = new LogDatabase(std::move(cfg));

        return Status::OK;
    }

    static Status recover(
            const std::string& config,
            const std::string& migrationConfig,
            const std::list<std::string>& files, DatabaseInterface** kvs) {
#if 0
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
        auto db = dynamic_cast<LogDatabase*>(*kvs);

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
#endif
        return Status::OK;
    }

    // LCOV_EXCL_START
    virtual std::string type() const override {
        return "log";
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
        // TODO add support for lock
        auto coll = std::make_shared<Collection>(name, m_path, m_chunk_size /*, m_lock != ABT_RWLOCK_NULL */);
        m_collections.emplace(name, coll);
        return Status::OK;
    }

    Status collDrop(int32_t mode, const char* name) override {
        (void)mode;
        ScopedWriteLock lock(m_lock);
        if(!m_collections.count(name))
            return Status::NotFound;
        m_collections.erase(name);
        // Remove the directory of the collection
        auto coll_path = m_path + "/" + name;
        fs::remove_all(coll_path);
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
        auto coll = p->second;
        *id = coll->last_id();
        return Status::OK;
    }

    Status collSize(int32_t mode, const char* name, size_t* size) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        auto p = m_collections.find(name);
        if(p == m_collections.end())
            return Status::NotFound;
        auto coll = p->second;
        *size = coll->size();
        return Status::OK;
    }

    Status docSize(const char* collection,
                   int32_t mode,
                   const BasicUserMem<yk_id_t>& ids,
                   BasicUserMem<size_t>& sizes) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end()) {
            for(size_t i = 0; i < ids.size; ++i) {
                sizes[i] = YOKAN_KEY_NOT_FOUND;
            }
            return Status::OK;
        }
        auto coll = p->second;
        for(size_t i = 0; i < ids.size; ++i) {
            sizes[i] = coll->entrySize(ids[i]);
        }
        return Status::OK;
    }

    Status docStore(const char* collection,
                    int32_t mode,
                    const UserMem& documents,
                    const BasicUserMem<size_t>& sizes,
                    BasicUserMem<yk_id_t>& ids) override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        auto p = m_collections.find(collection);
        if(p == m_collections.end())
            return Status::NotFound;
        auto coll = p->second;
        auto first_id = coll->appendMulti(ids.size, documents.data, sizes.data);
        for(size_t i = 0; i < ids.size; ++i) {
            ids[i] = first_id + i;
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
        auto coll = p->second;

        //ScopedWriteLock coll_lock(coll.m_lock);

        if(!(mode & YOKAN_MODE_UPDATE_NEW)) {
            for(size_t i = 0; i < ids.size; ++i) {
                if(ids[i] > coll->last_id())
                    return Status::InvalidID;
            }
        }

        const auto count = ids.size;
        const char* doc_ptr = documents.data;

        for(size_t i = 0; i < count; ++i) {
            const auto id = ids[i];
            const auto doc_size = sizes[i];

            if(id > coll->last_id()) {
                while(id != coll->last_id()) {
                    auto x = coll->append(nullptr, 0);
                    coll->erase(x);
                }
            }

            coll->update(id, doc_ptr, doc_size);

            doc_ptr += doc_size;
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
        auto coll = p->second;

        /*
        ScopedReadLock coll_lock(coll.m_lock);
        */
        const auto count = ids.size;
        char* doc_ptr = documents.data;

        if(packed) {

            size_t remaining = documents.size;
            for(size_t i = 0; i < count; ++i) {
                const auto id = ids[i];
                sizes[i] = coll->read(id, doc_ptr, remaining);
                if(sizes[i] == YOKAN_SIZE_TOO_SMALL) {
                    for(; i < count; ++i) {
                        sizes[i] = YOKAN_SIZE_TOO_SMALL;
                    }
                    continue;
                }
                if(sizes[i] <= YOKAN_LAST_VALID_SIZE) {
                    doc_ptr += sizes[i];
                    remaining -= sizes[i];
                }
            }

        } else {

            for(size_t i = 0; i < count; ++i) {
                const auto id = ids[i];
                const size_t buf_size = sizes[i];
                sizes[i] = coll->read(id, doc_ptr, buf_size);
                doc_ptr += buf_size;
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
        auto coll = p->second;

        /*
        ScopedReadLock coll_lock(coll.m_lock);
        */

        for(size_t i = 0; i < ids.size; ++i) {
            const auto id = ids[i];
            coll->fetch(id, func);
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
        auto coll = p->second;
        /*
        ScopedReadLock coll_lock(coll.m_lock);
        */
        for(size_t i = 0; i < ids.size; ++i)
            coll->erase(ids[i]);
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
        auto coll = p->second;

        /* ScopedReadLock coll_lock(coll.m_lock); */
        yk_id_t id = from_id;
        size_t i = 0;
        std::vector<char> doc_buffer;
        Status status = Status::OK;

        auto fetch_cb = [&](yk_id_t, const UserMem& doc) mutable {
            if(doc.size == YOKAN_KEY_NOT_FOUND) {
                id++;
                return Status::OK;
            }
            if(!filter->check(collection, id, doc.data, doc.size)) {
                if(filter->shouldStop(collection, doc.data, doc.size)) {
                    i = max;
                    return Status::OK;
                }
                ++id;
                return Status::OK;
            }
            auto filtered_size = filter->docSizeFrom(collection, doc.data, doc.size);
            doc_buffer.resize(filtered_size);
            filtered_size = filter->docCopy(
                collection, doc_buffer.data(), filtered_size, doc.data, doc.size);
            status = func(id, UserMem{doc_buffer.data(), filtered_size});
            if(status != Status::OK) {
                i = max;
                return Status::OK;
            }
            ++id;
            ++i;
            return Status::OK;
        };

        while(i < max && id <= coll->last_id()) {
            coll->fetch(id, fetch_cb);
            if(status != Status::OK) break;
        }

        return status;
    }

    void destroy() override {
        ScopedWriteLock lock(m_lock);
        m_collections.clear();
        fs::remove_all(m_path);
    }

#if 0
    struct LogMigrationHandle : public MigrationHandle {

        LogDatabase&  m_db;
        ScopedWriteLock m_db_lock;
        std::string     m_filename;
        int             m_fd;
        FILE*           m_file;
        bool            m_cancel = false;

        LogMigrationHandle(LogDatabase& db)
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

        ~LogMigrationHandle() {
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
#endif

    Status startMigration(std::unique_ptr<MigrationHandle>& mh) override {
#if 0
        if(m_migrated) return Status::Migrated;
        try {
            mh.reset(new LogMigrationHandle(*this));
        } catch(...) {
            return Status::IOError;
        }
        return Status::OK;
#endif
        return Status::Aborted;
    }

    ~LogDatabase() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_free(&m_lock);
    }

    private:

    LogDatabase(json cfg)
    : m_config(std::move(cfg))
    {
        if(m_config["use_lock"].get<bool>())
            ABT_rwlock_create(&m_lock);
        m_path = m_config["path"].get<std::string>();
        m_chunk_size = m_config["chunk_size"].get<size_t>();
        // TODO initialize m_collections by looking in the directory
    }

    std::unordered_map<std::string,
        std::shared_ptr<Collection>> m_collections;
    json                                        m_config;
    ABT_rwlock                                  m_lock = ABT_RWLOCK_NULL;
    std::string                                 m_path;
    size_t                                      m_chunk_size;
    std::atomic<bool>                           m_migrated{false};
};

}

YOKAN_REGISTER_BACKEND(log, yokan::LogDatabase);

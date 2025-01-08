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
#include "../common/logging.h"
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

        [[nodiscard]] Status syncMemory(void *addr, size_t size) {
            static long page_size = 0;
            if(page_size == 0) {
                // Get the system's page size
                page_size = sysconf(_SC_PAGESIZE);
                if (page_size == -1) {
                    YOKAN_LOG_ERROR(MARGO_INSTANCE_NULL,
                            "sysconf(_SC_PAGESIZE) failed: %s", strerror(errno));
                    return Status::IOError;
                }
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
                YOKAN_LOG_ERROR(MARGO_INSTANCE_NULL,
                        "msync failed: %s", strerror(errno));
                return Status::IOError;
            }

            return Status::OK;
        }

        [[nodiscard]] Status openFile() {
            // Open or create the file
            m_fd = open(m_filename.c_str(), O_RDWR | O_CREAT, 0644);
            if (m_fd < 0) {
                YOKAN_LOG_ERROR(MARGO_INSTANCE_NULL,
                        "Failed to open file %s: %s",
                        m_filename.c_str(), strerror(errno));
                return Status::IOError;
            }

            // Get size of the file
            auto size = lseek(m_fd, 0L, SEEK_END);
            if(size < 0) {
                YOKAN_LOG_ERROR(MARGO_INSTANCE_NULL,
                        "lseek failed for file %s: %s",
                        m_filename.c_str(), strerror(errno));
                close(m_fd);
                return Status::IOError;
            }
            lseek(m_fd, 0L, SEEK_SET);

            // Resize the file to the chunk size if needed
            if ((size_t)size < m_size) {
                if(ftruncate(m_fd, m_size) < 0) {
                    YOKAN_LOG_ERROR(MARGO_INSTANCE_NULL,
                            "Failed to resize file %s: %s",
                            m_filename.c_str(), strerror(errno));
                    close(m_fd);
                    return Status::IOError;
                }
            }

            // Memory-map the file
            m_data = mmap(nullptr, m_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
            if (m_data == MAP_FAILED) {
                YOKAN_LOG_ERROR(MARGO_INSTANCE_NULL,
                        "Failed to mmap file %s: %s",
                        m_filename.c_str(), strerror(errno));
                close(m_fd);
                return Status::IOError;
            }

            return Status::OK;
        }

        public:

        MemoryMappedFile(const std::string& filename, size_t size)
        : m_filename(filename)
        , m_size(size) {
            auto status = openFile();
            if(status != Status::OK)
                throw status;
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

        [[nodiscard]] Status write(size_t offset, const void* buffer, size_t size, bool do_flush = true) {
            if (offset + size > m_size)
                return Status::SizeError;
            std::memcpy(static_cast<char*>(m_data) + offset, buffer, size);
            if(do_flush) return flush(offset, size);
            return Status::OK;
        }

        [[nodiscard]] Status read(size_t offset, void* buffer, size_t size) const {
            if (offset + size > m_size)
                return Status::SizeError;
            std::memcpy(buffer, static_cast<const char*>(m_data) + offset, size);
            return Status::OK;
        }

        template<typename Function>
        [[nodiscard]] Status fetch(size_t offset, size_t size, Function&& func) {
            if (offset + size > m_size)
                return Status::SizeError;
            return func(UserMem{static_cast<char*>(m_data) + offset, size});
        }

        [[nodiscard]] Status flush(size_t offset, size_t size) {
            if (offset + size > m_size)
                return Status::SizeError;
            if (size == 0) return Status::OK;
            return syncMemory(static_cast<char*>(m_data) + offset, size);
        }

        [[nodiscard]] Status extend(size_t new_size) {
            if (new_size <= m_size) return Status::OK;
            if (m_data) munmap(m_data, m_size);
            if (m_fd >= 0) close(m_fd);
            m_size = new_size;
            return openFile();
        }

        [[nodiscard]] size_t size() const {
            return m_size;
        }

        [[nodiscard]] void* base() const {
            return m_data;
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
            uint64_t chunk     = 0;
            uint64_t offset    = 0;
            uint64_t size      = 0;
            uint64_t allocated = 0;
        };

        struct MetadataHeader {
            uint64_t coll_size     = 0;
            uint64_t next_id       = 0;
            uint64_t last_chunk_id = 0;
            uint64_t chunk_size    = 0;
        };

        static_assert(sizeof(EntryMetadata) == sizeof(MetadataHeader),
                      "EntryMetadata and MetadataHeader should have the same size");

        public:

        Collection(const std::string& name,
                   const std::string& path_prefix,
                   size_t chunk_size,
                   bool use_lock)
        : m_name{name}
        , m_path_prefix{path_prefix}
        , m_chunk_size{chunk_size} {
            // open the metadata file of the collection
            m_meta = std::make_shared<MetaFile>(
                    m_path_prefix + "/" + name + ".meta",
                    3*8*4096);
            // associate the header
            m_header = static_cast<MetadataHeader*>(m_meta->base());
            m_header->chunk_size = chunk_size;
            // open the last chunk
            auto last_chunk_id = m_header->last_chunk_id;
            m_last_chunk = std::make_shared<Chunk>(
                    m_path_prefix + "/" + name + "." + std::to_string(last_chunk_id),
                    chunk_size);
            if(use_lock)
                ABT_rwlock_create(&m_lock);
        }

        ~Collection() {
            if(m_lock != ABT_RWLOCK_NULL)
                ABT_rwlock_free(&m_lock);
        }

        [[nodiscard]] Status append(const void* data, size_t size, yk_id_t* id) {
            if(size > m_chunk_size)
                return Status::SizeError;
            ScopedWriteLock lock{m_lock};
            uint64_t next_offset;
            auto status = m_last_chunk->read(0, &next_offset, sizeof(next_offset));
            if(status != Status::OK) return status;
            auto last_chunk_id = m_header->last_chunk_id;
            if(size > m_chunk_size - next_offset) {
                last_chunk_id = m_header->last_chunk_id += 1;
                m_last_chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(last_chunk_id),
                        m_chunk_size);
                // first 8 bytes of a chunk represent the next available offset - 8
                next_offset = 8;
            }
            if(next_offset == 0) // happens when opening the very fist chunk
                next_offset = 8;
            // write the data
            status = m_last_chunk->write(next_offset, data, size);
            if(status != Status::OK) return status;
            auto new_next_offset = next_offset + size;
            // write the new offset
            status = m_last_chunk->write(0, &new_next_offset, sizeof(new_next_offset));
            if(status != Status::OK) return status;
            // write the metadata for the entry
            EntryMetadata entry{last_chunk_id, next_offset, size, size};
            size_t meta_offset = sizeof(MetadataHeader) + sizeof(EntryMetadata)*m_header->next_id;
            status = m_meta->write(meta_offset, &entry, sizeof(entry));
            if(status != Status::OK) return status;
            // update the header of the metadata file
            m_header->next_id += 1;
            m_header->coll_size += 1;
            flushHeader();
            // return the ID of the document
            *id = m_header->next_id - 1;
            return Status::OK;
        }

        [[nodiscard]] Status update(yk_id_t id, const void* data, size_t size) {
            if(size > m_chunk_size)
                return Status::SizeError;
            ScopedWriteLock lock{m_lock};
            // read the current metadata for the entry
            EntryMetadata entry;
            auto meta_offset = sizeof(MetadataHeader) + sizeof(EntryMetadata)*id;
            auto status = m_meta->read(meta_offset, &entry, sizeof(entry));
            if(status != Status::OK) return status;

            auto chunk = m_last_chunk;
            if(size <= entry.allocated) {
                // new entry is smaller than the allocated space, we can update in place
                if(entry.chunk != m_header->last_chunk_id) {
                    // not updating in the last chunk, we need to load the chunk
                    chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(entry.chunk),
                        m_chunk_size);
                }
                // update in place
                status = chunk->write(entry.offset, data, size);
                if(status != Status::OK) return status;
                // update the entry
                entry.size = size;
                status = m_meta->write(meta_offset, &entry, sizeof(entry));
                return status;
            }

            // new entry is larger than what was previously allocated,
            // we need to append to the chunk

            // first 8 bytes of a chunk represent the next offset
            uint64_t next_offset;
            status = m_last_chunk->read(0, &next_offset, sizeof(next_offset));
            if(status != Status::OK) return status;
            if(size > m_chunk_size - next_offset) {
                m_header->last_chunk_id += 1;
                m_last_chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(m_header->last_chunk_id),
                        m_chunk_size);
                chunk = m_last_chunk;
                next_offset = 8; // first 8 bytes of a chunk represent the next offset
            }
            if(next_offset == 0) // happens when opening the very fist chunk
                next_offset = 8;

            bool incr_coll_size = entry.size == YOKAN_KEY_NOT_FOUND; // previous entry was erased
            // write the new data
            status = chunk->write(next_offset, data, size);
            if(status != Status::OK) return status;
            auto new_next_offset = next_offset + size;
            // write the new offset
            status = chunk->write(0, &new_next_offset, sizeof(new_next_offset));
            if(status != Status::OK) return status;
            // write the metadata for the entry
            entry = EntryMetadata{m_header->last_chunk_id, next_offset, size, size};
            status = m_meta->write(meta_offset, &entry, sizeof(entry));
            if(status != Status::OK) return status;
            // update the header of the metadata file
            if(incr_coll_size)
                m_header->coll_size += 1;
            flushHeader();
            return Status::OK;
        }

        [[nodiscard]] Status erase(uint64_t id) {
            if(id >= m_header->next_id)
                return Status::InvalidID;
            ScopedWriteLock lock{m_lock};
            // read the metadata entry
            EntryMetadata entry;
            Status status;
            size_t meta_offset = sizeof(MetadataHeader) + sizeof(EntryMetadata)*id;
            status = m_meta->read(meta_offset, &entry, sizeof(entry));
            if(status != Status::OK) return status;
            if(entry.size == YOKAN_KEY_NOT_FOUND)
                return Status::OK;
            // update and write the metadata for the entry
            entry.chunk  = YOKAN_KEY_NOT_FOUND;
            entry.size   = YOKAN_KEY_NOT_FOUND;
            entry.offset = YOKAN_KEY_NOT_FOUND;
            status = m_meta->write(meta_offset, &entry, sizeof(entry));
            if(status != Status::OK) return status;
            // update the header of the metadata file
            m_header->coll_size -= 1;
            flushHeader();
            return Status::OK;
        }

        [[nodiscard]] Status appendMulti(
                size_t count, const void* data, const size_t* sizes,
                yk_id_t* ids) {
            //ScopedWriteLock lock{m_lock};
            // TODO optimize this function to make it update the metadata only when
            // all the documents have been stored
            size_t offset = 0;
            for(size_t i = 0; i < count; ++i) {
                auto status = append((const char*)data + offset, sizes[i], ids + i);
                if(status != Status::OK) return status;
                offset += sizes[i];
            }
            return Status::OK;
        }

        [[nodiscard]] Status read(size_t id, void* buffer, size_t* size) {
            size_t buf_size = *size;
            ScopedReadLock lock{m_lock};
            if(id >= m_header->next_id)
                return Status::InvalidID;
            EntryMetadata entry;
            Status status;
            size_t meta_offset = sizeof(MetadataHeader) + sizeof(EntryMetadata)*id;
            status = m_meta->read(meta_offset, &entry, sizeof(entry));
            if(status != Status::OK) return status;
            if(entry.size == YOKAN_KEY_NOT_FOUND)
                return Status::NotFound;
            if(buf_size < entry.size)
                return Status::SizeError;
            std::shared_ptr<Chunk> chunk;
            if(entry.chunk == m_header->last_chunk_id) {
                chunk = m_last_chunk;
            } else {
                chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(entry.chunk),
                        m_chunk_size);
            }
            status = chunk->read(entry.offset, buffer, entry.size);
            if(status != Status::OK) return status;
            *size = entry.size;
            return Status::OK;
        }

        [[nodiscard]] Status fetch(size_t entryNumber, DocFetchCallback cb) {
            ScopedReadLock lock{m_lock};
            if(entryNumber >= m_header->next_id)
                return cb(entryNumber, UserMem{nullptr, YOKAN_KEY_NOT_FOUND});
            EntryMetadata entry;
            Status status;
            auto meta_offset = sizeof(MetadataHeader) + sizeof(EntryMetadata)*entryNumber;
            status = m_meta->read(meta_offset, &entry, sizeof(entry));
            if(status == Status::NotFound || entry.size == YOKAN_KEY_NOT_FOUND)
                return cb(entryNumber, UserMem{nullptr, YOKAN_KEY_NOT_FOUND});
            std::shared_ptr<Chunk> chunk;
            if(entry.chunk == m_header->last_chunk_id) {
                chunk = m_last_chunk;
            } else {
                chunk = std::make_shared<Chunk>(
                        m_path_prefix + "/" + m_name + "." + std::to_string(entry.chunk),
                        m_chunk_size);
            }
            auto func = [entryNumber, &cb](const UserMem& doc) {
                return cb(entryNumber, doc);
            };
            return chunk->fetch(entry.offset, entry.size, func);
        }

        [[nodiscard]] Status entrySize(size_t entryNumber, size_t* size) {
            ScopedReadLock lock{m_lock};
            if(entryNumber >= m_header->next_id)
                return Status::NotFound;
            EntryMetadata entry;
            auto meta_offset = sizeof(MetadataHeader) + sizeof(EntryMetadata)*entryNumber;
            auto status = m_meta->read(meta_offset, &entry, sizeof(entry));
            if(status != Status::OK) return status;
            *size = entry.size;
            return status;
        }

        [[nodiscard]] auto last_id() const {
            ScopedReadLock lock{m_lock};
            return m_header->next_id-1;
        }

        [[nodiscard]] auto size() const {
            ScopedReadLock lock{m_lock};
            return m_header->coll_size;
        }

        private:

        std::string               m_name;
        std::string               m_path_prefix;
        size_t                    m_chunk_size;
        std::shared_ptr<MetaFile> m_meta;
        std::shared_ptr<Chunk>    m_last_chunk;
        MetadataHeader*           m_header = nullptr;
        ABT_rwlock                m_lock = ABT_RWLOCK_NULL;

        Status flushHeader() {
            return m_meta->flush(0, sizeof(*m_header));
        }
    };

    public:

    static Status create(const std::string& config, DatabaseInterface** kvs) {
        json cfg;
        try {
            cfg = json::parse(config);
            if(!cfg.is_object())
                return Status::InvalidConf;
            if(!(cfg.contains("path") && cfg["path"].is_string()))
                return Status::InvalidConf;
            if(cfg.contains("chunk_size") && !cfg["chunk_size"].is_number_unsigned())
                return Status::InvalidConf;
            if(cfg.contains("create_if_missing") && !cfg["create_if_missing"].is_boolean())
                return Status::InvalidConf;
            if(cfg.contains("error_if_exists") && !cfg["error_if_exists"].is_boolean())
                return Status::InvalidConf;
            if(cfg.contains("use_lock") && !cfg["use_lock"].is_boolean())
                return Status::InvalidConf;

            auto chunk_size = cfg.value("chunk_size", 10*1024*1024);
            cfg["chunk_size"] = chunk_size;
            auto create_if_missing = cfg.value("create_if_missing", true);
            cfg["create_if_missing"] = create_if_missing;
            auto error_if_exists = cfg.value("error_if_exists", false);
            cfg["error_if_exists"] = error_if_exists;
            auto use_lock = cfg.value("use_lock", true);
            cfg["use_lock"] = use_lock;
        } catch(...) {
            return Status::InvalidConf;
        }

        auto path = cfg["path"].get<std::string>();
        std::error_code ec;
        if(fs::is_directory(path, ec)) {
            if(cfg["error_if_exists"].get<bool>())
                return Status::Permission;
        } else {
            if(!cfg["create_if_missing"].get<bool>())
                return Status::Permission;
            fs::create_directories(path, ec);
        }

        *kvs = new LogDatabase(std::move(cfg));

        return Status::OK;
    }

    static Status recover(
            const std::string& config,
            const std::string& migrationConfig,
            const std::string& root,
            const std::list<std::string>& files, DatabaseInterface** kvs) {

        (void)migrationConfig;
        (void)files;

        json cfg = json::parse(config);
        std::string path = root.substr(0, root.find_last_of('/'));
        cfg["path"] = path;
        cfg["create_if_missing"] = false;
        cfg["error_if_exists"] = false;

        *kvs = new LogDatabase(cfg);

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
        auto coll = std::make_shared<Collection>(
            name, m_path, m_chunk_size , m_lock != ABT_RWLOCK_NULL);
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
        ScopedReadLock lock(m_lock);
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
            auto status = coll->entrySize(ids[i], &sizes[i]);
            if(status == Status::NotFound)
                sizes[i] = YOKAN_KEY_NOT_FOUND;
            else if(status != Status::OK)
                return status;
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
        return coll->appendMulti(ids.size, documents.data, sizes.data, ids.data);
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

        if(!(mode & YOKAN_MODE_UPDATE_NEW)) {
            for(size_t i = 0; i < ids.size; ++i) {
                if(ids[i] > coll->last_id())
                    return Status::InvalidID;
            }
        }

        const auto count = ids.size;
        const char* doc_ptr = documents.data;

        Status status;

        for(size_t i = 0; i < count; ++i) {
            const auto id = ids[i];
            const auto doc_size = sizes[i];

            if(id > coll->last_id() + 1) {
                while(id != coll->last_id() + 1) {
                    yk_id_t x;
                    status = coll->append(nullptr, 0, &x);
                    if(status != Status::OK) return status;
                    status = coll->erase(x);
                    if(status != Status::OK) return status;
                }
            }

            if(id == coll->last_id() + 1) {
                yk_id_t x;
                status = coll->append(doc_ptr, doc_size, &x);
            } else {
                status = coll->update(id, doc_ptr, doc_size);
            }
            if(status != Status::OK) return status;

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

        Status status;
        const auto count = ids.size;
        char* doc_ptr = documents.data;

        if(packed) {

            size_t remaining = documents.size;
            for(size_t i = 0; i < count; ++i) {
                const auto id = ids[i];
                size_t s = remaining;
                status = coll->read(id, doc_ptr, &s);
                if(status == Status::SizeError) {
                    for(; i < count; ++i) {
                        sizes[i] = YOKAN_SIZE_TOO_SMALL;
                    }
                    continue;
                } else if(status == Status::NotFound || status == Status::InvalidID) {
                    sizes[i] = YOKAN_KEY_NOT_FOUND;
                    continue;
                } else if(status != Status::OK) {
                    return status;
                }
                sizes[i] = s;
                if(s <= YOKAN_LAST_VALID_SIZE) {
                    doc_ptr += sizes[i];
                    remaining -= sizes[i];
                }
            }

        } else {

            for(size_t i = 0; i < count; ++i) {
                const auto id = ids[i];
                const size_t buf_size = sizes[i];
                size_t s = buf_size;
                status = coll->read(id, doc_ptr, &s);
                if(status == Status::OK)
                    sizes[i] = s;
                else if(status == Status::InvalidID || status == Status::NotFound)
                    sizes[i] = YOKAN_KEY_NOT_FOUND;
                else if(status == Status::SizeError)
                    sizes[i] = YOKAN_SIZE_TOO_SMALL;
                else
                    return status;

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

        for(size_t i = 0; i < ids.size; ++i) {
            const auto id = ids[i];
            (void)coll->fetch(id, func);
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
        for(size_t i = 0; i < ids.size; ++i)
            (void)coll->erase(ids[i]);
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
            (void)coll->fetch(id, fetch_cb);
            if(status != Status::OK) break;
        }

        return status;
    }

    void destroy() override {
        ScopedWriteLock lock(m_lock);
        m_collections.clear();
        fs::remove_all(m_path);
    }

    struct LogMigrationHandle : public MigrationHandle {

        LogDatabase&    m_db;
        ScopedWriteLock m_db_lock;
        bool            m_cancel = false;

        LogMigrationHandle(LogDatabase& db)
        : m_db(db)
        , m_db_lock(db.m_lock) {}

        ~LogMigrationHandle() {
            if(!m_cancel) {
                m_db_lock.unlock();
                m_db.destroy();
                m_db_lock.lock();
                m_db.m_migrated = true;
            }
        }

        std::string getRoot() const override {
            return m_db.m_path;
        }

        std::list<std::string> getFiles() const override {
            return {"/"};
        }

        void cancel() override {
            m_cancel = true;
        }
    };

    Status startMigration(std::unique_ptr<MigrationHandle>& mh) override {
        if(m_migrated) return Status::Migrated;
        try {
            mh.reset(new LogMigrationHandle(*this));
        } catch(...) {
            return Status::IOError;
        }
        return Status::OK;
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
        // lookup existing collections
        for (auto const& entry : std::filesystem::directory_iterator{m_path}) {
            if(!entry.is_regular_file())
                continue;
            auto filename = entry.path().string();
            if(filename.size() <= 5)
                continue;
            if(filename.substr(filename.size()-5) != ".meta")
                continue;
            auto name = filename.substr(filename.find_last_of('/')+1);
            name = name.substr(0, name.size()-5);
            auto coll = std::make_shared<Collection>(
                name, m_path, m_chunk_size, m_lock != ABT_RWLOCK_NULL);
            m_collections.emplace(name, coll);
        }
    }

    std::unordered_map<std::string,
        std::shared_ptr<Collection>> m_collections;
    json                             m_config;
    ABT_rwlock                       m_lock = ABT_RWLOCK_NULL;
    std::string                      m_path;
    size_t                           m_chunk_size;
    std::atomic<bool>                m_migrated{false};
};

}

YOKAN_REGISTER_BACKEND(log, yokan::LogDatabase);

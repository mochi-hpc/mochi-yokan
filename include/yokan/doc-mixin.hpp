/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_DOC_MIXIN_H
#define __YOKAN_DOC_MIXIN_H

#include <iostream>
#include <limits>
#include <yokan/backend.hpp>
#include <yokan/util/locks.hpp>
#include <memory>
#include <cstring>

namespace yokan {

/**
 * @brief This class allows any Database backend that provides key/value
 * storage functionalities to offer document storage on top. The way to
 * use it is by making your backend implementation class T inherite
 * from DocumentStoreMixin<DatabaseInterface> instead of DatabaseInterface.
 */
template <typename DB>
class DocumentStoreMixin : public DB {

    struct CollectionMetadata {
        yk_id_t size    = 0;
        yk_id_t next_id = 0;
    };

    ABT_rwlock m_lock = ABT_RWLOCK_NULL;
    mutable std::unordered_map<std::string, CollectionMetadata> m_cached_metadata;

    public:

    DocumentStoreMixin() {
        ABT_rwlock_create(&m_lock);

    }

    virtual ~DocumentStoreMixin() {
        ABT_rwlock_free(&m_lock);
    }

    DocumentStoreMixin(const DocumentStoreMixin&) = default;
    DocumentStoreMixin(DocumentStoreMixin&&) = default;
    DocumentStoreMixin& operator=(const DocumentStoreMixin&) = default;
    DocumentStoreMixin& operator=(DocumentStoreMixin&&) = default;

    using DB::isSorted;
    using DB::supportsMode;
    using DB::count;
    using DB::exists;
    using DB::length;
    using DB::put;
    using DB::get;
    using DB::erase;
    using DB::listKeys;
    using DB::listKeyValues;

    void disableDocMixinLock() {
        ABT_rwlock_free(&m_lock);
        m_lock = ABT_RWLOCK_NULL;
    }

    Status collCreate(int32_t mode, const char* name) override {
        (void)mode;
        ScopedWriteLock lock(m_lock);
        bool coll_exists;
        auto name_len = strlen(name);
        auto status = _collExists(name, name_len, &coll_exists);
        if(status != Status::OK) return status;
        if(coll_exists) return Status::KeyExists;
        CollectionMetadata metadata;
        return _collPutMetadata(name, name_len, &metadata);
    }

    Status collDrop(int32_t mode, const char* collection) override {
        (void)mode;
        ScopedWriteLock lock(m_lock);
        bool coll_exists;
        auto name_len = strlen(collection);
        auto status = _collExists(collection, name_len, &coll_exists);
        if(status != Status::OK) return status;
        CollectionMetadata metadata;
        status = _collGetMetadata(collection, name_len, &metadata);
        if(status != Status::OK) return status;
        std::vector<yk_id_t> ids(metadata.next_id);
        for(yk_id_t id = 0; id < ids.size(); id++) {
            ids[id] = id;
        }
        auto keys = _keysFromIds(collection, name_len, ids);
        std::vector<size_t> ksizes(ids.size()+1, name_len+1+sizeof(yk_id_t));
        keys.resize(keys.size() + name_len);
        std::memcpy(keys.data() + keys.size() - name_len, collection, name_len);
        ksizes[ksizes.size()-1] = name_len;
        m_cached_metadata.erase(std::string(collection, name_len));
        return erase(mode, keys, ksizes);
    }

    Status collExists(int32_t mode, const char* collection, bool* flag) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        return _collExists(collection, strlen(collection), flag);
    }

    Status collLastID(int32_t mode, const char* collection, yk_id_t* id) const override {
        (void)mode;
        CollectionMetadata metadata;
        ScopedReadLock lock(m_lock);
        auto status = _collGetMetadata(collection, strlen(collection), &metadata);
        if(status == Status::OK)
            *id = metadata.next_id-1;
        return status;
    }

    Status collSize(int32_t mode, const char* collection, size_t* size) const override {
        (void)mode;
        ScopedReadLock lock(m_lock);
        CollectionMetadata metadata;
        auto status = _collGetMetadata(collection, strlen(collection), &metadata);
        if(status == Status::OK)
            *size = metadata.size;
        return status;
    }

    Status docSize(const char* collection,
                   int32_t mode,
                   const BasicUserMem<yk_id_t>& ids,
                   BasicUserMem<size_t>& sizes) const override {
        if(collection == nullptr || collection[0] == 0)
            return Status::InvalidArg;
        auto name_len = strlen(collection);
        auto keys = _keysFromIds(collection, name_len, ids);
        std::vector<size_t> ksizes(ids.size, name_len+1+sizeof(yk_id_t));
        return length(mode, keys, ksizes, sizes);
    }

    Status docStore(const char* collection,
                    int32_t mode,
                    const UserMem& documents,
                    const BasicUserMem<size_t>& sizes,
                    BasicUserMem<yk_id_t>& ids) override {
        auto count = ids.size;
        CollectionMetadata metadata;
        auto name_len = strlen(collection);
        {
            ScopedWriteLock lock(m_lock);
            auto status = _collGetMetadata(collection, name_len, &metadata);
            if(status != Status::OK) return status;
            for(uint64_t i = 0; i < count; i++) {
                ids[i] = metadata.next_id + i;
            }
            metadata.next_id += count;
            status = _collPutMetadata(collection, name_len, &metadata, false);
            if(status != Status::OK) return status;
        }
        auto keys = _keysFromIds(collection, name_len, ids);
        std::vector<size_t> ksizes(ids.size, name_len+1+sizeof(yk_id_t));
        auto status = put(mode, keys, ksizes, documents, sizes);
        if(status != Status::OK) return status;
        ScopedWriteLock lock(m_lock);
        _collGetMetadata(collection, name_len, &metadata);
        metadata.size += count;
        return _collPutMetadata(collection, name_len, &metadata);
    }

    Status docUpdate(const char* collection,
                     int32_t mode,
                     const BasicUserMem<yk_id_t>& ids,
                     const UserMem& documents,
                     const BasicUserMem<size_t>& sizes) override {
        if(collection == nullptr || collection[0] == 0)
            return Status::InvalidArg;
        auto name_len = strlen(collection);
        auto keys = _keysFromIds(collection, name_len, ids);
        std::vector<size_t> ksizes(ids.size, name_len+1+sizeof(yk_id_t));
        ScopedWriteLock lock(m_lock);
        CollectionMetadata metadata;
        auto status = _collGetMetadata(collection, name_len, &metadata);
        if(status != Status::OK) return status;
        if(mode & YOKAN_MODE_UPDATE_NEW) {
            std::vector<uint8_t> existsBuffer(1 + sizes.size/8);
            BitField existsBitfield{existsBuffer.data(), sizes.size};
            status = exists(mode, keys, ksizes, existsBitfield);
            if(status != Status::OK) return status;
            size_t extraKeys = 0;
            yk_id_t maxID = 0;
            for(unsigned i=0; i < ids.size; i++) {
                if(!existsBitfield[i]) extraKeys += 1;
                maxID = std::max(maxID, ids[i]);
            }
            status = put(mode, keys, ksizes, documents, sizes);
            if(status != Status::OK) return status;
            metadata.size += extraKeys;
            metadata.next_id = maxID + 1;
            return _collPutMetadata(collection, name_len, &metadata);
        } else {
            for(unsigned i=0; i < ids.size; i++) {
                if(ids[i] >= metadata.next_id)
                    return Status::InvalidID;
            }
            // FIXME: we may be updating keys that have been previously deleted,
            // leading the metadata to no longer be correct.
            return put(mode, keys, ksizes, documents, sizes);
        }
    }

    Status docLoad(const char* collection,
                   int32_t mode, bool packed,
                   const BasicUserMem<yk_id_t>& ids,
                   UserMem& documents,
                   BasicUserMem<size_t>& sizes) override {
        if(collection == nullptr || collection[0] == 0)
            return Status::InvalidArg;
        auto name_len = strlen(collection);
        ScopedReadLock lock(m_lock);
        bool e;
        auto status = _collExists(collection, name_len, &e);
        if(status != Status::OK) return status;
        if(!e) return Status::NotFound;
        auto keys = _keysFromIds(collection, name_len, ids);
        std::vector<size_t> ksizes(ids.size, name_len+1+sizeof(yk_id_t));
        return get(mode, packed, keys, ksizes, documents, sizes);
    }

    Status docFetch(const char* collection,
                    int32_t mode,
                    const BasicUserMem<yk_id_t>& ids,
                    const DatabaseInterface::DocFetchCallback& func) override {
        if(collection == nullptr || collection[0] == 0)
              return Status::InvalidArg;
        auto name_len = strlen(collection);
        ScopedReadLock lock(m_lock);
        bool e;
        auto status = _collExists(collection, name_len, &e);
        if(status != Status::OK) return status;
        auto keys = _keysFromIds(collection, name_len, ids);
        std::vector<size_t> ksizes(ids.size, name_len+1+sizeof(yk_id_t));
        return this->fetch(mode, keys, ksizes,
            [&func, name_len](const UserMem& key, const UserMem& val) -> Status {
                yk_id_t id = _idFromKey(name_len, key.data);
                return func(id, val);
            });
    }

    Status docErase(const char* collection,
                    int32_t mode,
                    const BasicUserMem<yk_id_t>& ids) override {
        if(collection == nullptr || collection[0] == 0)
            return Status::InvalidArg;
        CollectionMetadata metadata;
        auto name_len = strlen(collection);
        auto keys = _keysFromIds(collection, name_len, ids);
        std::vector<size_t> ksizes(ids.size, name_len+1+sizeof(yk_id_t));
        std::vector<uint8_t> docs_exist(1+ids.size/8);
        auto docs_exist_bf = BitField{docs_exist.data(), ids.size};
        ScopedWriteLock lock(m_lock);
        auto status = exists(mode, keys, ksizes, docs_exist_bf);
        if(status != Status::OK) return status;
        size_t num_keys_to_erase = 0;
        for(size_t i=0; i < ids.size; i++) {
            if(docs_exist_bf[i]) num_keys_to_erase += 1;
        }
        status = _collGetMetadata(collection, name_len, &metadata);
        if(status != Status::OK) return status;
        status = erase(mode, keys, ksizes);
        if(status != Status::OK) return status;
        metadata.size -= num_keys_to_erase;
        return _collPutMetadata(collection, name_len, &metadata);
    }

    Status docList(const char* collection,
                   int32_t mode, bool packed,
                   yk_id_t from_id,
                   const std::shared_ptr<DocFilter>& filter,
                   BasicUserMem<yk_id_t>& ids,
                   UserMem& documents,
                   BasicUserMem<size_t>& docSizes) const override {
        if(collection == nullptr || collection[0] == 0)
            return Status::InvalidArg;
        if(!supportsMode(YOKAN_MODE_NO_PREFIX))
            return Status::NotSupported;

        auto status = Status::OK;

        auto name_len = strlen(collection);
        auto count = ids.size;
        auto kv_filter = FilterFactory::docToKeyValueFilter(filter, collection);

        CollectionMetadata metadata;
        {
            ScopedReadLock lock(m_lock);
            status = _collGetMetadata(collection, name_len, &metadata);
            if(status != Status::OK) {
                return status;
            }
        }

        if(isSorted()) { // use the underlying listKeyValues function

            auto first_key = _keyFromId(collection, name_len, from_id);

            auto keys_umem = UserMem{ reinterpret_cast<char*>(ids.data), ids.size*sizeof(yk_id_t) };
            std::vector<size_t> ksizes(count, sizeof(yk_id_t));
            auto ksizes_umem = BasicUserMem<size_t> {ksizes};

            status = listKeyValues(YOKAN_MODE_INCLUSIVE|YOKAN_MODE_NO_PREFIX,
                             packed, first_key, kv_filter,
                             keys_umem, ksizes_umem, documents, docSizes);
            if(status == Status::OK) {
                for(size_t i=0; i < count; i++) {
                    if(ksizes[i] == YOKAN_NO_MORE_KEYS)
                        ids[i] = YOKAN_NO_MORE_DOCS;
                    else
                        ids[i] = _ensureBigEndian(ids[i]);
                }
            }
        } else {

            yk_id_t id = from_id;
            size_t docs_offset = 0;
            size_t i = 0;
            while(i != count && id < metadata.next_id && docs_offset < documents.size) {
                auto key = _keyFromId(collection, name_len, id);
                auto ksize = key.size();
                auto doc_umem = UserMem{
                    documents.data + docs_offset,
                    packed ? (documents.size - docs_offset) : docSizes[i]
                };
                auto docsize_umem = BasicUserMem<size_t>{docSizes.data + i, 1};
                auto original_vsize = docsize_umem[0];
                status = const_cast<DocumentStoreMixin<DB>*>(this)->get(
                        mode, true,  key, BasicUserMem<size_t>{&ksize, 1}, doc_umem, docsize_umem);
                if(status != Status::OK) {
                    return status;
                }
                if(docSizes[i] >= YOKAN_SIZE_TOO_SMALL) break;

                if(!kv_filter->check(key.data(), ksize, doc_umem.data, docsize_umem[0])) {
                    docsize_umem[0] = original_vsize;
                    id += 1;
                    continue;
                }

                ids[i] = id;
                docs_offset += packed ? docsize_umem[0] : original_vsize;
                i += 1;
                id += 1;
            }
            for(; i < count; i++) {
                ids[i] = YOKAN_NO_MORE_DOCS;
                docSizes[i] = YOKAN_NO_MORE_DOCS;
            }
        }

        return status;
    }

    Status docIter(const char* collection, int32_t mode, uint64_t max,
                   yk_id_t from_id, const std::shared_ptr<DocFilter>& filter,
                   const DatabaseInterface::DocIterCallback& func) const override {
        if(collection == nullptr || collection[0] == 0)
            return Status::InvalidArg;

        auto status = Status::OK;

        auto name_len = strlen(collection);
        auto kv_filter = FilterFactory::docToKeyValueFilter(filter, collection);

        CollectionMetadata metadata;
        {
            ScopedReadLock lock(m_lock);
            status = _collGetMetadata(collection, name_len, &metadata);
            if(status != Status::OK) {
                return status;
            }
        }

        if(isSorted()) { // use the underlying iters function

            auto first_key = _keyFromId(collection, name_len, from_id);
            auto kv_func   = [&func, name_len](const UserMem& key, const UserMem& val) -> Status {
                yk_id_t id = _idFromKey(name_len, key.data);
                return func(id, val);
            };

            return this->iter(YOKAN_MODE_INCLUSIVE, max, first_key, kv_filter, false, kv_func);

        } else { // use the underlying fetch function

            yk_id_t id = from_id;
            size_t i = 0;
            while((max == 0 || i < max) && id < metadata.next_id) {
                auto key = _keyFromId(collection, name_len, id);
                auto ksize = key.size();
                auto kv_func   = [&func, &filter, &i, &collection, name_len](const UserMem& key, const UserMem& val) -> Status {
                    if(val.size == YOKAN_KEY_NOT_FOUND) return Status::OK;
                    yk_id_t id = _idFromKey(name_len, key.data);
                    if(!filter->check(collection, id, val.data, val.size)) return Status::OK;
                    else {
                        ++i;
                        return func(id, val);
                    }
                };
                status = const_cast<DocumentStoreMixin<DB>*>(this)->fetch(
                        mode, key, BasicUserMem<size_t>{&ksize, 1}, kv_func);
                if(status != Status::OK) {
                    return status;
                }
                id += 1;
            }
        }

        return Status::OK;
    }

    private:

    Status _collExists(const char* name, size_t name_size, bool* e) const {
        if(name == nullptr || name[0] == '\0')
            return Status::InvalidArg;
        size_t klen = name_size;
        size_t vlen;
        UserMem key_umem{const_cast<char*>(name), klen};
        BasicUserMem<size_t> ksize_umem{&klen, 1};
        BasicUserMem<size_t> vsize_umem{&vlen, 1};
        auto stt = length(0, key_umem, ksize_umem, vsize_umem);
        if(stt == Status::OK)
            *e = (vlen == sizeof(CollectionMetadata));
        return stt;
    }

    Status _collGetMetadata(const char* name, size_t name_size, CollectionMetadata* metadata,
                            bool from_disk=false) const {
        if(name == nullptr || name[0] == '\0')
            return Status::InvalidArg;
        auto coll_name = std::string(name, name_size);
        auto it = m_cached_metadata.find(coll_name);
        if(it != m_cached_metadata.end() && !from_disk) {
            *metadata = it->second;
            return Status::OK;
        }
        size_t klen = name_size;
        size_t vlen = sizeof(*metadata);
        UserMem key_umem{const_cast<char*>(name), klen};
        BasicUserMem<size_t> ksize_umem{&klen, 1};
        UserMem val_umem{reinterpret_cast<char*>(metadata), vlen};
        BasicUserMem<size_t> vsize_umem{&vlen, 1};
        auto status = const_cast<DocumentStoreMixin*>(this)->get(0, true, key_umem, ksize_umem, val_umem, vsize_umem);
        if(status != Status::OK) return status;
        if(vlen == YOKAN_KEY_NOT_FOUND) return Status::NotFound;
        if(vlen != sizeof(*metadata)) return Status::Corruption;
        if(it != m_cached_metadata.end())
            it->second = *metadata;
        else
            m_cached_metadata[coll_name] = *metadata;
        return Status::OK;
    }

    Status _collPutMetadata(const char* name, size_t name_size, CollectionMetadata* metadata, bool flush_to_disk=true) {
        if(name == nullptr || name[0] == '\0')
            return Status::InvalidArg;
        if(flush_to_disk) {
            size_t klen = name_size;
            size_t vlen = sizeof(*metadata);
            UserMem key_umem{const_cast<char*>(name), klen};
            BasicUserMem<size_t> ksize_umem{&klen, 1};
            UserMem val_umem{reinterpret_cast<char*>(metadata), vlen};
            BasicUserMem<size_t> vsize_umem{&vlen, 1};
            auto ret = put(0, key_umem, ksize_umem, val_umem, vsize_umem);
            if(ret != Status::OK)
                return ret;
        }
        auto coll_name = std::string(name, name_size);
        m_cached_metadata[coll_name] = *metadata;
        return Status::OK;
    }

    static std::vector<char> _keysFromIds(const char* name, size_t name_size, const BasicUserMem<yk_id_t>& ids) {
        auto count = ids.size;
        auto len = name_size;
        std::vector<char> buffer(count*(len+1+sizeof(yk_id_t)));
        for(size_t i = 0; i < count; i++) {
            auto offset = i*(len+1+sizeof(yk_id_t));
            std::memcpy(buffer.data()+offset, name, len+1);
            offset += len+1;
            auto be_id = _ensureBigEndian(ids[i]);
            std::memcpy(buffer.data()+offset, &be_id, sizeof(be_id));
        }
        return buffer;
    }

    static yk_id_t _idFromKey(size_t coll_name_len, const char* key) {
        yk_id_t be_id;
        std::memcpy(&be_id, key+coll_name_len+1, sizeof(yk_id_t));
        return _ensureBigEndian(be_id);
    }

    static std::vector<char> _keyFromId(const char* name, size_t name_len, yk_id_t id) {
        std::vector<char> buffer(name_len+1+sizeof(yk_id_t));
        auto be_id = _ensureBigEndian(id);
        std::memcpy(buffer.data(), name, name_len+1);
        std::memcpy(buffer.data()+name_len+1, &be_id, sizeof(be_id));
        return buffer;
    }

    static yk_id_t _ensureBigEndian(yk_id_t id) {
        #define IS_BIG_ENDIAN (*(uint16_t *)"\0\xff" < 0x100)
        if(IS_BIG_ENDIAN) {
            return id;
        } else {
            uint64_t x = id;
            x = (x & 0x00000000FFFFFFFF) << 32 | (x & 0xFFFFFFFF00000000) >> 32;
            x = (x & 0x0000FFFF0000FFFF) << 16 | (x & 0xFFFF0000FFFF0000) >> 16;
            x = (x & 0x00FF00FF00FF00FF) << 8  | (x & 0xFF00FF00FF00FF00) >> 8;
            return x;
        }
    }

};

}
#endif

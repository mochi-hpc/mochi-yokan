/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_COLLECTION_HPP
#define __YOKAN_COLLECTION_HPP

#include <yokan/cxx/database.hpp>
#include <string>

namespace yokan {

class Collection {

    public:

    template<typename ... Args>
    Collection(const char* name, Args&&... args)
    : m_db(std::forward<Args>(args)...)
    , m_name(name) {}

    Collection(const Collection&) = default;
    Collection(Collection&&) = default;
    Collection& operator=(const Collection&) = default;
    Collection& operator=(Collection&&) = default;
    ~Collection() = default;

    size_t size(int32_t mode = YOKAN_MODE_DEFAULT) const {
        size_t s;
        auto err = yk_collection_size(m_db.handle(), m_name.c_str(), mode, &s);
        YOKAN_CONVERT_AND_THROW(err);
        return s;
    }

    yk_id_t last_id(int32_t mode = YOKAN_MODE_DEFAULT) const {
        yk_id_t last;
        auto err = yk_collection_last_id(m_db.handle(), m_name.c_str(),
                                         mode, &last);
        YOKAN_CONVERT_AND_THROW(err);
        return last;
    }

    yk_id_t store(const void* doc, size_t docsize,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        yk_id_t id;
        auto err = yk_doc_store(m_db.handle(), m_name.c_str(),
                                mode, doc, docsize, &id);
        YOKAN_CONVERT_AND_THROW(err);
        return id;
    }

    void storeMulti(size_t count, const void* const* documents,
                    const size_t* docsizes, yk_id_t* ids,
                    int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_store_multi(m_db.handle(), m_name.c_str(),
                                      mode, count, documents,
                                      docsizes, ids);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void storePacked(size_t count, const void* documents,
                     const size_t* docsizes, yk_id_t* ids,
                     int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_store_packed(m_db.handle(), m_name.c_str(),
                                       mode, count, documents,
                                       docsizes, ids);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void storeBulk(size_t count, hg_bulk_t data,
                   size_t offset, size_t size,
                   yk_id_t* ids,
                   const char* origin = nullptr,
                   int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_store_bulk(m_db.handle(), m_name.c_str(),
                                     mode, count, origin, data,
                                     offset, size, ids);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void load(yk_id_t id, void* data, size_t* size,
              int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_load(m_db.handle(), m_name.c_str(),
                               mode, id, data, size);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void loadMulti(size_t count,
                   const yk_id_t* ids,
                   void* const* documents,
                   size_t* docsizes,
                   int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_load_multi(m_db.handle(), m_name.c_str(),
                                     mode, count, ids, documents, docsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void loadPacked(size_t count,
                    const yk_id_t* ids,
                    size_t bufsize,
                    void* documents,
                    size_t* docsizes,
                    int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_load_packed(m_db.handle(), m_name.c_str(),
                                      mode, count, ids, bufsize,
                                      documents, docsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void loadBulk(size_t count,
                  const yk_id_t* ids,
                  hg_bulk_t data,
                  size_t offset,
                  size_t size,
                  bool packed,
                  const char* origin = nullptr,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_load_bulk(m_db.handle(), m_name.c_str(),
                                    mode, count, ids, origin,
                                    data, offset, size, packed);
        YOKAN_CONVERT_AND_THROW(err);
    }

    size_t length(yk_id_t id,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        size_t size;
        auto err = yk_doc_length(m_db.handle(), m_name.c_str(),
                                 mode, id, &size);
        YOKAN_CONVERT_AND_THROW(err);
        return size;
    }

    void lengthMulti(size_t count, const yk_id_t* ids,
                     size_t* sizes, int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_length_multi(m_db.handle(), m_name.c_str(),
                                       mode, count, ids, sizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void update(yk_id_t id, const void* document, size_t docsize,
                   int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_update(m_db.handle(), m_name.c_str(),
                                 mode, id, document, docsize);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void updateMulti(size_t count,
                     const yk_id_t* ids,
                     const void* const* documents,
                     const size_t* docsizes,
                     int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_update_multi(m_db.handle(), m_name.c_str(),
                                       mode, count, ids,
                                       documents, docsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void updatePacked(size_t count,
                      const yk_id_t* ids,
                      const void* documents,
                      const size_t* docsizes,
                      int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_update_packed(m_db.handle(), m_name.c_str(),
                                        mode, count, ids,
                                        documents, docsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void updateBulk(size_t count,
                    const yk_id_t* ids,
                    hg_bulk_t data,
                    size_t offset,
                    size_t size,
                    const char* origin = nullptr,
                    int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_update_bulk(m_db.handle(), m_name.c_str(),
                                      mode, count, ids, origin,
                                      data, offset, size);
        YOKAN_CONVERT_AND_THROW(err);
    }

    auto handle() const {
        return m_db.handle();
    }

    void erase(yk_id_t id, int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_erase(m_db.handle(), m_name.c_str(), mode, id);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void eraseMulti(size_t count,
                    const yk_id_t* ids,
                    int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_erase_multi(m_db.handle(), m_name.c_str(),
                                      mode, count, ids);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void list(yk_id_t start_id,
              const void* filter,
              size_t filter_size,
              size_t max,
              yk_id_t* ids,
              void* const* docs,
              size_t* doc_sizes,
              int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_list(m_db.handle(), m_name.c_str(),
                               mode, start_id, filter, filter_size,
                               max, ids, docs, doc_sizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void listPacked(yk_id_t start_id,
                    const void* filter,
                    size_t filter_size,
                    size_t max,
                    yk_id_t* ids,
                    size_t bufsize,
                    void* docs,
                    size_t* doc_sizes,
                    int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_list_packed(m_db.handle(), m_name.c_str(),
                               mode, start_id, filter, filter_size,
                               max, ids, bufsize, docs, doc_sizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void listBulk(yk_id_t from_id,
                  size_t filter_size,
                  hg_bulk_t data,
                  size_t offset,
                  size_t docs_buf_size,
                  bool packed,
                  size_t count,
                  const char* origin = nullptr,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_doc_list_bulk(m_db.handle(), m_name.c_str(),
                                    mode, from_id, filter_size,
                                    origin, data, offset,
                                    docs_buf_size,
                                    packed, count);
        YOKAN_CONVERT_AND_THROW(err);
    }

    private:

    Database    m_db;
    std::string m_name;

};

}

#endif

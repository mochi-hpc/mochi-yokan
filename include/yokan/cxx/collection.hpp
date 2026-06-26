/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_COLLECTION_HPP
#define __YOKAN_COLLECTION_HPP

#include <yokan/cxx/database.hpp>
#include <string>
#include <utility>

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

    template <typename... Extras>
    size_t size(int32_t mode = YOKAN_MODE_DEFAULT, Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        size_t s;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_collection_size(m_db.handle(), m_name.c_str(), mode, &s);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_collection_size(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, &s,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return s;
    }

    template <typename... Extras>
    yk_id_t last_id(int32_t mode = YOKAN_MODE_DEFAULT, Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_id_t last;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_collection_last_id(m_db.handle(), m_name.c_str(),
                                        mode, &last);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_collection_last_id(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, &last,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return last;
    }

    template <typename... Extras>
    yk_id_t store(const void* doc, size_t docsize,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_id_t id;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_store(m_db.handle(), m_name.c_str(),
                               mode, doc, docsize, &id);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_store(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, doc, docsize, &id,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return id;
    }

    template <typename... Extras>
    void storeMulti(size_t count, const void* const* documents,
                    const size_t* docsizes, yk_id_t* ids,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_store_multi(m_db.handle(), m_name.c_str(),
                                     mode, count, documents,
                                     docsizes, ids);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_store_multi(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, documents, docsizes, ids,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void storePacked(size_t count, const void* documents,
                     const size_t* docsizes, yk_id_t* ids,
                     int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_store_packed(m_db.handle(), m_name.c_str(),
                                      mode, count, documents,
                                      docsizes, ids);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_store_packed(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, documents, docsizes, ids,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void storeBulk(size_t count, hg_bulk_t data,
                   size_t offset, size_t size,
                   yk_id_t* ids,
                   const char* origin = nullptr,
                   int32_t mode = YOKAN_MODE_DEFAULT,
                   Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_store_bulk(m_db.handle(), m_name.c_str(),
                                    mode, count, origin, data,
                                    offset, size, ids);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_store_bulk(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, origin, data,
                offset, size, ids,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void load(yk_id_t id, void* data, size_t* size,
              int32_t mode = YOKAN_MODE_DEFAULT,
              Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_load(m_db.handle(), m_name.c_str(),
                              mode, id, data, size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_load(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, id, data, size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void loadMulti(size_t count,
                   const yk_id_t* ids,
                   void* const* documents,
                   size_t* docsizes,
                   int32_t mode = YOKAN_MODE_DEFAULT,
                   Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_load_multi(m_db.handle(), m_name.c_str(),
                                    mode, count, ids, documents, docsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_load_multi(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids, documents, docsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void loadPacked(size_t count,
                    const yk_id_t* ids,
                    size_t bufsize,
                    void* documents,
                    size_t* docsizes,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_load_packed(m_db.handle(), m_name.c_str(),
                                     mode, count, ids, bufsize,
                                     documents, docsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_load_packed(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids, bufsize,
                documents, docsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void loadBulk(size_t count,
                  const yk_id_t* ids,
                  hg_bulk_t data,
                  size_t offset,
                  size_t size,
                  bool packed,
                  const char* origin = nullptr,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_load_bulk(m_db.handle(), m_name.c_str(),
                                   mode, count, ids, origin,
                                   data, offset, size, packed);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_load_bulk(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids, origin,
                data, offset, size, packed,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void fetch(yk_id_t id,
               yk_document_callback_t cb,
               void* uargs,
               int32_t mode = YOKAN_MODE_DEFAULT,
               Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_fetch(m_db.handle(), m_name.c_str(), mode, id, cb, uargs);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_fetch(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, id, cb, uargs,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    using fetch_callback_type =
        std::function<yk_return_t(size_t,yk_id_t,const void*,size_t)>;

    static yk_return_t _fetch_dispatch(void* uargs, size_t index, yk_id_t id,
                                       const void* val, size_t vsize) {
        const fetch_callback_type* cb_ptr =
            static_cast<const fetch_callback_type*>(uargs);
        return (*cb_ptr)(index, id, val, vsize);
    }

    template <typename... Extras>
    void fetch(yk_id_t id,
               const fetch_callback_type& cb,
               int32_t mode = YOKAN_MODE_DEFAULT,
               Extras&&... extras) const {
        fetch(id, _fetch_dispatch, (void*)&cb, mode,
              std::forward<Extras>(extras)...);
    }

    template <typename... Extras>
    void fetchMulti(size_t count,
                    const yk_id_t* ids,
                    yk_document_callback_t cb,
                    void* uargs,
                    const yk_doc_fetch_options_t* options = nullptr,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_fetch_multi(
                    m_db.handle(), m_name.c_str(),
                    mode, count, ids, cb, uargs, options);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_fetch_multi(
                    m_db.handle(), m_name.c_str(),
                    mode | YOKAN_MODE_EXTRA, count, ids, cb, uargs, options,
                    YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                    YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void fetchMulti(size_t count,
                    const yk_id_t* ids,
                    const fetch_callback_type& cb,
                    const yk_doc_fetch_options_t* options = nullptr,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        fetchMulti(count, ids, _fetch_dispatch, (void*)&cb, options, mode,
                   std::forward<Extras>(extras)...);
    }

    template <typename... Extras>
    size_t length(yk_id_t id,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        size_t size;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_length(m_db.handle(), m_name.c_str(),
                                mode, id, &size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_length(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, id, &size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return size;
    }

    template <typename... Extras>
    void lengthMulti(size_t count, const yk_id_t* ids,
                     size_t* sizes, int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_length_multi(m_db.handle(), m_name.c_str(),
                                      mode, count, ids, sizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_length_multi(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids, sizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void update(yk_id_t id, const void* document, size_t docsize,
                int32_t mode = YOKAN_MODE_DEFAULT,
                Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_update(m_db.handle(), m_name.c_str(),
                                mode, id, document, docsize);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_update(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, id, document, docsize,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void updateMulti(size_t count,
                     const yk_id_t* ids,
                     const void* const* documents,
                     const size_t* docsizes,
                     int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_update_multi(m_db.handle(), m_name.c_str(),
                                      mode, count, ids,
                                      documents, docsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_update_multi(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids,
                documents, docsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void updatePacked(size_t count,
                      const yk_id_t* ids,
                      const void* documents,
                      const size_t* docsizes,
                      int32_t mode = YOKAN_MODE_DEFAULT,
                      Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_update_packed(m_db.handle(), m_name.c_str(),
                                       mode, count, ids,
                                       documents, docsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_update_packed(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids,
                documents, docsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void updateBulk(size_t count,
                    const yk_id_t* ids,
                    hg_bulk_t data,
                    size_t offset,
                    size_t size,
                    const char* origin = nullptr,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_update_bulk(m_db.handle(), m_name.c_str(),
                                     mode, count, ids, origin,
                                     data, offset, size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_update_bulk(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids, origin,
                data, offset, size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    auto handle() const {
        return m_db.handle();
    }

    template <typename... Extras>
    void erase(yk_id_t id, int32_t mode = YOKAN_MODE_DEFAULT,
               Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_erase(m_db.handle(), m_name.c_str(), mode, id);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_erase(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, id,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void eraseMulti(size_t count,
                    const yk_id_t* ids,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_erase_multi(m_db.handle(), m_name.c_str(),
                                     mode, count, ids);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_erase_multi(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, count, ids,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void list(yk_id_t start_id,
              const void* filter,
              size_t filter_size,
              size_t max,
              yk_id_t* ids,
              void* const* docs,
              size_t* doc_sizes,
              int32_t mode = YOKAN_MODE_DEFAULT,
              Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_list(m_db.handle(), m_name.c_str(),
                              mode, start_id, filter, filter_size,
                              max, ids, docs, doc_sizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_list(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, start_id, filter, filter_size,
                max, ids, docs, doc_sizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listPacked(yk_id_t start_id,
                    const void* filter,
                    size_t filter_size,
                    size_t max,
                    yk_id_t* ids,
                    size_t bufsize,
                    void* docs,
                    size_t* doc_sizes,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_list_packed(m_db.handle(), m_name.c_str(),
                                     mode, start_id, filter, filter_size,
                                     max, ids, bufsize, docs, doc_sizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_list_packed(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, start_id, filter, filter_size,
                max, ids, bufsize, docs, doc_sizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listBulk(yk_id_t from_id,
                  size_t filter_size,
                  hg_bulk_t data,
                  size_t offset,
                  size_t docs_buf_size,
                  bool packed,
                  size_t count,
                  const char* origin = nullptr,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_list_bulk(m_db.handle(), m_name.c_str(),
                                   mode, from_id, filter_size,
                                   origin, data, offset,
                                   docs_buf_size,
                                   packed, count);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_list_bulk(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, from_id, filter_size,
                origin, data, offset, docs_buf_size,
                packed, count,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void iter(yk_id_t from_id,
              const void* filter,
              size_t filter_size,
              size_t max,
              yk_document_callback_t cb,
              void* uargs,
              const yk_doc_iter_options_t* options = nullptr,
              int32_t mode = YOKAN_MODE_DEFAULT,
              Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_doc_iter(m_db.handle(), m_name.c_str(),
                              mode, from_id, filter, filter_size,
                              max, cb, uargs, options);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_doc_iter(m_db.handle(), m_name.c_str(),
                mode | YOKAN_MODE_EXTRA, from_id, filter, filter_size,
                max, cb, uargs, options,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    using iter_callback_type =
        std::function<yk_return_t(size_t,yk_id_t,const void*,size_t)>;

    static yk_return_t _iter_dispatch(void* uargs, size_t index, yk_id_t id,
                                       const void* val, size_t vsize) {
        const iter_callback_type* cb_ptr =
            static_cast<const iter_callback_type*>(uargs);
        return (*cb_ptr)(index, id, val, vsize);
    }

    template <typename... Extras>
    void iter(yk_id_t from_id,
              const void* filter,
              size_t filter_size,
              size_t max,
              const iter_callback_type& cb,
              const yk_doc_iter_options_t* options = nullptr,
              int32_t mode = YOKAN_MODE_DEFAULT,
              Extras&&... extras) const {
        iter(from_id, filter, filter_size, max,
             _iter_dispatch, (void*)&cb, options, mode,
             std::forward<Extras>(extras)...);
    }

    private:

    Database    m_db;
    std::string m_name;

};

}

#endif

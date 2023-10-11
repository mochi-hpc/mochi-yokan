#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <yokan/cxx/client.hpp>
#include <yokan/cxx/database.hpp>
#include <yokan/cxx/collection.hpp>
#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id")
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t")


static auto get_buffer_info(const py::buffer& buf) {
    return buf.request();
}

static auto get_buffer_info(const std::string& str) {
    return py::buffer_info{ str.data(), (ssize_t)str.size(), false };
}

#define CHECK_BUFFER_IS_CONTIGUOUS(__buf_info__) do { \
    ssize_t __stride__ = (__buf_info__).itemsize;     \
    for(ssize_t i=0; i < (__buf_info__).ndim; i++) {  \
        if(__stride__ != (__buf_info__).strides[i])   \
            throw yokan::Exception(YOKAN_ERR_NONCONTIG);  \
        __stride__ *= (__buf_info__).shape[i];        \
    }                                                 \
} while(0)

#define CHECK_BUFFER_IS_WRITABLE(__buf_info__) do { \
    if((__buf_info__).readonly)                     \
        throw yokan::Exception(YOKAN_ERR_READONLY);     \
} while(0)

template <typename KeyType, typename ValueType>
static void put_helper(const yokan::Database& db, const KeyType& key,
                       const ValueType& val, int32_t mode) {
    auto key_info = get_buffer_info(key);
    auto val_info = get_buffer_info(val);
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    CHECK_BUFFER_IS_CONTIGUOUS(val_info);
    db.put(key_info.ptr,
            key_info.itemsize*key_info.size,
            val_info.ptr,
            val_info.itemsize*val_info.size,
            mode);
}

template <typename KeyType, typename ValueType>
static void put_multi_helper(const yokan::Database& db,
                             const std::vector<std::pair<KeyType,ValueType>>& keyvals,
                             int32_t mode) {
    auto count = keyvals.size();
    std::vector<const void*> keys(count);
    std::vector<const void*> vals(count);
    std::vector<size_t> key_sizes(count);
    std::vector<size_t> val_sizes(count);
    for(size_t i = 0; i < count; i++) {
        auto key_info = get_buffer_info(keyvals[i].first);
        auto val_info = get_buffer_info(keyvals[i].second);
        CHECK_BUFFER_IS_CONTIGUOUS(key_info);
        CHECK_BUFFER_IS_CONTIGUOUS(val_info);
        keys[i] = key_info.ptr;
        vals[i] = val_info.ptr;
        key_sizes[i] = key_info.itemsize*key_info.size;
        val_sizes[i] = val_info.itemsize*val_info.size;
    }
    db.putMulti(count,
            keys.data(),
            key_sizes.data(),
            vals.data(),
            val_sizes.data(),
            mode);
}

template<typename KeyType>
static auto get_helper(const yokan::Database& db, const KeyType& key,
                       py::buffer& val, int32_t mode) {
    auto key_info = get_buffer_info(key);
    auto val_info = val.request();
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    CHECK_BUFFER_IS_CONTIGUOUS(val_info);
    CHECK_BUFFER_IS_WRITABLE(val_info);
    size_t vsize = val_info.itemsize*val_info.size;
    db.get(key_info.ptr,
           key_info.itemsize*key_info.size,
           val_info.ptr,
           &vsize,
           mode);
    return vsize;
}

template<typename KeyType>
static auto get_multi_helper(const yokan::Database& db,
                             const std::vector<std::pair<KeyType, py::buffer>>& keyvals,
                             int32_t mode) {
    auto count = keyvals.size();
    std::vector<const void*> keys(count);
    std::vector<void*>       vals(count);
    std::vector<size_t>      key_sizes(count);
    std::vector<size_t>      val_sizes(count);
    for(size_t i = 0; i < count; i++) {
        auto key_info = get_buffer_info(keyvals[i].first);
        auto val_info = get_buffer_info(keyvals[i].second);
        CHECK_BUFFER_IS_CONTIGUOUS(key_info);
        CHECK_BUFFER_IS_CONTIGUOUS(val_info);
        CHECK_BUFFER_IS_WRITABLE(val_info);
        keys[i] = key_info.ptr;
        vals[i] = val_info.ptr;
        key_sizes[i] = key_info.itemsize*key_info.size;
        val_sizes[i] = val_info.itemsize*val_info.size;
    }
    db.getMulti(count,
                keys.data(),
                key_sizes.data(),
                vals.data(),
                val_sizes.data(),
                mode);
    py::list result;
    for(size_t i=0; i < count; i++) {
        if(val_sizes[i] == YOKAN_KEY_NOT_FOUND)
            result.append(py::none());
        else if(val_sizes[i] == YOKAN_SIZE_TOO_SMALL)
            result.append(-1);
        else
            result.append(val_sizes[i]);
    }
    return result;
}

template<typename KeyType>
static auto exists_helper(const yokan::Database& db,
                          const KeyType& key, int32_t mode) {
    auto key_info = get_buffer_info(key);
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    size_t ksize = key_info.itemsize*key_info.size;
    return db.exists(key_info.ptr, ksize, mode);
}

template<typename KeyType>
static auto exists_multi_helper(const yokan::Database& db,
                                const std::vector<KeyType>& keys,
                                int32_t mode) {
    const auto count = keys.size();
    std::vector<const void*> key_ptrs(count);
    std::vector<size_t>      key_size(count);
    for(size_t i = 0; i < count; i++) {
        auto key_info = get_buffer_info(keys[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(key_info);
        key_ptrs[i] = key_info.ptr;
        key_size[i] = key_info.itemsize*key_info.size;
    }
    return db.existsMulti(count, key_ptrs.data(), key_size.data(), mode);
}

template<typename KeyType>
static auto length_helper(const yokan::Database& db,
                          const KeyType& key,
                          int32_t mode) {
    auto key_info = get_buffer_info(key);
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    size_t ksize = key_info.itemsize*key_info.size;
    return db.length(key_info.ptr, ksize, mode);
}

template<typename KeyType>
static auto length_multi_helper(const yokan::Database& db,
                                const std::vector<KeyType>& keys,
                                int32_t mode) {
    const auto count = keys.size();
    std::vector<const void*> key_ptrs(count);
    std::vector<size_t>      key_size(count);
    std::vector<size_t>      val_size(count);
    for(size_t i = 0; i < count; i++) {
        auto key_info = get_buffer_info(keys[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(key_info);
        key_ptrs[i] = key_info.ptr;
        key_size[i] = key_info.itemsize*key_info.size;
    }
    db.lengthMulti(count, key_ptrs.data(), key_size.data(), val_size.data(), mode);
    py::list result;
    for(size_t i = 0; i < count; i++) {
        if(val_size[i] != YOKAN_KEY_NOT_FOUND)
            result.append(val_size[i]);
        else
            result.append(py::none());
    }
    return result;
}

template<typename KeyType>
static void erase_helper(const yokan::Database& db,
                         const KeyType& key,
                         int32_t mode) {
    auto key_info = get_buffer_info(key);
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    size_t ksize = key_info.itemsize*key_info.size;
    db.erase(key_info.ptr, ksize, mode);
}

template<typename KeyType>
static void erase_multi_helper(const yokan::Database& db,
                               const std::vector<KeyType>& keys,
                               int32_t mode) {
    const auto count = keys.size();
    std::vector<const void*> key_ptrs(count);
    std::vector<size_t>      key_size(count);
    for(size_t i = 0; i < count; i++) {
        auto key_info = get_buffer_info(keys[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(key_info);
        key_ptrs[i] = key_info.ptr;
        key_size[i] = key_info.itemsize*key_info.size;
    }
    return db.eraseMulti(count, key_ptrs.data(), key_size.data(), mode);
}

template<typename FromKeyType, typename FilterType>
static auto list_keys_helper(const yokan::Database& db,
                             std::vector<py::buffer>& keys,
                             const FromKeyType& from_key,
                             const FilterType& filter,
                             int32_t mode) {
    auto count = keys.size();
    std::vector<void*>  keys_data(count);
    std::vector<size_t> keys_size(count);
    auto from_key_info = get_buffer_info(from_key);
    CHECK_BUFFER_IS_CONTIGUOUS(from_key_info);
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    for(size_t i = 0; i < count; i++) {
        auto key_info = get_buffer_info(keys[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(key_info);
        CHECK_BUFFER_IS_WRITABLE(key_info);
        keys_data[i] = key_info.ptr;
        keys_size[i] = key_info.itemsize*key_info.size;
    }
    db.listKeys(from_key_info.ptr,
            from_key_info.itemsize*from_key_info.size,
            filter_info.ptr,
            filter_info.itemsize*filter_info.size,
            count,
            keys_data.data(),
            keys_size.data(),
            mode);
    py::list result;
    for(size_t i=0; i < count; i++) {
        if(keys_size[i] == YOKAN_NO_MORE_KEYS)
            break;
        else if(keys_size[i] == YOKAN_SIZE_TOO_SMALL)
            result.append(-1);
        else
            result.append(keys_size[i]);
    }
    return result;
}

template<typename FromKeyType, typename FilterType>
static auto list_keys_packed_helper(const yokan::Database& db,
                                    py::buffer& keys,
                                    size_t count,
                                    const FromKeyType& from_key,
                                    const FilterType& filter,
                                    int32_t mode) {
    auto from_key_info = get_buffer_info(from_key);
    CHECK_BUFFER_IS_CONTIGUOUS(from_key_info);
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    auto keys_info = get_buffer_info(keys);
    CHECK_BUFFER_IS_CONTIGUOUS(keys_info);
    CHECK_BUFFER_IS_WRITABLE(keys_info);
    size_t keys_buf_size = (size_t)keys_info.itemsize*keys_info.size;
    std::vector<size_t> key_sizes(count);
    db.listKeysPacked(from_key_info.ptr,
            from_key_info.itemsize*from_key_info.size,
            filter_info.ptr,
            filter_info.itemsize*filter_info.size,
            count,
            keys_info.ptr,
            keys_buf_size,
            key_sizes.data(),
            mode);
    py::list result;
    for(size_t i=0; i < count; i++) {
        if(key_sizes[i] == YOKAN_NO_MORE_KEYS)
            break;
        else if(key_sizes[i] == YOKAN_SIZE_TOO_SMALL)
            result.append(-1);
        else
            result.append(key_sizes[i]);
    }
    return result;
}

template<typename FromKeyType, typename FilterType>
static auto list_keyvals_helper(const yokan::Database& db,
                std::vector<std::pair<py::buffer, py::buffer>>& pairs,
                const FromKeyType& from_key,
                const FilterType& filter,
                int32_t mode) {
    auto count = pairs.size();
    std::vector<void*>  keys_data(count);
    std::vector<size_t> keys_size(count);
    std::vector<void*>  vals_data(count);
    std::vector<size_t> vals_size(count);
    auto from_key_info = get_buffer_info(from_key);
    CHECK_BUFFER_IS_CONTIGUOUS(from_key_info);
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    for(size_t i = 0; i < count; i++) {
        auto key_info = get_buffer_info(pairs[i].first);
        auto val_info = get_buffer_info(pairs[i].second);
        CHECK_BUFFER_IS_CONTIGUOUS(key_info);
        CHECK_BUFFER_IS_WRITABLE(key_info);
        CHECK_BUFFER_IS_CONTIGUOUS(val_info);
        CHECK_BUFFER_IS_WRITABLE(val_info);
        keys_data[i] = key_info.ptr;
        keys_size[i] = key_info.itemsize*key_info.size;
        vals_data[i] = val_info.ptr;
        vals_size[i] = val_info.itemsize*val_info.size;
    }
    db.listKeyVals(from_key_info.ptr,
            from_key_info.itemsize*from_key_info.size,
            filter_info.ptr,
            filter_info.itemsize*filter_info.size,
            count,
            keys_data.data(),
            keys_size.data(),
            vals_data.data(),
            vals_size.data(),
            mode);
    std::vector<std::pair<ssize_t, ssize_t>> result;
    result.reserve(count);
    for(size_t i=0; i < count; i++) {
        if(keys_size[i] == YOKAN_NO_MORE_KEYS)
            break;
        result.emplace_back(
                keys_size[i] != YOKAN_SIZE_TOO_SMALL ? keys_size[i] : -1,
                vals_size[i] != YOKAN_SIZE_TOO_SMALL ? vals_size[i] : -1);
    }
    return result;
}

template<typename FromKeyType, typename FilterType>
static auto list_keyvals_packed_helper(
                const yokan::Database& db,
                py::buffer& keys,
                py::buffer& vals,
                size_t count,
                const FromKeyType& from_key,
                const FilterType& filter,
                int32_t mode) {
    auto from_key_info = get_buffer_info(from_key);
    CHECK_BUFFER_IS_CONTIGUOUS(from_key_info);
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    auto keys_info = get_buffer_info(keys);
    CHECK_BUFFER_IS_CONTIGUOUS(keys_info);
    CHECK_BUFFER_IS_WRITABLE(keys_info);
    auto vals_info = get_buffer_info(vals);
    CHECK_BUFFER_IS_CONTIGUOUS(vals_info);
    CHECK_BUFFER_IS_WRITABLE(vals_info);
    size_t kbuf_size = (size_t)keys_info.itemsize*keys_info.size;
    size_t vbuf_size = (size_t)vals_info.itemsize*vals_info.size;
    std::vector<size_t> key_sizes(count);
    std::vector<size_t> val_sizes(count);
    db.listKeyValsPacked(from_key_info.ptr,
            from_key_info.itemsize*from_key_info.size,
            filter_info.ptr,
            filter_info.itemsize*filter_info.size,
            count,
            keys_info.ptr,
            kbuf_size,
            key_sizes.data(),
            vals_info.ptr,
            vbuf_size,
            val_sizes.data(),
            mode);
    std::vector<std::pair<ssize_t, ssize_t>> result;
    result.reserve(count);
    for(size_t i=0; i < count; i++) {
        if(key_sizes[i] == YOKAN_NO_MORE_KEYS)
            break;
        result.emplace_back(
                key_sizes[i] != YOKAN_SIZE_TOO_SMALL ? key_sizes[i] : -1,
                val_sizes[i] != YOKAN_SIZE_TOO_SMALL ? val_sizes[i] : -1);
    }
    return result;
}

template<typename BufferType>
struct to_memory_view;

template<>
struct to_memory_view<std::string> {
    static inline std::string apply(const void* data, size_t size) {
        return std::string{(const char*)data, size};
    }
};

template<>
struct to_memory_view<py::buffer> {
    static inline py::memoryview apply(const void* data, size_t size) {
        return py::memoryview::from_memory((void*)data, size, true);
    }
};

template<typename KeyType, typename FilterType>
static auto iter_helper(
                const yokan::Database& db,
                std::function<void(size_t, const KeyType&, const py::object&)> cb,
                const KeyType& from_key,
                const FilterType& filter,
                size_t count, int32_t mode, unsigned batch_size, bool ignore_values) {
    auto from_key_info = get_buffer_info(from_key);
    CHECK_BUFFER_IS_CONTIGUOUS(from_key_info);
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    yokan::Database::iter_callback_type func =
        [&cb, ignore_values](size_t i, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
            try {
                if(ignore_values)
                    cb(i, to_memory_view<KeyType>::apply(key, ksize), py::none());
                else
                    cb(i, to_memory_view<KeyType>::apply(key, ksize),
                          to_memory_view<py::buffer>::apply(val, vsize));
            } catch(py::error_already_set &e) {
                std::cout << e.what() << std::endl;
                return YOKAN_ERR_OTHER;
            }
            return YOKAN_SUCCESS;
        };
    yk_iter_options_t options;
    options.batch_size    = batch_size;
    options.ignore_values = ignore_values;
    options.pool          = ABT_POOL_NULL;
    db.iter(from_key_info.ptr,
            from_key_info.itemsize*from_key_info.size,
            filter_info.ptr,
            filter_info.itemsize*filter_info.size,
            count, func, &options, mode);
}

template <typename DocType>
static auto doc_store_helper(const yokan::Collection& coll,
                             const DocType& doc, int32_t mode) {
    auto doc_info = get_buffer_info(doc);
    CHECK_BUFFER_IS_CONTIGUOUS(doc_info);
    return coll.store(doc_info.ptr, doc_info.itemsize*doc_info.size, mode);
}

template <typename DocType>
static auto doc_store_multi_helper(const yokan::Collection& coll,
                                   const std::vector<DocType>& docs,
                                   int32_t mode) {
    auto count = docs.size();
    std::vector<yk_id_t> ids(count);
    std::vector<const void*> doc_ptrs(count);
    std::vector<size_t> doc_sizes(count);
    for(size_t i = 0; i < count; i++) {
        auto doc_info = get_buffer_info(docs[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(doc_info);
        doc_ptrs[i] = doc_info.ptr;
        doc_sizes[i] = doc_info.itemsize*doc_info.size;
    }
    coll.storeMulti(count, doc_ptrs.data(), doc_sizes.data(),
                    ids.data(), mode);
    return ids;
}

template <typename DocType>
static void doc_update_helper(const yokan::Collection& coll, yk_id_t id,
                              const DocType& doc, int32_t mode) {
    auto doc_info = get_buffer_info(doc);
    CHECK_BUFFER_IS_CONTIGUOUS(doc_info);
    coll.update(id, doc_info.ptr, doc_info.itemsize*doc_info.size, mode);
}

template <typename DocType>
static void doc_update_multi_helper(const yokan::Collection& coll,
                                    const std::vector<yk_id_t>& ids,
                                    const std::vector<DocType>& docs,
                                    int32_t mode) {
    auto count = docs.size();
    if(ids.size() != count) {
        throw std::length_error("\"ids\" and \"documents\" arguments should be the same size");
    }
    std::vector<const void*> doc_ptrs(count);
    std::vector<size_t> doc_sizes(count);
    for(size_t i = 0; i < count; i++) {
        auto doc_info = get_buffer_info(docs[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(doc_info);
        doc_ptrs[i] = doc_info.ptr;
        doc_sizes[i] = doc_info.itemsize*doc_info.size;
    }
    coll.updateMulti(count, ids.data(), doc_ptrs.data(), doc_sizes.data(), mode);
}

static auto doc_load_helper(const yokan::Collection& coll, yk_id_t id,
                            py::buffer& val, int32_t mode) {
    auto val_info = val.request();
    CHECK_BUFFER_IS_CONTIGUOUS(val_info);
    CHECK_BUFFER_IS_WRITABLE(val_info);
    size_t vsize = val_info.itemsize*val_info.size;
    coll.load(id, val_info.ptr, &vsize, mode);
    return vsize;
}

static auto doc_load_multi_helper(const yokan::Collection& coll,
                                  const std::vector<yk_id_t>& ids,
                                  const std::vector<py::buffer>& docs,
                                  int32_t mode) {
    if(docs.size() != ids.size()) {
        throw std::length_error("\"ids\" and \"buffers\" arguments should have the same size");
    }
    auto count = docs.size();
    std::vector<void*>  doc_ptrs(count);
    std::vector<size_t> doc_sizes(count);
    for(size_t i = 0; i < count; i++) {
        auto doc_info = get_buffer_info(docs[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(doc_info);
        CHECK_BUFFER_IS_WRITABLE(doc_info);
        doc_ptrs[i] = doc_info.ptr;
        doc_sizes[i] = doc_info.itemsize*doc_info.size;
    }
    coll.loadMulti(count, ids.data(),
                   doc_ptrs.data(), doc_sizes.data(), mode);
    py::list result;
    for(size_t i=0; i < count; i++) {
        if(doc_sizes[i] == YOKAN_KEY_NOT_FOUND)
            result.append(py::none());
        else if(doc_sizes[i] == YOKAN_SIZE_TOO_SMALL)
            result.append(-1);
        else
            result.append(doc_sizes[i]);
    }
    return result;
}

template<typename FilterType>
static auto list_docs_helper(const yokan::Collection& coll,
                             yk_id_t start_id,
                             std::vector<py::buffer>& buffers,
                             const FilterType& filter,
                             int32_t mode) {
    auto count = buffers.size();
    std::vector<void*>  buf_data(count);
    std::vector<size_t> buf_size(count);
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    for(size_t i = 0; i < count; i++) {
        auto buf_info = get_buffer_info(buffers[i]);
        CHECK_BUFFER_IS_CONTIGUOUS(buf_info);
        CHECK_BUFFER_IS_WRITABLE(buf_info);
        buf_data[i] = buf_info.ptr;
        buf_size[i] = buf_info.itemsize*buf_info.size;
    }
    std::vector<yk_id_t> ids(count);
    coll.list(start_id,
              filter_info.ptr,
              filter_info.itemsize*filter_info.size,
              count, ids.data(),
              buf_data.data(),
              buf_size.data(),
              mode);
    py::list result;
    for(size_t i=0; i < count; i++) {
        if(buf_size[i] == YOKAN_NO_MORE_DOCS)
            break;
        else if(buf_size[i] == YOKAN_SIZE_TOO_SMALL) {
            result.append(std::make_pair(ids[i], -1));
        }
        else {
            result.append(std::make_pair(ids[i], buf_size[i]));
        }
    }
    return result;
}

template<typename FilterType>
static auto list_docs_packed_helper(const yokan::Collection& coll,
                                    yk_id_t start_id,
                                    py::buffer& buffer,
                                    size_t count,
                                    const FilterType& filter,
                                    int32_t mode) {
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    auto buf_info = get_buffer_info(buffer);
    CHECK_BUFFER_IS_CONTIGUOUS(buf_info);
    CHECK_BUFFER_IS_WRITABLE(buf_info);
    size_t buf_size = (size_t)buf_info.itemsize*buf_info.size;
    std::vector<size_t> doc_sizes(count);
    std::vector<yk_id_t> ids(count);
    coll.listPacked(start_id,
            filter_info.ptr,
            filter_info.itemsize*filter_info.size,
            count, ids.data(),
            buf_size,
            buf_info.ptr,
            doc_sizes.data(),
            mode);
    py::list result;
    for(size_t i=0; i < count; i++) {
        if(doc_sizes[i] == YOKAN_NO_MORE_DOCS)
            break;
        else if(doc_sizes[i] == YOKAN_SIZE_TOO_SMALL)
            result.append(std::make_pair(ids[i], -1));
        else
            result.append(std::make_pair(ids[i], doc_sizes[i]));
    }
    return result;
}

template<typename FilterType>
static auto doc_iter_helper(
                const yokan::Collection& coll,
                std::function<void(size_t, yk_id_t, const py::buffer&)> cb,
                yk_id_t from_id,
                const FilterType& filter,
                size_t max, int32_t mode, unsigned batch_size) {
    auto filter_info = get_buffer_info(filter);
    CHECK_BUFFER_IS_CONTIGUOUS(filter_info);
    yokan::Collection::iter_callback_type func =
        [&cb](size_t i, yk_id_t id, const void* doc, size_t docsize) -> yk_return_t {
            try {
                cb(i, id, py::memoryview::from_memory((void*)doc, docsize, true));
            } catch(py::error_already_set &e) {
                std::cout << e.what() << std::endl;
                return YOKAN_ERR_OTHER;
            }
            return YOKAN_SUCCESS;
        };
    yk_doc_iter_options_t options;
    options.batch_size    = batch_size;
    options.pool          = ABT_POOL_NULL;
    coll.iter(from_id, filter_info.ptr, filter_info.itemsize*filter_info.size,
              max, func, &options, mode);
}


PYBIND11_MODULE(pyyokan_client, m) {
    m.doc() = "Python binding for the YOKAN client library";

    py::module::import("pyyokan_common");

    py::class_<yokan::Client>(m, "Client")

        .def(py::init<py_margo_instance_id>(), "mid"_a)

        .def("make_database_handle",
             [](const yokan::Client& client,
                py_hg_addr_t addr,
                uint16_t provider_id,
                yk_database_id_t database_id) {
                return client.makeDatabaseHandle(addr, provider_id, database_id);
             },
             "address"_a, "provider_id"_a, "database_id"_a)

        .def("find_database_by_name",
             [](const yokan::Client& client,
                py_hg_addr_t addr,
                uint16_t provider_id,
                const std::string& name) {
                return client.findDatabaseByName(addr, provider_id, name.c_str());
             },
             "address"_a, "provider_id"_a, "name"_a);

    py::class_<yokan::Database>(m, "Database")
        // --------------------------------------------------------------
        // ID
        // --------------------------------------------------------------
        .def_property_readonly("id",
            &yokan::Database::id)
        // --------------------------------------------------------------
        // COUNT
        // --------------------------------------------------------------
        .def("count",
             [](const yokan::Database& db, int32_t mode) {
                return db.count(mode);
             }, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT
        // --------------------------------------------------------------
        .def("put",
             static_cast<void(*)(const yokan::Database&, const py::buffer&,
                         const py::buffer&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put",
             static_cast<void(*)(const yokan::Database&, const std::string&,
                         const py::buffer&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put",
             static_cast<void(*)(const yokan::Database&, const std::string&,
                         const std::string&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT_MULTI
        // --------------------------------------------------------------
        .def("put_multi",
             static_cast<void(*)(const yokan::Database&,
                const std::vector<std::pair<py::buffer,py::buffer>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put_multi",
             static_cast<void(*)(const yokan::Database&,
                const std::vector<std::pair<std::string,py::buffer>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put_multi",
             static_cast<void(*)(const yokan::Database&,
                const std::vector<std::pair<std::string,std::string>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT_PACKED
        // --------------------------------------------------------------
        .def("put_packed",
             [](const yokan::Database& db, const py::buffer& keys,
                const std::vector<size_t> key_sizes,
                const py::buffer& vals,
                const std::vector<size_t>& val_sizes,
                int32_t mode) {
                size_t count = key_sizes.size();
                if(count != val_sizes.size()) {
                    throw std::length_error("key_sizes and value_sizes should have the same length");
                }
                auto key_info = keys.request();
                auto val_info = vals.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                auto total_val_size = std::accumulate(val_sizes.begin(), val_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer is smaller than accumulated key_sizes");
                }
                if((ssize_t)total_val_size > val_info.itemsize*val_info.size) {
                    throw std::length_error("values buffer is smaller than accumulated value_sizes");
                }
                db.putPacked(count,
                       key_info.ptr,
                       key_sizes.data(),
                       val_info.ptr,
                       val_sizes.data(),
                       mode);
             }, "keys"_a, "key_sizes"_a, "values"_a, "value_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET
        // --------------------------------------------------------------
        .def("get",
             static_cast<size_t(*)(const yokan::Database&, const py::buffer&,
                py::buffer&, int32_t)>(&get_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("get",
             static_cast<size_t(*)(const yokan::Database&, const std::string&,
                py::buffer&, int32_t)>(&get_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET_MULTI
        // --------------------------------------------------------------
        .def("get_multi",
             static_cast<py::list(*)(const yokan::Database&,
                const std::vector<std::pair<py::buffer, py::buffer>>&,
                int32_t)>(&get_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("get_multi",
             static_cast<py::list(*)(const yokan::Database&,
                const std::vector<std::pair<std::string, py::buffer>>&,
                int32_t)>(&get_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET_PACKED
        // --------------------------------------------------------------
        .def("get_packed",
             [](const yokan::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                py::buffer& vals,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                auto val_info = vals.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                size_t vbuf_size = (size_t)(val_info.itemsize*val_info.size);
                std::vector<size_t> val_sizes(count);
                db.getPacked(count, key_info.ptr, key_sizes.data(),
                             vbuf_size, val_info.ptr, val_sizes.data(), mode);
                py::list result;
                for(size_t i = 0; i < count; i++) {
                    if(val_sizes[i] != YOKAN_KEY_NOT_FOUND)
                        result.append(val_sizes[i]);
                    else
                        result.append(py::none());
                }
                return result;
             }, "keys"_a, "key_sizes"_a, "values"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // FETCH
        // --------------------------------------------------------------
        .def("fetch",
             [](const yokan::Database& db, const py::buffer& key,
                std::function<void(size_t, const py::buffer&, const py::object&)> cb, int32_t mode) {
                auto key_info = get_buffer_info(key);
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto func =
                    [&cb](size_t index, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, py::memoryview::from_memory(key, ksize), py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, py::memoryview::from_memory(key, ksize), py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                db.fetch(
                    (const void*)key_info.ptr,
                    key_info.itemsize*key_info.size,
                    func, mode);
             },
             "key"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("fetch",
             [](const yokan::Database& db, const std::string& key,
                std::function<void(size_t, const std::string&, const py::object&)> cb, int32_t mode) {
                auto key_info = get_buffer_info(key);
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto func =
                    [&cb, &key](size_t index, const void*, size_t, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, key, py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, key, py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                db.fetch(
                    (const void*)key_info.ptr,
                    key_info.itemsize*key_info.size,
                    func, mode);
             },
             "key"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // FETCH_MULTI
        // --------------------------------------------------------------
        .def("fetch_multi",
             [](const yokan::Database& db, const std::vector<py::buffer>& keys,
                std::function<void(size_t, const py::buffer&, const py::object&)> cb,
                int32_t mode, unsigned batch_size) {
                std::vector<const void*> key_ptrs;
                std::vector<size_t>      key_sizes;
                key_ptrs.reserve(keys.size());
                key_sizes.reserve(keys.size());
                for(auto& key : keys) {
                    auto key_info = get_buffer_info(key);
                    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                    key_ptrs.push_back(key_info.ptr);
                    key_sizes.push_back(key_info.itemsize*key_info.size);
                }
                auto func =
                    [&cb](size_t index, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, py::memoryview::from_memory(key, ksize), py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, py::memoryview::from_memory(key, ksize), py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                yk_fetch_options_t options;
                options.pool       = ABT_POOL_NULL;
                options.batch_size = batch_size;
                db.fetchMulti(
                    keys.size(), key_ptrs.data(), key_sizes.data(),
                    func, &options, mode);
             },
             "keys"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        .def("fetch_multi",
             [](const yokan::Database& db, const std::vector<std::string>& keys,
                std::function<void(size_t, const std::string&, const py::object&)> cb,
                int32_t mode, unsigned batch_size) {
                std::vector<const void*> key_ptrs;
                std::vector<size_t>      key_sizes;
                key_ptrs.reserve(keys.size());
                key_sizes.reserve(keys.size());
                for(auto& key : keys) {
                    key_ptrs.push_back(key.data());
                    key_sizes.push_back(key.size());
                }
                auto func =
                    [&cb, &keys](size_t index, const void*, size_t, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, keys[index], py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, keys[index], py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                yk_fetch_options_t options;
                options.pool       = ABT_POOL_NULL;
                options.batch_size = batch_size;
                db.fetchMulti(
                    keys.size(), key_ptrs.data(), key_sizes.data(),
                    func, &options, mode);
             },
             "keys"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        // --------------------------------------------------------------
        // FETCH_PACKED
        // --------------------------------------------------------------
        .def("fetch_packed",
             [](const yokan::Database& db, const py::buffer& keys,
                const std::vector<size_t>& ksizes,
                std::function<void(size_t, const py::buffer&, const py::object&)> cb,
                int32_t mode, unsigned batch_size) {
                auto key_info = get_buffer_info(keys);
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(ksizes.begin(), ksizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                auto func =
                    [&cb](size_t index, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, py::memoryview::from_memory(key, ksize), py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, py::memoryview::from_memory(key, ksize), py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                yk_fetch_options_t options;
                options.pool       = ABT_POOL_NULL;
                options.batch_size = batch_size;
                db.fetchPacked(ksizes.size(),
                    key_info.ptr, ksizes.data(),
                    func, &options, mode);
             },
             "keys"_a, "key_sizes"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        // --------------------------------------------------------------
        // EXISTS
        // --------------------------------------------------------------
        .def("exists",
             static_cast<bool(*)(const yokan::Database&, const std::string&, int32_t)>(&exists_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("exists",
             static_cast<bool(*)(const yokan::Database&, const py::buffer&, int32_t)>(&exists_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS_MULTI
        // --------------------------------------------------------------
        .def("exists_multi",
             static_cast<std::vector<bool>(*)(const yokan::Database&,
                 const std::vector<std::string>&, int32_t)>(&exists_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("exists_multi",
             static_cast<std::vector<bool>(*)(const yokan::Database&,
                 const std::vector<py::buffer>&, int32_t)>(&exists_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS_PACKED
        // --------------------------------------------------------------
        .def("exists_packed",
             [](const yokan::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                return db.existsPacked(count, key_info.ptr, key_sizes.data(), mode);
             }, "keys"_a, "key_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH
        // --------------------------------------------------------------
        .def("length",
             static_cast<size_t(*)(const yokan::Database&, const std::string&, int32_t)>(&length_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("length",
             static_cast<size_t(*)(const yokan::Database&, const py::buffer&, int32_t)>(&length_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_MULTI
        // --------------------------------------------------------------
        .def("length_multi",
             static_cast<py::list(*)(const yokan::Database&,
                         const std::vector<std::string>&,
                         int32_t)>(&length_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("length_multi",
             static_cast<py::list(*)(const yokan::Database&,
                         const std::vector<py::buffer>&,
                         int32_t)>(&length_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_PACKED
        // --------------------------------------------------------------
        .def("length_packed",
             [](const yokan::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                std::vector<size_t> val_sizes(count);
                db.lengthPacked(count, key_info.ptr, key_sizes.data(), val_sizes.data(), mode);
                py::list result;
                for(size_t i = 0; i < count; i++) {
                    if(val_sizes[i] != YOKAN_KEY_NOT_FOUND)
                        result.append(val_sizes[i]);
                    else
                        result.append(py::none());
                }
                return result;
             }, "keys"_a, "key_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE
        // --------------------------------------------------------------
        .def("erase",
             static_cast<void(*)(const yokan::Database&, const std::string&, int32_t)>(&erase_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("erase",
             static_cast<void(*)(const yokan::Database&, const py::buffer&, int32_t)>(&erase_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_MULTI
        // --------------------------------------------------------------
        .def("erase_multi",
             static_cast<void(*)(const yokan::Database&,
                                 const std::vector<std::string>&,
                                 int32_t)>(&erase_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("erase_multi",
             static_cast<void(*)(const yokan::Database&,
                                 const std::vector<py::buffer>&,
                                 int32_t)>(&erase_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_PACKED
        // --------------------------------------------------------------
        .def("erase_packed",
             [](const yokan::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                db.erasePacked(count, key_info.ptr, key_sizes.data(), mode);
             }, "keys"_a, "key_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYS
        // --------------------------------------------------------------
        .def("list_keys",
             static_cast<py::list(*)(const yokan::Database&,
                std::vector<py::buffer>&,
                const py::buffer&,
                const py::buffer&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const yokan::Database&,
                std::vector<py::buffer>&,
                const py::buffer&,
                const std::string&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const yokan::Database&,
                std::vector<py::buffer>&,
                const std::string&,
                const py::buffer&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const yokan::Database&,
                std::vector<py::buffer>&,
                const std::string&,
                const std::string&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYS_PACKED
        // --------------------------------------------------------------
        .def("list_keys_packed",
             static_cast<py::list(*)(const yokan::Database&,
                py::buffer&, size_t, const py::buffer&,
                const py::buffer&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const yokan::Database&,
                py::buffer&, size_t, const std::string&,
                const py::buffer&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const yokan::Database&,
                py::buffer&, size_t, const py::buffer&,
                const std::string&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const yokan::Database&,
                py::buffer&, size_t, const std::string&,
                const std::string&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYVALS
        // --------------------------------------------------------------
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const py::buffer&, const py::buffer&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const std::string&, const py::buffer&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const py::buffer&, const std::string&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const std::string&, const std::string&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYVALS_PACKED
        // --------------------------------------------------------------
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&, py::buffer&, py::buffer&, size_t,
                const py::buffer&, const py::buffer&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&, py::buffer&, py::buffer&, size_t,
                const std::string&, const py::buffer&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&, py::buffer&, py::buffer&, size_t,
                const py::buffer&, const std::string&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const yokan::Database&, py::buffer&, py::buffer&, size_t,
                const std::string&, const std::string&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ITER
        // --------------------------------------------------------------
        .def("iter",
             static_cast<void(*)(
                const yokan::Database&,
                std::function<void(size_t, const py::buffer&, const py::object&)>,
                const py::buffer&, const py::buffer&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=py::bytes{}, "filter"_a=py::bytes{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        .def("iter",
             static_cast<void(*)(
                const yokan::Database&,
                std::function<void(size_t, const py::buffer&, const py::object&)>,
                const py::buffer&, const std::string&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=py::bytes{}, "filter"_a=std::string{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        .def("iter",
             static_cast<void(*)(
                const yokan::Database&,
                std::function<void(size_t, const std::string&, const py::object&)>,
                const std::string&, const py::buffer&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=std::string{}, "filter"_a=py::bytes{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        .def("iter",
             static_cast<void(*)(
                const yokan::Database&,
                std::function<void(size_t, const std::string&, const py::object&)>,
                const std::string&, const std::string&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=std::string{}, "filter"_a=std::string{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        // --------------------------------------------------------------
        // COLLECTION MANAGEMENT
        // --------------------------------------------------------------
        .def("create_collection",
             [](const yokan::Database& db, const std::string& name, int32_t mode) {
                db.createCollection(name.c_str(), mode);
                return yokan::Collection(name.c_str(), db);
             },
             "name"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("__getitem__",
             [](const yokan::Database& db, const std::string& name) {
                return yokan::Collection(name.c_str(), db);
             },
             "name"_a)
        .def("drop_collection",
             [](const yokan::Database& db, const std::string& name, int32_t mode) {
                db.dropCollection(name.c_str(), mode);
             },
             "name"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("collection_exists",
             [](const yokan::Database& db, const std::string& name, int32_t mode) {
                return db.collectionExists(name.c_str(), mode);
             },
             "name"_a, "mode"_a=YOKAN_MODE_DEFAULT)
    ;

    py::class_<yokan::Collection>(m, "Collection")
        .def(py::init<const char*, yokan::Database>())

        // --------------------------------------------------------------
        // SIZE
        // --------------------------------------------------------------
        .def("size",
             [](const yokan::Collection& coll, int32_t mode) {
                return coll.size(mode);
             }, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // STORE
        // --------------------------------------------------------------
        .def("store",
             static_cast<yk_id_t(*)(const yokan::Collection&,
                         const py::buffer&, int32_t)>(&doc_store_helper),
             "document"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("store",
             static_cast<yk_id_t(*)(const yokan::Collection&,
                         const std::string&, int32_t)>(&doc_store_helper),
             "document"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // STORE_MULTI
        // --------------------------------------------------------------
        .def("store_multi",
             static_cast<std::vector<yk_id_t>(*)(const yokan::Collection&,
                const std::vector<py::buffer>&,
                int32_t)>(&doc_store_multi_helper),
             "documents"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("store_multi",
             static_cast<std::vector<yk_id_t>(*)(const yokan::Collection&,
                const std::vector<std::string>&,
                int32_t)>(&doc_store_multi_helper),
             "documents"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // STORE_PACKED
        // --------------------------------------------------------------
        .def("store_packed",
             [](const yokan::Collection& coll,
                const py::buffer& docs,
                const std::vector<size_t>& doc_sizes,
                int32_t mode) {
                size_t count = doc_sizes.size();
                auto docs_info = docs.request();
                CHECK_BUFFER_IS_CONTIGUOUS(docs_info);
                auto total_doc_sizes = std::accumulate(doc_sizes.begin(), doc_sizes.end(), (size_t)0);
                if((ssize_t)total_doc_sizes > docs_info.itemsize*docs_info.size) {
                    throw std::length_error("documents buffer is smaller than accumulated doc_sizes");
                }
                std::vector<yk_id_t> ids(count);
                coll.storePacked(count, docs_info.ptr, doc_sizes.data(), ids.data(), mode);
                return ids;
             }, "documents"_a, "doc_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // UPDATE
        // --------------------------------------------------------------
        .def("update",
             static_cast<void(*)(const yokan::Collection&, yk_id_t,
                         const py::buffer&, int32_t)>(&doc_update_helper),
             "id"_a, "document"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("update",
             static_cast<void(*)(const yokan::Collection&, yk_id_t,
                         const std::string&, int32_t)>(&doc_update_helper),
             "id"_a, "document"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // UPDATE_MULTI
        // --------------------------------------------------------------
        .def("update_multi",
             static_cast<void(*)(const yokan::Collection&,
                const std::vector<yk_id_t>&,
                const std::vector<py::buffer>&,
                int32_t)>(&doc_update_multi_helper),
             "ids"_a, "documents"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("update_multi",
             static_cast<void(*)(const yokan::Collection&,
                const std::vector<yk_id_t>&,
                const std::vector<std::string>&,
                int32_t)>(&doc_update_multi_helper),
             "ids"_a, "documents"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // UPDATE_PACKED
        // --------------------------------------------------------------
        .def("update_packed",
             [](const yokan::Collection& coll,
                const std::vector<yk_id_t>& ids,
                const py::buffer& docs,
                const std::vector<size_t>& doc_sizes,
                int32_t mode) {
                size_t count = doc_sizes.size();
                if(count != ids.size()) {
                    throw std::length_error("\"ids\" and \"doc_sizes\" arguments should be the same size");
                }
                auto docs_info = docs.request();
                CHECK_BUFFER_IS_CONTIGUOUS(docs_info);
                auto total_doc_sizes = std::accumulate(doc_sizes.begin(), doc_sizes.end(), (size_t)0);
                if((ssize_t)total_doc_sizes > docs_info.itemsize*docs_info.size) {
                    throw std::length_error("documents buffer is smaller than accumulated doc_sizes");
                }
                coll.updatePacked(count, ids.data(), docs_info.ptr, doc_sizes.data(), mode);
                return ids;
             }, "ids"_a, "documents"_a, "doc_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LOAD
        // --------------------------------------------------------------
        .def("load",
             static_cast<size_t(*)(const yokan::Collection&, yk_id_t,
                py::buffer&, int32_t)>(&doc_load_helper),
             "id"_a, "buffer"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LOAD_MULTI
        // --------------------------------------------------------------
        .def("load_multi",
             static_cast<py::list(*)(const yokan::Collection&,
                const std::vector<yk_id_t>& ids,
                const std::vector<py::buffer>&,
                int32_t)>(&doc_load_multi_helper),
             "ids"_a, "buffers"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LOAD_PACKED
        // --------------------------------------------------------------
        .def("load_packed",
             [](const yokan::Collection& coll, const std::vector<yk_id_t>& ids,
                py::buffer& docs, int32_t mode) {
                auto count = ids.size();
                auto doc_info = docs.request();
                CHECK_BUFFER_IS_CONTIGUOUS(doc_info);
                size_t doc_buf_size = (size_t)(doc_info.itemsize*doc_info.size);
                std::vector<size_t> doc_sizes(count);
                coll.loadPacked(count, ids.data(),
                                doc_buf_size, doc_info.ptr, doc_sizes.data(), mode);
                py::list result;
                for(size_t i = 0; i < count; i++) {
                    if(doc_sizes[i] != YOKAN_KEY_NOT_FOUND)
                        result.append(doc_sizes[i]);
                    else
                        result.append(py::none());
                }
                return result;
             }, "ids"_a, "buffer"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // FETCH
        // --------------------------------------------------------------
        .def("fetch",
             [](const yokan::Collection& coll, yk_id_t id,
                std::function<void(size_t, yk_id_t, const py::object&)> cb, int32_t mode) {
                auto func =
                    [&cb](size_t index, yk_id_t id, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, id, py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, id, py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                coll.fetch(id, func, mode);
             },
             "id"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // FETCH_MULTI
        // --------------------------------------------------------------
        .def("fetch_multi",
             [](const yokan::Collection& coll, const std::vector<yk_id_t>& ids,
                std::function<void(size_t, yk_id_t, const py::object&)> cb,
                int32_t mode, unsigned batch_size) {
                auto func =
                    [&cb](size_t index, yk_id_t id, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, id, py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, id, py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                yk_doc_fetch_options_t options;
                options.pool       = ABT_POOL_NULL;
                options.batch_size = batch_size;
                coll.fetchMulti(
                    ids.size(), ids.data(),
                    func, &options, mode);
             },
             "ids"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        // --------------------------------------------------------------
        // LENGTH
        // --------------------------------------------------------------
        .def("length",
             [](const yokan::Collection& coll, yk_id_t id, int32_t mode) {
                return coll.length(id, mode);
             },
             "id"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_MULTI
        // --------------------------------------------------------------
        .def("length_multi",
             [](const yokan::Collection& coll, const std::vector<yk_id_t>& ids, int32_t mode) {
                std::vector<size_t> len(ids.size());
                coll.lengthMulti(ids.size(), ids.data(), len.data(), mode);
                py::list result;
                for(size_t i = 0; i < ids.size(); i++) {
                    if(len[i] != YOKAN_KEY_NOT_FOUND)
                        result.append(len[i]);
                    else
                        result.append(py::none());
                }
                return result;
             },
             "ids"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE
        // --------------------------------------------------------------
        .def("erase",
             [](const yokan::Collection& coll, yk_id_t id, int32_t mode) {
                coll.erase(id, mode);
             },
             "id"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_MULTI
        // --------------------------------------------------------------
        .def("erase_multi",
             [](const yokan::Collection& coll, const std::vector<yk_id_t>& ids, int32_t mode) {
                coll.eraseMulti(ids.size(), ids.data(), mode);
             },
             "ids"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_DOCS
        // --------------------------------------------------------------
        .def("list_docs",
             static_cast<py::list(*)(const yokan::Collection&,
                yk_id_t,
                std::vector<py::buffer>&,
                const py::buffer&,
                int32_t)>(&list_docs_helper),
             "start_id"_a, "buffers"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_docs",
             static_cast<py::list(*)(const yokan::Collection&,
                yk_id_t,
                std::vector<py::buffer>&,
                const std::string&,
                int32_t)>(&list_docs_helper),
             "start_id"_a, "buffers"_a,
             "filter"_a=std::string(), "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_DOCS_PACKED
        // --------------------------------------------------------------
        .def("list_docs_packed",
             static_cast<py::list(*)(const yokan::Collection&,
                yk_id_t, py::buffer&, size_t, const py::buffer&,
                int32_t)>(&list_docs_packed_helper),
             "start_id"_a, "buffer"_a, "count"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_docs_packed",
             static_cast<py::list(*)(const yokan::Collection&,
                yk_id_t, py::buffer&, size_t, const std::string&,
                int32_t)>(&list_docs_packed_helper),
             "start_id"_a, "buffer"_a, "count"_a,
             "filter"_a=std::string(), "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // DOC_ITER
        // --------------------------------------------------------------
        .def("iter",
             static_cast<void(*)(
                const yokan::Collection&,
                std::function<void(size_t, yk_id_t, const py::buffer&)>,
                yk_id_t, const py::buffer&,
                size_t, int32_t, unsigned)>(&doc_iter_helper),
             "callback"_a,
             "from_id"_a=0, "filter"_a=py::bytes{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        .def("iter",
             static_cast<void(*)(
                const yokan::Collection&,
                std::function<void(size_t, yk_id_t, const py::buffer&)>,
                yk_id_t, const std::string&,
                size_t, int32_t, unsigned)>(&doc_iter_helper),
             "callback"_a,
             "from_id"_a=0, "filter"_a=std::string{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        ;
}


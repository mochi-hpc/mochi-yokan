#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <yokan/cxx/client.hpp>
#include <yokan/cxx/database.hpp>
#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t", nullptr)


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
            throw rkv::Exception(RKV_ERR_NONCONTIG);  \
        __stride__ *= (__buf_info__).shape[i];        \
    }                                                 \
} while(0)

#define CHECK_BUFFER_IS_WRITABLE(__buf_info__) do { \
    if((__buf_info__).readonly)                     \
        throw rkv::Exception(RKV_ERR_READONLY);     \
} while(0)

template <typename KeyType, typename ValueType>
static void put_helper(const rkv::Database& db, const KeyType& key,
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
static void put_multi_helper(const rkv::Database& db,
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
static auto get_helper(const rkv::Database& db, const KeyType& key,
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
static auto get_multi_helper(const rkv::Database& db,
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
        if(val_sizes[i] == RKV_KEY_NOT_FOUND)
            result.append(py::none());
        else if(val_sizes[i] == RKV_SIZE_TOO_SMALL)
            result.append(-1);
        else
            result.append(val_sizes[i]);
    }
    return result;
}

template<typename KeyType>
static auto exists_helper(const rkv::Database& db,
                          const KeyType& key, int32_t mode) {
    auto key_info = get_buffer_info(key);
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    size_t ksize = key_info.itemsize*key_info.size;
    return db.exists(key_info.ptr, ksize, mode);
}

template<typename KeyType>
static auto exists_multi_helper(const rkv::Database& db,
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
static auto length_helper(const rkv::Database& db,
                          const KeyType& key,
                          int32_t mode) {
    auto key_info = get_buffer_info(key);
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    size_t ksize = key_info.itemsize*key_info.size;
    return db.length(key_info.ptr, ksize, mode);
}

template<typename KeyType>
static auto length_multi_helper(const rkv::Database& db,
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
        if(val_size[i] != RKV_KEY_NOT_FOUND)
            result.append(val_size[i]);
        else
            result.append(py::none());
    }
    return result;
}

template<typename KeyType>
static void erase_helper(const rkv::Database& db,
                         const KeyType& key,
                         int32_t mode) {
    auto key_info = get_buffer_info(key);
    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
    size_t ksize = key_info.itemsize*key_info.size;
    db.erase(key_info.ptr, ksize, mode);
}

template<typename KeyType>
static void erase_multi_helper(const rkv::Database& db,
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
static auto list_keys_helper(const rkv::Database& db,
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
        if(keys_size[i] == RKV_NO_MORE_KEYS)
            break;
        else if(keys_size[i] == RKV_SIZE_TOO_SMALL)
            result.append(-1);
        else
            result.append(keys_size[i]);
    }
    return result;
}

template<typename FromKeyType, typename FilterType>
static auto list_keys_packed_helper(const rkv::Database& db,
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
        if(key_sizes[i] == RKV_NO_MORE_KEYS)
            break;
        else if(key_sizes[i] == RKV_SIZE_TOO_SMALL)
            result.append(-1);
        else
            result.append(key_sizes[i]);
    }
    return result;
}

template<typename FromKeyType, typename FilterType>
static auto list_keyvals_helper(const rkv::Database& db,
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
        if(keys_size[i] == RKV_NO_MORE_KEYS)
            break;
        result.emplace_back(
                keys_size[i] != RKV_SIZE_TOO_SMALL ? keys_size[i] : -1,
                vals_size[i] != RKV_SIZE_TOO_SMALL ? vals_size[i] : -1);
    }
    return result;
}

template<typename FromKeyType, typename FilterType>
static auto list_keyvals_packed_helper(
                const rkv::Database& db,
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
        if(key_sizes[i] == RKV_NO_MORE_KEYS)
            break;
        result.emplace_back(
                key_sizes[i] != RKV_SIZE_TOO_SMALL ? key_sizes[i] : -1,
                val_sizes[i] != RKV_SIZE_TOO_SMALL ? val_sizes[i] : -1);
    }
    return result;
}

PYBIND11_MODULE(pyyokan_client, m) {
    m.doc() = "Python binding for the RKV client library";

    py::module::import("pyyokan_common");

    m.attr("RKV_MODE_DEFAULT")     = RKV_MODE_DEFAULT;
    m.attr("RKV_MODE_INCLUSIVE")   = RKV_MODE_INCLUSIVE;
    m.attr("RKV_MODE_APPEND")      = RKV_MODE_APPEND;
    m.attr("RKV_MODE_CONSUME")     = RKV_MODE_CONSUME;
    m.attr("RKV_MODE_WAIT")        = RKV_MODE_WAIT;
    m.attr("RKV_MODE_NOTIFY")      = RKV_MODE_NOTIFY;
    m.attr("RKV_MODE_NEW_ONLY")    = RKV_MODE_NEW_ONLY;
    m.attr("RKV_MODE_EXIST_ONLY")  = RKV_MODE_EXIST_ONLY;
    m.attr("RKV_MODE_NO_PREFIX")   = RKV_MODE_NO_PREFIX;
    m.attr("RKV_MODE_IGNORE_KEYS") = RKV_MODE_IGNORE_KEYS;
    m.attr("RKV_MODE_KEEP_LAST")   = RKV_MODE_KEEP_LAST;
    m.attr("RKV_MODE_SUFFIX")      = RKV_MODE_SUFFIX;
    m.attr("RKV_MODE_LUA_FILTER")  = RKV_MODE_LUA_FILTER;

    py::class_<rkv::Client>(m, "Client")

        .def(py::init<py_margo_instance_id>(), "mid"_a)

        .def("make_database_handle",
             [](const rkv::Client& client,
                py_hg_addr_t addr,
                uint16_t provider_id,
                rkv_database_id_t database_id) {
                return client.makeDatabaseHandle(addr, provider_id, database_id);
             },
             "address"_a, "provider_id"_a, "database_id"_a);

    py::class_<rkv::Database>(m, "Database")
        // --------------------------------------------------------------
        // COUNT
        // --------------------------------------------------------------
        .def("count",
             [](const rkv::Database& db, int32_t mode) {
                return db.count(mode);
             }, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT
        // --------------------------------------------------------------
        .def("put",
             static_cast<void(*)(const rkv::Database&, const py::buffer&,
                         const py::buffer&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put",
             static_cast<void(*)(const rkv::Database&, const std::string&,
                         const py::buffer&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put",
             static_cast<void(*)(const rkv::Database&, const std::string&,
                         const std::string&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT_MULTI
        // --------------------------------------------------------------
        .def("put_multi",
             static_cast<void(*)(const rkv::Database&,
                const std::vector<std::pair<py::buffer,py::buffer>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put_multi",
             static_cast<void(*)(const rkv::Database&,
                const std::vector<std::pair<std::string,py::buffer>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put_multi",
             static_cast<void(*)(const rkv::Database&,
                const std::vector<std::pair<std::string,std::string>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT_PACKED
        // --------------------------------------------------------------
        .def("put_packed",
             [](const rkv::Database& db, const py::buffer& keys,
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
             }, "keys"_a, "key_sizes"_a, "values"_a, "value_sizes"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET
        // --------------------------------------------------------------
        .def("get",
             static_cast<size_t(*)(const rkv::Database&, const py::buffer&,
                py::buffer&, int32_t)>(&get_helper),
             "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("get",
             static_cast<size_t(*)(const rkv::Database&, const std::string&,
                py::buffer&, int32_t)>(&get_helper),
             "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET_MULTI
        // --------------------------------------------------------------
        .def("get_multi",
             static_cast<py::list(*)(const rkv::Database&,
                const std::vector<std::pair<py::buffer, py::buffer>>&,
                int32_t)>(&get_multi_helper),
             "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("get_multi",
             static_cast<py::list(*)(const rkv::Database&,
                const std::vector<std::pair<std::string, py::buffer>>&,
                int32_t)>(&get_multi_helper),
             "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET_PACKED
        // --------------------------------------------------------------
        .def("get_packed",
             [](const rkv::Database& db, const py::buffer& keys,
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
                    if(val_sizes[i] != RKV_KEY_NOT_FOUND)
                        result.append(val_sizes[i]);
                    else
                        result.append(py::none());
                }
                return result;
             }, "keys"_a, "key_sizes"_a, "values"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS
        // --------------------------------------------------------------
        .def("exists",
             static_cast<bool(*)(const rkv::Database&, const std::string&, int32_t)>(&exists_helper),
             "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("exists",
             static_cast<bool(*)(const rkv::Database&, const py::buffer&, int32_t)>(&exists_helper),
             "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS_MULTI
        // --------------------------------------------------------------
        .def("exists_multi",
             static_cast<std::vector<bool>(*)(const rkv::Database&,
                 const std::vector<std::string>&, int32_t)>(&exists_multi_helper),
             "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("exists_multi",
             static_cast<std::vector<bool>(*)(const rkv::Database&,
                 const std::vector<py::buffer>&, int32_t)>(&exists_multi_helper),
             "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS_PACKED
        // --------------------------------------------------------------
        .def("exists_packed",
             [](const rkv::Database& db, const py::buffer& keys,
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
             }, "keys"_a, "key_sizes"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH
        // --------------------------------------------------------------
        .def("length",
             static_cast<size_t(*)(const rkv::Database&, const std::string&, int32_t)>(&length_helper),
             "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("length",
             static_cast<size_t(*)(const rkv::Database&, const py::buffer&, int32_t)>(&length_helper),
             "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_MULTI
        // --------------------------------------------------------------
        .def("length_multi",
             static_cast<py::list(*)(const rkv::Database&,
                         const std::vector<std::string>&,
                         int32_t)>(&length_multi_helper),
             "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("length_multi",
             static_cast<py::list(*)(const rkv::Database&,
                         const std::vector<py::buffer>&,
                         int32_t)>(&length_multi_helper),
             "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_PACKED
        // --------------------------------------------------------------
        .def("length_packed",
             [](const rkv::Database& db, const py::buffer& keys,
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
                    if(val_sizes[i] != RKV_KEY_NOT_FOUND)
                        result.append(val_sizes[i]);
                    else
                        result.append(py::none());
                }
                return result;
             }, "keys"_a, "key_sizes"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE
        // --------------------------------------------------------------
        .def("erase",
             static_cast<void(*)(const rkv::Database&, const std::string&, int32_t)>(&erase_helper),
             "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("erase",
             static_cast<void(*)(const rkv::Database&, const py::buffer&, int32_t)>(&erase_helper),
             "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_MULTI
        // --------------------------------------------------------------
        .def("erase_multi",
             static_cast<void(*)(const rkv::Database&,
                                 const std::vector<std::string>&,
                                 int32_t)>(&erase_multi_helper),
             "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("erase_multi",
             static_cast<void(*)(const rkv::Database&,
                                 const std::vector<py::buffer>&,
                                 int32_t)>(&erase_multi_helper),
             "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_PACKED
        // --------------------------------------------------------------
        .def("erase_packed",
             [](const rkv::Database& db, const py::buffer& keys,
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
             }, "keys"_a, "key_sizes"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYS
        // --------------------------------------------------------------
        .def("list_keys",
             static_cast<py::list(*)(const rkv::Database&,
                std::vector<py::buffer>&,
                const py::buffer&,
                const py::buffer&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const rkv::Database&,
                std::vector<py::buffer>&,
                const py::buffer&,
                const std::string&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const rkv::Database&,
                std::vector<py::buffer>&,
                const std::string&,
                const py::buffer&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const rkv::Database&,
                std::vector<py::buffer>&,
                const std::string&,
                const std::string&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYS_PACKED
        // --------------------------------------------------------------
        .def("list_keys_packed",
             static_cast<py::list(*)(const rkv::Database&,
                py::buffer&, size_t, const py::buffer&,
                const py::buffer&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const rkv::Database&,
                py::buffer&, size_t, const std::string&,
                const py::buffer&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const rkv::Database&,
                py::buffer&, size_t, const py::buffer&,
                const std::string&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const rkv::Database&,
                py::buffer&, size_t, const std::string&,
                const std::string&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYVALS
        // --------------------------------------------------------------
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const py::buffer&, const py::buffer&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const std::string&, const py::buffer&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const py::buffer&, const std::string&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const std::string&, const std::string&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYVALS_PACKED
        // --------------------------------------------------------------
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&, py::buffer&, py::buffer&, size_t,
                const py::buffer&, const py::buffer&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&, py::buffer&, py::buffer&, size_t,
                const std::string&, const py::buffer&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&, py::buffer&, py::buffer&, size_t,
                const py::buffer&, const std::string&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const rkv::Database&, py::buffer&, py::buffer&, size_t,
                const std::string&, const std::string&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=RKV_MODE_DEFAULT)
    ;
}


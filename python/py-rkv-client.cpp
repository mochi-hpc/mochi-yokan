#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <rkv/cxx/rkv-client.hpp>
#include <rkv/cxx/rkv-database.hpp>
#include <iostream>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t", nullptr)

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

PYBIND11_MODULE(pyrkv_client, m) {
    m.doc() = "Python binding for the RKV client library";

    py::module::import("pyrkv_common");

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
             [](const rkv::Database& db, const py::buffer& key,
                const py::buffer& val, int32_t mode) {
                auto key_info = key.request();
                auto val_info = val.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                db.put(key_info.ptr,
                       key_info.itemsize*key_info.size,
                       val_info.ptr,
                       val_info.itemsize*val_info.size,
                       mode);
             }, "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put",
             [](const rkv::Database& db, const std::string& key,
                const py::buffer& val, int32_t mode) {
                auto val_info = val.request();
                CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                db.put(key.data(),
                       key.size(),
                       val_info.ptr,
                       val_info.itemsize*val_info.size,
                       mode);
             }, "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put",
             [](const rkv::Database& db, const std::string& key,
                const std::string& val, int32_t mode) {
                db.put(key.data(),
                       key.size(),
                       val.data(),
                       val.size(),
                       mode);
             }, "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT_MULTI
        // --------------------------------------------------------------
        .def("put_multi",
             [](const rkv::Database& db,
                const std::vector<std::pair<py::buffer,py::buffer>>& keyvals,
                int32_t mode) {
                auto count = keyvals.size();
                std::vector<const void*> keys(count);
                std::vector<const void*> vals(count);
                std::vector<size_t> key_sizes(count);
                std::vector<size_t> val_sizes(count);
                for(size_t i = 0; i < count; i++) {
                    auto key_info = keyvals[i].first.request();
                    auto val_info = keyvals[i].second.request();
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
             }, "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put_multi",
             [](const rkv::Database& db,
                const std::vector<std::pair<std::string,py::buffer>>& keyvals,
                int32_t mode) {
                auto count = keyvals.size();
                std::vector<const void*> keys(count);
                std::vector<const void*> vals(count);
                std::vector<size_t> key_sizes(count);
                std::vector<size_t> val_sizes(count);
                for(size_t i = 0; i < count; i++) {
                    auto& key = keyvals[i].first;
                    auto val_info = keyvals[i].second.request();
                    CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                    keys[i] = key.data();
                    vals[i] = val_info.ptr;
                    key_sizes[i] = key.size();
                    val_sizes[i] = val_info.itemsize*val_info.size;
                }
                db.putMulti(count,
                            keys.data(),
                            key_sizes.data(),
                            vals.data(),
                            val_sizes.data(),
                            mode);
             }, "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("put_multi",
             [](const rkv::Database& db,
                const std::vector<std::pair<std::string,std::string>>& keyvals,
                int32_t mode) {
                auto count = keyvals.size();
                std::vector<const void*> keys(count);
                std::vector<const void*> vals(count);
                std::vector<size_t> key_sizes(count);
                std::vector<size_t> val_sizes(count);
                for(size_t i = 0; i < count; i++) {
                    auto& key = keyvals[i].first;
                    auto& val = keyvals[i].second;
                    keys[i] = key.data();
                    vals[i] = val.data();
                    key_sizes[i] = key.size();
                    val_sizes[i] = val.size();
                }
                db.putMulti(count,
                            keys.data(),
                            key_sizes.data(),
                            vals.data(),
                            val_sizes.data(),
                            mode);
             }, "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET
        // --------------------------------------------------------------
        .def("get",
             [](const rkv::Database& db, const py::buffer& key,
                py::buffer& val, int32_t mode) {
                auto key_info = key.request();
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
             }, "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("get",
             [](const rkv::Database& db, const std::string& key,
                py::buffer& val, int32_t mode) {
                auto val_info = val.request();
                CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                CHECK_BUFFER_IS_WRITABLE(val_info);
                size_t vsize = val_info.itemsize*val_info.size;
                db.get(key.data(),
                       key.size(),
                       val_info.ptr,
                       &vsize,
                       mode);
                return vsize;
             }, "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET_MULTI
        // --------------------------------------------------------------
        .def("get_multi",
             [](const rkv::Database& db,
                const std::vector<std::pair<py::buffer, py::buffer>>& keyvals,
                int32_t mode) {
                auto count = keyvals.size();
                std::vector<const void*> keys(count);
                std::vector<void*>       vals(count);
                std::vector<size_t>      key_sizes(count);
                std::vector<size_t>      val_sizes(count);
                for(size_t i = 0; i < count; i++) {
                    auto key_info = keyvals[i].first.request();
                    auto val_info = keyvals[i].second.request();
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
                        result.append(py::object());
                    else if(val_sizes[i] == RKV_SIZE_TOO_SMALL)
                        result.append(-1);
                    else
                        result.append(val_sizes[i]);
                }
                return result;
             }, "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("get_multi",
             [](const rkv::Database& db,
                const std::vector<std::pair<std::string, py::buffer>>& keyvals,
                int32_t mode) {
                auto count = keyvals.size();
                std::vector<const void*> keys(count);
                std::vector<void*>       vals(count);
                std::vector<size_t>      key_sizes(count);
                std::vector<size_t>      val_sizes(count);
                for(size_t i = 0; i < count; i++) {
                    auto& key = keyvals[i].first;
                    auto val_info = keyvals[i].second.request();
                    CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                    CHECK_BUFFER_IS_WRITABLE(val_info);
                    keys[i] = key.data();
                    vals[i] = val_info.ptr;
                    key_sizes[i] = key.size();
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
                        result.append(py::object());
                    else if(val_sizes[i] == RKV_SIZE_TOO_SMALL)
                        result.append(-1);
                    else
                        result.append(val_sizes[i]);
                }
                return result;
             }, "pairs"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS
        // --------------------------------------------------------------
        .def("exists",
             [](const rkv::Database& db, const std::string& key,
                int32_t mode) {
                return db.exists(key.data(), key.size(), mode);
             }, "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("exists",
             [](const rkv::Database& db, const py::buffer& key,
                int32_t mode) {
                auto key_info = key.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                size_t ksize = key_info.itemsize*key_info.size;
                return db.exists(key_info.ptr, ksize, mode);
             }, "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS_MULTI
        // --------------------------------------------------------------
        .def("exists_multi",
             [](const rkv::Database& db, const std::vector<std::string>& keys,
                int32_t mode) {
                const auto count = keys.size();
                std::vector<const void*> key_ptrs(count);
                std::vector<size_t>      key_size(count);
                for(size_t i = 0; i < count; i++) {
                    key_ptrs[i] = keys[i].data();
                    key_size[i] = keys[i].size();
                }
                return db.existsMulti(count, key_ptrs.data(), key_size.data(), mode);
             }, "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("exists_multi",
             [](const rkv::Database& db, const std::vector<py::buffer>& keys,
                int32_t mode) {
                const auto count = keys.size();
                std::vector<const void*> key_ptrs(count);
                std::vector<size_t>      key_size(count);
                for(size_t i = 0; i < count; i++) {
                    auto key_info = keys[i].request();
                    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                    key_ptrs[i] = key_info.ptr;
                    key_size[i] = key_info.itemsize*key_info.size;
                }
                return db.existsMulti(count, key_ptrs.data(), key_size.data(), mode);
             }, "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH
        // --------------------------------------------------------------
        .def("length",
             [](const rkv::Database& db, const std::string& key,
                int32_t mode) {
                return db.length(key.data(), key.size(), mode);
             }, "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("length",
             [](const rkv::Database& db, const py::buffer& key,
                int32_t mode) {
                auto key_info = key.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                size_t ksize = key_info.itemsize*key_info.size;
                return db.length(key_info.ptr, ksize, mode);
             }, "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_MULTI
        // --------------------------------------------------------------
        .def("length_multi",
             [](const rkv::Database& db, const std::vector<std::string>& keys,
                int32_t mode) {
                const auto count = keys.size();
                std::vector<const void*> key_ptrs(count);
                std::vector<size_t>      key_size(count);
                std::vector<size_t>      val_size(count);
                for(size_t i = 0; i < count; i++) {
                    key_ptrs[i] = keys[i].data();
                    key_size[i] = keys[i].size();
                }
                db.lengthMulti(count, key_ptrs.data(), key_size.data(), val_size.data(), mode);
                py::list result;
                for(size_t i = 0; i < count; i++) {
                    if(val_size[i] != RKV_KEY_NOT_FOUND)
                        result.append(val_size[i]);
                    else
                        result.append(py::object());
                }
                return result;
             }, "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("length_multi",
             [](const rkv::Database& db, const std::vector<py::buffer>& keys,
                int32_t mode) {
                const auto count = keys.size();
                std::vector<const void*> key_ptrs(count);
                std::vector<size_t>      key_size(count);
                std::vector<size_t>      val_size(count);
                for(size_t i = 0; i < count; i++) {
                    auto key_info = keys[i].request();
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
                        result.append(py::object());
                }
                return result;
             }, "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE
        // --------------------------------------------------------------
        .def("erase",
             [](const rkv::Database& db, const std::string& key,
                int32_t mode) {
                db.erase(key.data(), key.size(), mode);
             }, "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("erase",
             [](const rkv::Database& db, const py::buffer& key,
                int32_t mode) {
                auto key_info = key.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                size_t ksize = key_info.itemsize*key_info.size;
                db.erase(key_info.ptr, ksize, mode);
             }, "key"_a, "mode"_a=RKV_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_MULTI
        // --------------------------------------------------------------
        .def("erase_multi",
             [](const rkv::Database& db, const std::vector<std::string>& keys,
                int32_t mode) {
                const auto count = keys.size();
                std::vector<const void*> key_ptrs(count);
                std::vector<size_t>      key_size(count);
                for(size_t i = 0; i < count; i++) {
                    key_ptrs[i] = keys[i].data();
                    key_size[i] = keys[i].size();
                }
                db.eraseMulti(count, key_ptrs.data(), key_size.data(), mode);
             }, "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        .def("erase_multi",
             [](const rkv::Database& db, const std::vector<py::buffer>& keys,
                int32_t mode) {
                const auto count = keys.size();
                std::vector<const void*> key_ptrs(count);
                std::vector<size_t>      key_size(count);
                for(size_t i = 0; i < count; i++) {
                    auto key_info = keys[i].request();
                    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                    key_ptrs[i] = key_info.ptr;
                    key_size[i] = key_info.itemsize*key_info.size;
                }
                return db.eraseMulti(count, key_ptrs.data(), key_size.data(), mode);
             }, "keys"_a, "mode"_a=RKV_MODE_DEFAULT)
        ;
}


#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <rkv/cxx/rkv-client.hpp>
#include <rkv/cxx/rkv-database.hpp>

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
             "address"_a, "provider_id"_a, "database_id"_a,
             py::keep_alive<0,1>());

    py::class_<rkv::Database>(m, "Database")

        .def("count",
             [](const rkv::Database& db, int32_t mode) {
                return db.count(mode);
             }, "mode"_a=RKV_MODE_DEFAULT)

        .def("put",
             [](const rkv::Database& db, py::buffer key,
                py::buffer val, int32_t mode) {
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

        .def("get",
             [](const rkv::Database& db, py::buffer key,
                py::buffer val, int32_t mode) {
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
             }, "key"_a, "value"_a, "mode"_a=RKV_MODE_DEFAULT);
}


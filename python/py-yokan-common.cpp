#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <yokan/cxx/exception.hpp>

namespace py = pybind11;
using namespace pybind11::literals;

PYBIND11_MODULE(pyyokan_common, m) {
    m.doc() = "Python binding for common stuff in the YOKAN library";

    py::register_exception<yokan::Exception>(m, "Exception");

    m.attr("YOKAN_MODE_DEFAULT")      = YOKAN_MODE_DEFAULT;
    m.attr("YOKAN_MODE_INCLUSIVE")    = YOKAN_MODE_INCLUSIVE;
    m.attr("YOKAN_MODE_APPEND")       = YOKAN_MODE_APPEND;
    m.attr("YOKAN_MODE_CONSUME")      = YOKAN_MODE_CONSUME;
    m.attr("YOKAN_MODE_WAIT")         = YOKAN_MODE_WAIT;
    m.attr("YOKAN_MODE_NOTIFY")       = YOKAN_MODE_NOTIFY;
    m.attr("YOKAN_MODE_NEW_ONLY")     = YOKAN_MODE_NEW_ONLY;
    m.attr("YOKAN_MODE_EXIST_ONLY")   = YOKAN_MODE_EXIST_ONLY;
    m.attr("YOKAN_MODE_NO_PREFIX")    = YOKAN_MODE_NO_PREFIX;
    m.attr("YOKAN_MODE_IGNORE_KEYS")  = YOKAN_MODE_IGNORE_KEYS;
    m.attr("YOKAN_MODE_KEEP_LAST")    = YOKAN_MODE_KEEP_LAST;
    m.attr("YOKAN_MODE_SUFFIX")       = YOKAN_MODE_SUFFIX;
    m.attr("YOKAN_MODE_LUA_FILTER")   = YOKAN_MODE_LUA_FILTER;
    m.attr("YOKAN_MODE_IGNORE_DOCS")  = YOKAN_MODE_IGNORE_DOCS;
    m.attr("YOKAN_MODE_FILTER_VALUE") = YOKAN_MODE_FILTER_VALUE;
    m.attr("YOKAN_MODE_LIB_FILTER")   = YOKAN_MODE_LIB_FILTER;
    m.attr("YOKAN_MODE_NO_RDMA")      = YOKAN_MODE_NO_RDMA;

    py::class_<yk_database_id_t>(m, "DatabaseID")
        .def(py::init<>())
        .def("__str__", [](const yk_database_id_t& id) {
            char id_str[37];
            yk_database_id_to_string(id, id_str);
            return std::string(id_str, 36);
        })
        .def("__eq__", [](const yk_database_id_t& id1, const yk_database_id_t& id2) {
            return uuid_compare(id1.uuid, id2.uuid) == 0;
        })
        .def("__hash__", [](const yk_database_id_t& id) {
                auto data = reinterpret_cast<const uint64_t*>(id.uuid);
                uint64_t h = (~data[0]) ^ data[1];
            return h;
        })
        .def_static("from_str", [](const std::string& str) {
            if(str.size() != 36) {
                throw std::invalid_argument("string should have 36 characters");
            }
            yk_database_id_t db_id;
            yk_database_id_from_string(str.data(), &db_id);
            return db_id;
        });
}


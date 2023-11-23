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
}


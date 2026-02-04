#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/eval.h>
#include <yokan/cxx/exception.hpp>

namespace py = pybind11;
using namespace pybind11::literals;

// Static storage for the exception class (needed for exception translator)
static py::object yokan_exception_class;

PYBIND11_MODULE(pyyokan_common, m) {
    m.doc() = "Python binding for common stuff in the YOKAN library";

    // Define the Exception class in Python with a code property
    py::exec(R"(
class Exception(RuntimeError):
    def __init__(self, message, code):
        super().__init__(message)
        self._code = code

    @property
    def code(self):
        return self._code
)", m.attr("__dict__"));

    // Store reference in static variable for the translator
    yokan_exception_class = m.attr("Exception");

    // Register exception translator (lambda must not capture)
    py::register_exception_translator([](std::exception_ptr p) {
        try {
            if (p) std::rethrow_exception(p);
        } catch (const yokan::Exception &e) {
            auto exc = yokan_exception_class(e.what(), static_cast<int>(e.code()));
            PyErr_SetObject(yokan_exception_class.ptr(), exc.ptr());
        }
    });

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

    m.attr("YOKAN_SUCCESS")               = static_cast<int>(YOKAN_SUCCESS);
    m.attr("YOKAN_ERR_ALLOCATION")        = static_cast<int>(YOKAN_ERR_ALLOCATION);
    m.attr("YOKAN_ERR_INVALID_MID")       = static_cast<int>(YOKAN_ERR_INVALID_MID);
    m.attr("YOKAN_ERR_INVALID_ARGS")      = static_cast<int>(YOKAN_ERR_INVALID_ARGS);
    m.attr("YOKAN_ERR_INVALID_PROVIDER")  = static_cast<int>(YOKAN_ERR_INVALID_PROVIDER);
    m.attr("YOKAN_ERR_INVALID_DATABASE")  = static_cast<int>(YOKAN_ERR_INVALID_DATABASE);
    m.attr("YOKAN_ERR_INVALID_BACKEND")   = static_cast<int>(YOKAN_ERR_INVALID_BACKEND);
    m.attr("YOKAN_ERR_INVALID_CONFIG")    = static_cast<int>(YOKAN_ERR_INVALID_CONFIG);
    m.attr("YOKAN_ERR_INVALID_ID")        = static_cast<int>(YOKAN_ERR_INVALID_ID);
    m.attr("YOKAN_ERR_INVALID_FILTER")    = static_cast<int>(YOKAN_ERR_INVALID_FILTER);
    m.attr("YOKAN_ERR_FROM_MERCURY")      = static_cast<int>(YOKAN_ERR_FROM_MERCURY);
    m.attr("YOKAN_ERR_FROM_ARGOBOTS")     = static_cast<int>(YOKAN_ERR_FROM_ARGOBOTS);
    m.attr("YOKAN_ERR_FROM_REMI")         = static_cast<int>(YOKAN_ERR_FROM_REMI);
    m.attr("YOKAN_ERR_OP_UNSUPPORTED")    = static_cast<int>(YOKAN_ERR_OP_UNSUPPORTED);
    m.attr("YOKAN_ERR_OP_FORBIDDEN")      = static_cast<int>(YOKAN_ERR_OP_FORBIDDEN);
    m.attr("YOKAN_ERR_KEY_NOT_FOUND")     = static_cast<int>(YOKAN_ERR_KEY_NOT_FOUND);
    m.attr("YOKAN_ERR_BUFFER_SIZE")       = static_cast<int>(YOKAN_ERR_BUFFER_SIZE);
    m.attr("YOKAN_ERR_KEY_EXISTS")        = static_cast<int>(YOKAN_ERR_KEY_EXISTS);
    m.attr("YOKAN_ERR_CORRUPTION")        = static_cast<int>(YOKAN_ERR_CORRUPTION);
    m.attr("YOKAN_ERR_IO")                = static_cast<int>(YOKAN_ERR_IO);
    m.attr("YOKAN_ERR_INCOMPLETE")        = static_cast<int>(YOKAN_ERR_INCOMPLETE);
    m.attr("YOKAN_ERR_TIMEOUT")           = static_cast<int>(YOKAN_ERR_TIMEOUT);
    m.attr("YOKAN_ERR_ABORTED")           = static_cast<int>(YOKAN_ERR_ABORTED);
    m.attr("YOKAN_ERR_BUSY")              = static_cast<int>(YOKAN_ERR_BUSY);
    m.attr("YOKAN_ERR_EXPIRED")           = static_cast<int>(YOKAN_ERR_EXPIRED);
    m.attr("YOKAN_ERR_TRY_AGAIN")         = static_cast<int>(YOKAN_ERR_TRY_AGAIN);
    m.attr("YOKAN_ERR_SYSTEM")            = static_cast<int>(YOKAN_ERR_SYSTEM);
    m.attr("YOKAN_ERR_CANCELED")          = static_cast<int>(YOKAN_ERR_CANCELED);
    m.attr("YOKAN_ERR_PERMISSION")        = static_cast<int>(YOKAN_ERR_PERMISSION);
    m.attr("YOKAN_ERR_MODE")              = static_cast<int>(YOKAN_ERR_MODE);
    m.attr("YOKAN_ERR_NONCONTIG")         = static_cast<int>(YOKAN_ERR_NONCONTIG);
    m.attr("YOKAN_ERR_READONLY")          = static_cast<int>(YOKAN_ERR_READONLY);
    m.attr("YOKAN_ERR_MIGRATED")          = static_cast<int>(YOKAN_ERR_MIGRATED);
    m.attr("YOKAN_ERR_MID_NOT_LISTENING") = static_cast<int>(YOKAN_ERR_MID_NOT_LISTENING);
    m.attr("YOKAN_STOP_ITERATION")        = static_cast<int>(YOKAN_STOP_ITERATION);
    m.attr("YOKAN_ERR_OTHER")             = static_cast<int>(YOKAN_ERR_OTHER);
}


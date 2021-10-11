#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <yokan/cxx/server.hpp>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t", nullptr)

PYBIND11_MODULE(pyyokan_server, m) {
    m.doc() = "Python binding for the RKV server library";

    py::module::import("pyyokan_common");

    py::class_<rkv::Provider>(m, "Provider")
        .def(py::init<py_margo_instance_id,
                      uint16_t,
                      const char*,
                      const char*>(),
             "mid"_a,
             "provider_id"_a,
             "token"_a="",
             "config"_a="{}");
}

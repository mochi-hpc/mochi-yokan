#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <yokan/cxx/server.hpp>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id")
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t")

PYBIND11_MODULE(pyyokan_server, m) {
    m.doc() = "Python binding for the YOKAN server library";

    py::module::import("pyyokan_common");

    py::class_<yokan::Provider>(m, "Provider")
        .def(py::init<py_margo_instance_id,
                      uint16_t,
                      const char*>(),
             "mid"_a,
             "provider_id"_a,
             "config"_a)
        .def(py::init([](py::object engine, uint16_t provider_id, const char* config) {
            // Get mid from engine
            py::object mid_attr = engine.attr("mid");
            py::object mid;
            if (PyCallable_Check(mid_attr.ptr())) {
                mid = mid_attr();
            } else {
                mid = mid_attr;
            }
            py::capsule mid_capsule = mid.cast<py::capsule>();

            // Create provider
            return new yokan::Provider(mid_capsule, provider_id, config);
        }), "engine"_a, "provider_id"_a, "config"_a);
}

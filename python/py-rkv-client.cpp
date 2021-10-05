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

PYBIND11_MODULE(pyrkv_client, m) {
    m.doc() = "Python binding for the RKV client library";

    py::register_exception<rkv::Exception>(m, "ClientException");

    py::class_<rkv::Client>(m, "Client")
        .def(py::init<py_margo_instance_id>());
}


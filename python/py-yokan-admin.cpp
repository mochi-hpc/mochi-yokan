#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <yokan/cxx/admin.hpp>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t", nullptr)

PYBIND11_MODULE(pyyokan_admin, m) {
    m.doc() = "Python binding for the YOKAN admin library";

    py::module::import("pyyokan_common");

    py::class_<yokan::Admin>(m, "Admin")
        .def(py::init<py_margo_instance_id>(),
             "mid"_a)
        .def("open_database",
             [](const yokan::Admin& admin,
                py_hg_addr_t address,
                uint16_t provider_id,
                const char* token,
                const char* type,
                const char* config) {
                    return admin.openDatabase(
                        address, provider_id, token, type, config);
             },
             "address"_a, "provider_id"_a, "token"_a,
             "type"_a, "config"_a)
        .def("close_database",
             [](const yokan::Admin& admin,
                py_hg_addr_t address,
                uint16_t provider_id,
                const char* token,
                yk_database_id_t id) {
                    return admin.closeDatabase(
                        address, provider_id, token, id);
             },
             "address"_a, "provider_id"_a, "token"_a,
             "database_id"_a)
        .def("destroy_database",
             [](const yokan::Admin& admin,
                py_hg_addr_t address,
                uint16_t provider_id,
                const char* token,
                yk_database_id_t id) {
                    return admin.destroyDatabase(
                        address, provider_id, token, id);
             },
             "address"_a, "provider_id"_a, "token"_a,
             "database_id"_a)
        .def("list_databases",
             [](const yokan::Admin& admin,
                py_hg_addr_t address,
                uint16_t provider_id,
                const char* token) {
                    return admin.listDatabases(
                        address, provider_id, token);
             },
             "address"_a, "provider_id"_a, "token"_a);
}


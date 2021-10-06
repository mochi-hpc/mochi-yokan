add_library (pyrkv_common MODULE py-rkv-common.cpp)
target_link_libraries (pyrkv_common PRIVATE pybind11::module rkv-headers)
pybind11_extension (pyrkv_common)
pybind11_strip (pyrkv_common)

add_library (pyrkv_client MODULE py-rkv-client.cpp)
target_link_libraries (pyrkv_client PRIVATE pybind11::module rkv-client)
pybind11_extension (pyrkv_client)
pybind11_strip (pyrkv_client)

add_library (pyrkv_admin MODULE py-rkv-admin.cpp)
target_link_libraries (pyrkv_admin PRIVATE pybind11::module rkv-admin)
pybind11_extension (pyrkv_admin)
pybind11_strip (pyrkv_admin)

add_library (pyrkv_server MODULE py-rkv-server.cpp)
target_link_libraries (pyrkv_server PRIVATE pybind11::module rkv-server)
pybind11_extension (pyrkv_server)
pybind11_strip (pyrkv_server)

install (TARGETS pyrkv_common pyrkv_client pyrkv_admin pyrkv_server
         EXPORT rkv-targets
         ARCHIVE DESTINATION lib/python3/dist-packages
         LIBRARY DESTINATION lib/python3/dist-packages)

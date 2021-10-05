add_library (pyrkv_client MODULE py-rkv-client.cpp)
target_link_libraries (pyrkv_client PRIVATE pybind11::module rkv-client)
pybind11_extension (pyrkv_client)
pybind11_strip (pyrkv_client)

add_library (pyrkv_admin MODULE py-rkv-admin.cpp)
target_link_libraries (pyrkv_admin PRIVATE pybind11::module rkv-admin)
pybind11_extension (pyrkv_admin)
pybind11_strip (pyrkv_admin)

install (TARGETS pyrkv_client pyrkv_admin
         EXPORT rkv-targets
         ARCHIVE DESTINATION lib/python3/dist-packages
         LIBRARY DESTINATION lib/python3/dist-packages)

add_library (pyyokan_common MODULE py-yokan-common.cpp)
target_link_libraries (pyyokan_common PRIVATE pybind11::module yokan-headers PRIVATE coverage_config)
pybind11_extension (pyyokan_common)
pybind11_strip (pyyokan_common)

add_library (pyyokan_client MODULE py-yokan-client.cpp)
target_link_libraries (pyyokan_client PRIVATE pybind11::module yokan-client PRIVATE coverage_config)
pybind11_extension (pyyokan_client)
pybind11_strip (pyyokan_client)

add_library (pyyokan_server MODULE py-yokan-server.cpp)
target_link_libraries (pyyokan_server PRIVATE pybind11::module yokan-server PRIVATE coverage_config)
pybind11_extension (pyyokan_server)
pybind11_strip (pyyokan_server)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

install (TARGETS pyyokan_common pyyokan_client pyyokan_server
         EXPORT yokan-targets
         ARCHIVE DESTINATION lib/python${PY_VERSION}/site-packages
         LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)

add_executable (example-server ${CMAKE_CURRENT_SOURCE_DIR}/server.c)
target_link_libraries (example-server rkv-server)

add_executable (example-admin ${CMAKE_CURRENT_SOURCE_DIR}/admin.c)
target_link_libraries (example-admin rkv-admin)

add_executable (example-client ${CMAKE_CURRENT_SOURCE_DIR}/client.c)
target_link_libraries (example-client rkv-client)

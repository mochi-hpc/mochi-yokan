include_directories (${CMAKE_CURRENT_SOURCE_DIR}/munit)

file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/test-*.c)

foreach (test-source ${test-sources})
    get_filename_component (test-target ${test-source} NAME_WE)
    add_executable (${test-target} ${test-source} munit/munit.c)
    target_include_directories (${test-target} PUBLIC
      ${CMAKE_CURRENT_SOURCE_DIR}/munit
      ${CMAKE_CURRENT_SOURCE_DIR}/../include
      ${CMAKE_CURRENT_BINARY_DIR}/../src
    )
    target_link_libraries (${test-target} rkv-server rkv-admin rkv-client)
    add_test (NAME ${test-target} COMMAND ./${test-target})
endforeach ()

#add_executable (test-admin test-admin.c munit/munit.c)
#target_include_directories (test-admin PUBLIC
#  ${CMAKE_CURRENT_SOURCE_DIR}/munit
#  ${CMAKE_CURRENT_SOURCE_DIR}/../include
#  ${CMAKE_CURRENT_BINARY_DIR}/../src
#)
#target_link_libraries (test-admin rkv-server rkv-admin)

#add_executable (test-client test-client.c munit/munit.c)
#target_include_directories (test-client PUBLIC
#  ${CMAKE_CURRENT_SOURCE_DIR}/munit
#  ${CMAKE_CURRENT_SOURCE_DIR}/../include
#  ${CMAKE_CURRENT_BINARY_DIR}/../src
#)
#target_link_libraries (test-client rkv-server rkv-admin rkv-client)

#add_executable (test-put test-client.c munit/munit.c)
#target_include_directories (test-client PUBLIC
#  ${CMAKE_CURRENT_SOURCE_DIR}/munit
#  ${CMAKE_CURRENT_SOURCE_DIR}/../include
#  ${CMAKE_CURRENT_BINARY_DIR}/../src
#)
#target_link_libraries (test-client rkv-server rkv-admin rkv-client)

#add_test (NAME TestAdmin COMMAND ./test-admin)
#add_test (NAME TestClient COMMAND ./test-client)

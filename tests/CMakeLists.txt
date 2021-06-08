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
    target_link_libraries (${test-target}
        rkv-server
        rkv-admin
        rkv-client)
    add_test (NAME ${test-target} COMMAND ./${test-target})
endforeach ()

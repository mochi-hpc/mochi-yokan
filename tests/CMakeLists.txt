include_directories (${CMAKE_CURRENT_SOURCE_DIR}/munit)

file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/test-*.cpp)

foreach (test-source ${test-sources})
    get_filename_component (test-target ${test-source} NAME_WE)
    add_executable (${test-target} ${test-source} munit/munit.c)
    target_include_directories (${test-target} PUBLIC
      ${CMAKE_CURRENT_SOURCE_DIR}/munit
      ${CMAKE_CURRENT_SOURCE_DIR}/../include
      ${CMAKE_CURRENT_BINARY_DIR}/../src
    )
    target_link_libraries (${test-target}
        yokan::server
        yokan::client
        nlohmann_json::nlohmann_json)
    add_test (NAME ${test-target} COMMAND timeout 600 ${CMAKE_CURRENT_SOURCE_DIR}/run-test.sh ./${test-target})
endforeach ()

if (ENABLE_PYTHON)
    file (GLOB test-python-sources ${CMAKE_CURRENT_SOURCE_DIR}/python/test-*.py)
    foreach (test-python-source ${test-python-sources})
        get_filename_component (test-target ${test-python-source} NAME_WE)
        set (test-target "${test-target}-python")
        add_test (NAME ${test-target} COMMAND ${Python3_EXECUTABLE} ${test-python-source})
    endforeach ()
endif ()

add_library (custom-filters SHARED
             ${CMAKE_CURRENT_SOURCE_DIR}/extra/custom-filters.cpp)
target_link_libraries (custom-filters yokan-server)

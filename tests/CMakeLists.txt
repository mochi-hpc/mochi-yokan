include_directories (${CMAKE_CURRENT_SOURCE_DIR}/munit)

file (GLOB test-sources ${CMAKE_CURRENT_SOURCE_DIR}/test-*.cpp)

add_library (custom-filters SHARED
             ${CMAKE_CURRENT_SOURCE_DIR}/extra/custom-filters.cpp)
target_link_libraries (custom-filters yokan-server)

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

# Set the path to the python directory
set (PYTHON_MODULE_DIR ${CMAKE_SOURCE_DIR}/python)
# Use file(GLOB_RECURSE ...) to find all files matching the test_*.py pattern
file (GLOB_RECURSE PYTHON_TEST_FILES "${PYTHON_MODULE_DIR}/test_*.py")

foreach (PYTHON_TEST_FILE ${PYTHON_TEST_FILES})
    # Remove the directory part
    file (RELATIVE_PATH PYTHON_TEST_FILE_REL ${PYTHON_MODULE_DIR} ${PYTHON_TEST_FILE})
    # Remove the file extension
    string (REPLACE ".py" "" PYTHON_TEST_FILE_BASE ${PYTHON_TEST_FILE_REL})
    # Replace slashes with dots
    string (REPLACE "/" "." PYTHON_TEST_NAME ${PYTHON_TEST_FILE_BASE})
    # Add the test
    if (${ENABLE_COVERAGE})
        message (STATUS "${PYTHON_TEST_NAME} test will run with code coverage")
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND coverage run -a -m unittest ${PYTHON_TEST_NAME})
    else ()
        add_test (NAME ${PYTHON_TEST_NAME} COMMAND python -m unittest ${PYTHON_TEST_NAME})
    endif ()
    set_property (TEST ${PYTHON_TEST_NAME} PROPERTY ENVIRONMENT
                  PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH})
endforeach()

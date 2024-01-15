# (C) 2021 The University of Chicago
# See COPYRIGHT in top-level directory.
cmake_minimum_required (VERSION 3.15)
project (yokan C CXX)
enable_testing ()

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set (CMAKE_CXX_STANDARD 14)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -Wall -Wpedantic")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wextra -Wall -Wpedantic")

add_library (coverage_config INTERFACE)

option (ENABLE_COVERAGE   "Enable coverage reporting" OFF)
option (ENABLE_TESTS      "Build tests" OFF)
option (ENABLE_BENCHMARK  "Build benchmark" OFF)
option (ENABLE_BEDROCK    "Build bedrock module" OFF)
option (ENABLE_LEVELDB    "Build with leveldb support" OFF)
option (ENABLE_ROCKSDB    "Build with rocksdb support" OFF)
option (ENABLE_BERKELEYDB "Build with berkeleydb support" OFF)
option (ENABLE_TKRZW      "Build with tkrzw support" OFF)
option (ENABLE_LMDB       "Build with lmdb support" OFF)
option (ENABLE_GDBM       "Build with gdbm support" OFF)
option (ENABLE_UNQLITE    "Build with unqlite support" OFF)
option (ENABLE_LUA        "Build with Lua support" OFF)
option (ENABLE_PYTHON     "Build the Python module" OFF)
option (ENABLE_YCSB       "Build the YCSB adaptor" OFF)
option (ENABLE_REMI       "Build with REMI support" OFF)

if (ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options (coverage_config INTERFACE
      -O0        # no optimization
      -g         # generate debug info
      --coverage # sets all required flags
  )
  if (CMAKE_VERSION VERSION_GREATER_EQUAL 3.13)
    target_link_options (coverage_config INTERFACE --coverage)
  else ()
    target_link_libraries (coverage_config INTERFACE --coverage)
  endif ()
endif ()

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "ON" CACHE BOOL "Build a shared library")

find_package (nlohmann_json REQUIRED)
find_package (PkgConfig REQUIRED)

if (${ENABLE_BEDROCK})
    find_package (bedrock REQUIRED)
endif ()

if (${ENABLE_REMI})
    find_package (remi REQUIRED)
    set (YOKAN_HAS_REMI ON)
else ()
    set (YOKAN_HAS_REMI OFF)
endif ()

# search for margo
pkg_check_modules (margo REQUIRED IMPORTED_TARGET margo)
# search for tclap
pkg_check_modules (tclap REQUIRED IMPORTED_TARGET tclap)

if (ENABLE_LEVELDB)
    pkg_check_modules (leveldb REQUIRED IMPORTED_TARGET leveldb)
    set (YOKAN_HAS_LEVELDB ON)
else ()
    set (YOKAN_HAS_LEVELDB OFF)
endif ()

if (ENABLE_ROCKSDB)
    pkg_check_modules (rocksdb REQUIRED IMPORTED_TARGET rocksdb)
    set (YOKAN_HAS_ROCKSDB ON)
    if (${rocksdb_VERSION} VERSION_GREATER_EQUAL 7)
        set (CMAKE_CXX_STANDARD 17)
    endif ()
else ()
    set (YOKAN_HAS_ROCKSDB OFF)
endif ()

if (ENABLE_BERKELEYDB)
    find_package (BerkeleyDB REQUIRED)
    set (YOKAN_HAS_BERKELEYDB ON)
else ()
    set (YOKAN_HAS_BERKELEYDB OFF)
endif ()

if (ENABLE_TKRZW)
    pkg_check_modules (tkrzw REQUIRED IMPORTED_TARGET tkrzw)
    set (YOKAN_HAS_TKRZW ON)
    set (CMAKE_CXX_STANDARD 17)
else ()
    set (YOKAN_HAS_TKRZW OFF)
endif ()

if (ENABLE_GDBM)
    find_package (GDBM REQUIRED)
    set (YOKAN_HAS_GDBM ON)
else ()
    set (YOKAN_HAS_GDBM OFF)
endif ()

if (ENABLE_LMDB)
    pkg_check_modules (lmdb REQUIRED IMPORTED_TARGET lmdb)
    set (YOKAN_HAS_LMDB ON)
else ()
    set (YOKAN_HAS_LMDB OFF)
endif ()

if (ENABLE_UNQLITE)
    find_package (UnQLite REQUIRED)
    set (YOKAN_HAS_UNQLITE ON)
else ()
    set (YOKAN_HAS_UNQLITE OFF)
endif ()

if (ENABLE_LUA)
    find_package (sol2 REQUIRED)
    find_package (Lua REQUIRED)
    set (YOKAN_HAS_LUA ON)
    set (CMAKE_CXX_STANDARD 17)
else ()
    set (YOKAN_HAS_LUA OFF)
endif ()

if (ENABLE_PYTHON)
    find_package (Python3 COMPONENTS Interpreter Development REQUIRED)
    find_package (pybind11 REQUIRED)
    add_subdirectory (python)
endif ()

# Detect std::string_view or std::experimental::string_view
try_compile (HAS_STRING_VIEW ${CMAKE_CURRENT_BINARY_DIR}
             ${CMAKE_CURRENT_SOURCE_DIR}/src/cmake-tests/has_string_view.cpp)
if (${HAS_STRING_VIEW})
    message (STATUS "Found std::string_view to be available")
    add_definitions (-DYOKAN_USE_STD_STRING_VIEW)
else ()
    try_compile (HAS_EXP_STRING_VIEW ${CMAKE_CURRENT_BINARY_DIR}
                 ${CMAKE_CURRENT_SOURCE_DIR}/src/cmake-tests/has_experimental_string_view.cpp)
    if (${HAS_EXP_STRING_VIEW})
        message (STATUS "Found std::experimental::string_view to be available")
    else ()
        message (FATAL_ERROR "Yokan needs either std::string_view or std::experimental::string_view")
    endif ()
endif ()

# Detect std::string_view or std::experimental::string_view
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.0")
        set (FS_LIB c++fs)
    endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set (FS_LIB stdc++fs)
endif ()
try_compile (HAS_FILESYSTEM ${CMAKE_CURRENT_BINARY_DIR}
             ${CMAKE_CURRENT_SOURCE_DIR}/src/cmake-tests/has_filesystem.cpp
             LINK_LIBRARIES ${FS_LIB})
    if (${HAS_FILESYSTEM})
    message (STATUS "Found std::filesystem to be available")
    add_definitions (-DYOKAN_USE_STD_FILESYSTEM)
else ()
    try_compile (HAS_EXP_FILESYSTEM ${CMAKE_CURRENT_BINARY_DIR}
                 ${CMAKE_CURRENT_SOURCE_DIR}/src/cmake-tests/has_experimental_filesystem.cpp
                 LINK_LIBRARIES ${FS_LIB})
    if (${HAS_EXP_FILESYSTEM})
        message (STATUS "Found std::experimental::filesystem to be available")
    else ()
        message (FATAL_ERROR "Yokan needs either std::filesystem or std::experimental::filesystem")
    endif ()
endif ()

if (${ENABLE_BENCHMARK})
  add_subdirectory (benchmark)
endif ()

if (${ENABLE_YCSB})
  find_package (ycsb-cpp-interface CONFIG REQUIRED)
  add_subdirectory (ycsb)
endif ()

# library version set here (e.g. for shared libs).
set (YOKAN_VERSION_MAJOR 0)
set (YOKAN_VERSION_MINOR 4)
set (YOKAN_VERSION_PATCH 1)
set (YOKAN_VERSION
     "${YOKAN_VERSION_MAJOR}.${YOKAN_VERSION_MINOR}.${YOKAN_VERSION_PATCH}")

add_subdirectory (src)
if (${ENABLE_TESTS})
  add_subdirectory (tests)
endif ()

install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindBerkeleyDB.cmake
               ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindGDBM.cmake
               ${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindUnQLite.cmake
         DESTINATION share/cmake/yokan)

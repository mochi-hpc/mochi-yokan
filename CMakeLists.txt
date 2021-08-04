# (C) 2021 The University of Chicago
# See COPYRIGHT in top-level directory.
cmake_minimum_required (VERSION 3.13)
project (rkv C CXX)
enable_testing ()

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set (CMAKE_CXX_STANDARD 14)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -Wall -Wpedantic")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wextra -Wall -Wpedantic")

add_library(coverage_config INTERFACE)

option (ENABLE_COVERAGE   "Enable coverage reporting" OFF)
option (ENABLE_TESTS      "Build tests" OFF)
option (ENABLE_BEDROCK    "Build bedrock module" OFF)
option (ENABLE_LEVELDB    "Build with leveldb support" OFF)
option (ENABLE_ROCKSDB    "Build with rocksdb support" OFF)
option (ENABLE_BERKELEYDB "Build with berkeleydb support" OFF)
option (ENABLE_TKRZW      "Build with tkrzw support" OFF)
option (ENABLE_LMDB       "Build with lmdb support" OFF)
option (ENABLE_GDBM       "Build with gdbm support" OFF)
option (ENABLE_UNQLITE    "Build with unqlite support" OFF)
option (ENABLE_LUA        "Build with Lua support" OFF)

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
    find_package(bedrock REQUIRED)
endif ()

# search for margo
pkg_check_modules (margo REQUIRED IMPORTED_TARGET margo)
# search for uuid
pkg_check_modules (uuid REQUIRED IMPORTED_TARGET uuid)
# search for json-c
pkg_check_modules (json-c REQUIRED IMPORTED_TARGET json-c)

if (ENABLE_LEVELDB)
    pkg_check_modules (leveldb REQUIRED IMPORTED_TARGET leveldb)
    set (HAS_LEVELDB ON)
endif ()

if (ENABLE_ROCKSDB)
    pkg_check_modules (rocksdb REQUIRED IMPORTED_TARGET rocksdb)
    set (HAS_ROCKSDB ON)
endif ()

if (ENABLE_BERKELEYDB)
    find_package (BerkeleyDB REQUIRED)
    set (HAS_BERKELEYDB ON)
endif ()

if (ENABLE_TKRZW)
    pkg_check_modules (tkrzw REQUIRED IMPORTED_TARGET tkrzw)
    set (HAS_TKRZW ON)
    set (CMAKE_CXX_STANDARD 17)
endif ()

if (ENABLE_GDBM)
    find_package (GDBM REQUIRED)
    set (HAS_GDBM ON)
endif ()

if (ENABLE_LMDB)
    pkg_check_modules (lmdb REQUIRED IMPORTED_TARGET lmdb)
    set (HAS_LMDB ON)
endif ()

if (ENABLE_UNQLITE)
    find_package (UnQLite REQUIRED)
    set (HAS_UNQLITE ON)
endif ()

if (ENABLE_LUA)
    find_package (sol2 REQUIRED)
    find_package (Lua REQUIRED)
    set (HAS_LUA ON)
    set (CMAKE_CXX_STANDARD 17)
endif ()

# library version set here (e.g. for shared libs).
set (RKV_VERSION_MAJOR 0)
set (RKV_VERSION_MINOR 1)
set (RKV_VERSION_PATCH 0)
set (RKV_VERSION
     "${RKV_VERSION_MAJOR}.${RKV_VERSION_MINOR}.${RKV_VERSION_PATCH}")

add_subdirectory (src)
if(${ENABLE_TESTS})
  add_subdirectory (tests)
endif(${ENABLE_TESTS})

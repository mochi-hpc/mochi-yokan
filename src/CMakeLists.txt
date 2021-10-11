# set source files
set (server-src-files
     server/provider.cpp
     server/count.cpp
     server/put.cpp
     server/erase.cpp
     server/get.cpp
     server/length.cpp
     server/exists.cpp
     server/list_keys.cpp
     server/list_keyvals.cpp
     buffer/default_bulk_cache.cpp)

set (backend-src-files
     backends/backends.cpp
     backends/map.cpp
     backends/unordered_map.cpp
     backends/set.cpp
     backends/unordered_set.cpp)

set(DB_DEPENDENCIES "")

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.0")
        list (APPEND DB_DEPENDENCIES c++fs)
    endif()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list (APPEND DB_DEPENDENCIES stdc++fs)
endif()

if (ENABLE_LEVELDB)
    list (APPEND backend-src-files backends/leveldb.cpp)
    list (APPEND DB_DEPENDENCIES PkgConfig::leveldb)
endif ()

if (ENABLE_ROCKSDB)
    list (APPEND backend-src-files backends/rocksdb.cpp)
    list (APPEND DB_DEPENDENCIES PkgConfig::rocksdb)
endif ()

if (ENABLE_BERKELEYDB)
    list (APPEND backend-src-files backends/berkeleydb.cpp)
    list (APPEND DB_DEPENDENCIES Oracle::BerkeleyDB)
endif ()

if (ENABLE_TKRZW)
    list (APPEND backend-src-files backends/tkrzw.cpp)
    list (APPEND DB_DEPENDENCIES PkgConfig::tkrzw)
endif ()

if (ENABLE_GDBM)
    list (APPEND backend-src-files backends/gdbm.cpp)
    list (APPEND DB_DEPENDENCIES ${GDBM_LIBRARIES})
    include_directories (${GDBM_INCLUDE_DIRS})
endif ()

if (ENABLE_LMDB)
    list (APPEND backend-src-files backends/lmdb.cpp)
    list (APPEND DB_DEPENDENCIES PkgConfig::lmdb)
endif ()

if (ENABLE_UNQLITE)
    list (APPEND backend-src-files backends/unqlite.cpp)
    list (APPEND DB_DEPENDENCIES ${UNQLITE_LIBRARIES})
    include_directories (${UNQLITE_INCLUDE_DIRS})
endif ()

if (ENABLE_LUA)
    list (APPEND DB_DEPENDENCIES sol2::sol2 ${LUA_LIBRARIES})
    include_directories (${LUA_INCLUDE_DIRS})
endif ()

set (client-src-files
     client/client.c
     client/count.cpp
     client/put.cpp
     client/erase.cpp
     client/get.cpp
     client/length.cpp
     client/exists.cpp
     client/list_keys.cpp
     client/list_keyvals.cpp)

set (admin-src-files
     admin/admin.c)

set (bedrock-module-src-files
     bedrock/bedrock-module.c)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (rkv-pkg "share/cmake/rkv")

# set version fo shared library
set (rkv-vers "${RKV_VERSION_MAJOR}.${RKV_VERSION_MINOR}")

# headers (necessary for the pyrkv_common target to know where to find headers
# and what to link against)
add_library (rkv-headers INTERFACE)
target_include_directories (rkv-headers INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries (rkv-headers INTERFACE PkgConfig::uuid)

# server library
add_library (rkv-server ${server-src-files} ${backend-src-files})
target_link_libraries (rkv-server
    PUBLIC PkgConfig::margo
    PUBLIC PkgConfig::uuid
    PRIVATE nlohmann_json
    PRIVATE ${DB_DEPENDENCIES}
    PRIVATE coverage_config)
target_include_directories (rkv-server PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (rkv-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (rkv-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (rkv-server
    PROPERTIES VERSION ${RKV_VERSION}
    SOVERSION ${RKV_VERSION_MAJOR})

# client library
add_library (rkv-client ${client-src-files})
target_link_libraries (rkv-client
    PUBLIC PkgConfig::margo
    PUBLIC PkgConfig::uuid
    PRIVATE coverage_config)
target_include_directories (rkv-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (rkv-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (rkv-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (rkv-client
    PROPERTIES VERSION ${RKV_VERSION}
    SOVERSION ${RKV_VERSION_MAJOR})

# admin library
add_library (rkv-admin ${admin-src-files})
target_link_libraries (rkv-admin
    PUBLIC PkgConfig::margo
    PUBLIC PkgConfig::uuid
    PRIVATE coverage_config)
target_include_directories (rkv-admin PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (rkv-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (rkv-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (rkv-admin
    PROPERTIES VERSION ${RKV_VERSION}
    SOVERSION ${RKV_VERSION_MAJOR})

if (${ENABLE_BEDROCK})
# bedrock-module library
add_library (rkv-bedrock-module ${bedrock-module-src-files})
target_link_libraries (rkv-bedrock-module
    PUBLIC rkv-server
    PUBLIC rkv-admin
    PUBLIC rkv-client
    PUBLIC bedrock-server
    PRIVATE coverage_config)
target_include_directories (rkv-bedrock-module PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (rkv-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (rkv-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (rkv-bedrock-module
    PROPERTIES VERSION ${RKV_VERSION}
    SOVERSION ${RKV_VERSION_MAJOR})
endif ()

# installation stuff (packaging and install commands)
write_basic_package_version_file(
    "rkv-config-version.cmake"
    VERSION ${RKV_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set(INCLUDE_INSTALL_DIR include/)
set(LIB_INSTALL_DIR lib/)
configure_package_config_file(rkv-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/rkv-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/rkv/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (SERVER_PRIVATE_LIBS "-lrkv-server")
set (CLIENT_PRIVATE_LIBS "-lrkv-client")
set (ADMIN_PRIVATE_LIBS  "-lrkv-admin")
configure_file ("rkv-server.pc.in" "rkv-server.pc" @ONLY)
configure_file ("rkv-client.pc.in" "rkv-client.pc" @ONLY)
configure_file ("rkv-admin.pc.in"  "rkv-admin.pc"  @ONLY)

# configure config.h
configure_file ("config.h.in" "config.h" @ONLY)

# "make install" rules
install (TARGETS rkv-server rkv-client rkv-admin
         EXPORT rkv-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if (${ENABLE_BEDROCK})
    install (TARGETS rkv-bedrock-module
             ARCHIVE DESTINATION lib
             LIBRARY DESTINATION lib)
endif ()
install (EXPORT rkv-targets
         DESTINATION ${rkv-pkg}
         FILE "rkv-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/rkv-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/rkv-config-version.cmake"
         DESTINATION ${rkv-pkg} )
install (DIRECTORY ../include/rkv
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/rkv-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/rkv-client.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/rkv-admin.pc"
         DESTINATION "lib/pkgconfig/")

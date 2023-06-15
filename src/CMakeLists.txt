# set source files
set (server-src-files
     server/provider.cpp
     server/count.cpp
     server/put.cpp
     server/erase.cpp
     server/get.cpp
     server/fetch.cpp
     server/length.cpp
     server/exists.cpp
     server/list_keys.cpp
     server/list_keyvals.cpp
     server/iter.cpp
     server/coll_create.cpp
     server/coll_drop.cpp
     server/coll_exists.cpp
     server/coll_last_id.cpp
     server/coll_size.cpp
     server/doc_erase.cpp
     server/doc_store.cpp
     server/doc_update.cpp
     server/doc_load.cpp
     server/doc_fetch.cpp
     server/doc_length.cpp
     server/doc_list.cpp
     server/util/filters.cpp
     buffer/default_bulk_cache.cpp
     buffer/lru_bulk_cache.cpp
     buffer/keep_all_bulk_cache.cpp)

if (ENABLE_LUA)
     list (APPEND server-src-files
           server/util/lua-cjson/fpconv.c
           server/util/lua-cjson/lua_cjson.c
           server/util/lua-cjson/strbuf.c)
endif (ENABLE_LUA)

set (backend-src-files
     backends/backends.cpp
     backends/null.cpp
     backends/map.cpp
     backends/unordered_map.cpp
     backends/set.cpp
     backends/unordered_set.cpp)

set (DB_DEPENDENCIES "")

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.0")
        list (APPEND DB_DEPENDENCIES c++fs)
    endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list (APPEND DB_DEPENDENCIES stdc++fs)
endif ()

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
     client/client.cpp
     client/find_by_name.cpp
     client/count.cpp
     client/put.cpp
     client/erase.cpp
     client/get.cpp
     client/fetch.cpp
     client/length.cpp
     client/exists.cpp
     client/list_keys.cpp
     client/list_keyvals.cpp
     client/iter.cpp
     client/coll_create.cpp
     client/coll_drop.cpp
     client/coll_exists.cpp
     client/coll_last_id.cpp
     client/coll_size.cpp
     client/doc_erase.cpp
     client/doc_store.cpp
     client/doc_update.cpp
     client/doc_length.cpp
     client/doc_load.cpp
     client/doc_fetch.cpp
     client/doc_list.cpp)

set (admin-src-files
     admin/admin.c)

set (bedrock-module-src-files
     bedrock/bedrock-module.c)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (yokan-pkg "share/cmake/yokan")

# set version fo shared library
set (yokan-vers "${YOKAN_VERSION_MAJOR}.${YOKAN_VERSION_MINOR}")

# headers (necessary for the pyyokan_common target to know where to find headers
# and what to link against)
add_library (yokan-headers INTERFACE)
target_include_directories (yokan-headers INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries (yokan-headers INTERFACE PkgConfig::uuid)

if (ENABLE_REMI)
  set (OPTIONAL_REMI remi)
else ()
  set (OPTIONAL_REMI)
endif ()

# server library
add_library (yokan-server ${server-src-files} ${backend-src-files})
target_link_libraries (yokan-server
    PUBLIC PkgConfig::margo PkgConfig::uuid ${OPTIONAL_REMI}
    PRIVATE nlohmann_json ${DB_DEPENDENCIES} coverage_config)
target_compile_definitions (yokan-server PRIVATE -DJSON_HAS_CPP_14)
target_include_directories (yokan-server PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (yokan-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (yokan-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (yokan-server
    PROPERTIES VERSION ${YOKAN_VERSION}
    SOVERSION ${YOKAN_VERSION_MAJOR})

# client library
add_library (yokan-client ${client-src-files})
target_link_libraries (yokan-client
    PUBLIC PkgConfig::margo
    PUBLIC PkgConfig::uuid
    PRIVATE coverage_config)
target_compile_definitions (yokan-client PRIVATE -DJSON_HAS_CPP_14)
target_include_directories (yokan-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (yokan-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (yokan-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (yokan-client
    PROPERTIES VERSION ${YOKAN_VERSION}
    SOVERSION ${YOKAN_VERSION_MAJOR})

# admin library
add_library (yokan-admin ${admin-src-files})
target_link_libraries (yokan-admin
    PUBLIC PkgConfig::margo
    PUBLIC PkgConfig::uuid
    PRIVATE coverage_config)
target_compile_definitions (yokan-admin PRIVATE -DJSON_HAS_CPP_14)
target_include_directories (yokan-admin PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (yokan-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (yokan-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (yokan-admin
    PROPERTIES VERSION ${YOKAN_VERSION}
    SOVERSION ${YOKAN_VERSION_MAJOR})

if (${ENABLE_BEDROCK})
# bedrock-module library
add_library (yokan-bedrock-module ${bedrock-module-src-files})
target_link_libraries (yokan-bedrock-module
    PUBLIC yokan-server
    PUBLIC yokan-admin
    PUBLIC yokan-client
    PUBLIC bedrock-server
    PRIVATE coverage_config)
target_compile_definitions (yokan-bedrock-module PRIVATE -DJSON_HAS_CPP_14)
target_include_directories (yokan-bedrock-module PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (yokan-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (yokan-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (yokan-bedrock-module
    PROPERTIES VERSION ${YOKAN_VERSION}
    SOVERSION ${YOKAN_VERSION_MAJOR})
endif ()

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "yokan-config-version.cmake"
    VERSION ${YOKAN_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (yokan-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/yokan-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/yokan/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (SERVER_PRIVATE_LIBS "-lyokan-server")
set (CLIENT_PRIVATE_LIBS "-lyokan-client")
set (ADMIN_PRIVATE_LIBS  "-lyokan-admin")
configure_file ("yokan-server.pc.in" "yokan-server.pc" @ONLY)
configure_file ("yokan-client.pc.in" "yokan-client.pc" @ONLY)
configure_file ("yokan-admin.pc.in"  "yokan-admin.pc"  @ONLY)

# configure config.h
configure_file ("config.h.in" "config.h" @ONLY)

# "make install" rules
install (TARGETS yokan-server yokan-client yokan-admin
         EXPORT yokan-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if (${ENABLE_BEDROCK})
    install (TARGETS yokan-bedrock-module
             ARCHIVE DESTINATION lib
             LIBRARY DESTINATION lib)
endif ()
install (EXPORT yokan-targets
         DESTINATION ${yokan-pkg}
         FILE "yokan-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/yokan-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/yokan-config-version.cmake"
         DESTINATION ${yokan-pkg} )
install (DIRECTORY ../include/yokan
         DESTINATION include)
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/yokan-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/yokan-client.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/yokan-admin.pc"
         DESTINATION "lib/pkgconfig/")

add_library (yokan-ycsb SHARED ${CMAKE_CURRENT_SOURCE_DIR}/YokanYCSB.cpp)
target_link_libraries (yokan-ycsb PRIVATE YCSBCppInterface yokan-client)

install (TARGETS yokan-ycsb
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)

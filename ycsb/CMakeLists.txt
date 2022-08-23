add_library (yokan-ycsb SHARED ${CMAKE_CURRENT_SOURCE_DIR}/YokanYCSB.cpp)
target_link_libraries (yokan-ycsb PRIVATE ycsb-cpp-interface yokan-client)

install (TARGETS yokan-ycsb
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)

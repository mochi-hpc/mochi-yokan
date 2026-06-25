cmake_minimum_required (VERSION 3.10)
project (yokan-snapshot C)

add_executable (14_yokan_snapshot_example snapshot_example.c)
target_link_libraries (14_yokan_snapshot_example
    yokan::server
    yokan::client)

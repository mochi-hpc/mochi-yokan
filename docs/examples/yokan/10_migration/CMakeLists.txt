cmake_minimum_required (VERSION 3.10)
project (yokan-migration C)

if (ENABLE_REMI)

  find_package (remi REQUIRED)

  add_executable (10_yokan_migrate_example migrate_example.c)
  target_link_libraries (10_yokan_migrate_example
      remi
      yokan::server
      yokan::client)

endif ()

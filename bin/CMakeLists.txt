add_executable(yk-admin admin.cpp)
target_link_libraries(yk-admin yokan-admin PkgConfig::tclap)

install(TARGETS yk-admin DESTINATION bin)

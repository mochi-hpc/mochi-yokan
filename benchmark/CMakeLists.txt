add_executable(yokan-benchmark benchmark.cpp)
target_link_libraries(yokan-benchmark yokan-client PkgConfig::tclap)

install(TARGETS yokan-benchmark DESTINATION bin)

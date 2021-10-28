add_executable(yk-benchmark benchmark.cpp)
target_link_libraries(yk-benchmark yokan-client PkgConfig::tclap)

install(TARGETS yk-benchmark DESTINATION bin)

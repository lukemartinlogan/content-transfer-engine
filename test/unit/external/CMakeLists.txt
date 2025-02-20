set(CMAKE_CXX_STANDARD 17)
find_package(Hermes REQUIRED)
find_package(MPI REQUIRED COMPONENTS C CXX)
include_directories(${Hermes_INCLUDE_DIRS})
add_executable(test_hermes_external_compile
        external.cc
)
target_link_libraries(test_hermes_external_compile
        Hermes::client Catch2::Catch2 MPI::MPI_CXX)

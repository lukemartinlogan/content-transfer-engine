cmake_minimum_required(VERSION 3.10)
project(hermes)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------
# if (HERMES_ENABLE_CUDA)
#         add_cuda_executable(test_hermes_exec FALSE
#         ${TEST_MAIN}/
#         # test_init.cc
#         test_bucket_cuda.cc
#         )
# else()
#         add_executable(test_hermes_exec
#         ${TEST_MAIN}/main_mpi.cc
#         test_init.cc
#         test_bucket.cc
#         )
# endif()

 
add_executable(test_hermes_exec
        ${TEST_MAIN}/main_mpi.cc
        test_init.cc
        test_bucket.cc
)

# if (HERMES_ENABLE_CUDA)
#         target_sources(test_hermes_exec PRIVATE test_bucket_cuda.cc)
# endif()

# add_executable(test_bucket test_bucket.cc) 
add_cuda_executable(test_bucket_cuda TRUE test_bucket_cuda.cc)
add_dependencies(test_bucket_cuda
        ${Hermes_CLIENT_DEPS})
target_link_libraries(test_bucket_cuda PUBLIC 
        hermes_client MPI::MPI_CXX hshm::cudacxx) 



add_dependencies(test_hermes_exec
        ${Hermes_CLIENT_DEPS})
target_link_libraries(test_hermes_exec PUBLIC 
        hermes_client Catch2::Catch2 MPI::MPI_CXX)
jarvis_test(hermes test_hermes)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------

# STRING TESTS
#add_test(NAME test_ipc COMMAND
#        ${CMAKE_BINARY_DIR}/bin/test_messages "TestIpc")

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_hermes_exec
        test_bucket_cuda
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_hermes_exec)
endif()

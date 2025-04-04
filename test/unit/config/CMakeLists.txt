# ------------------------------------------------------------------------------
# Build Tests
# ------------------------------------------------------------------------------

add_executable(test_config_exec
        ${TEST_MAIN}/main_mpi.cc
        test_init.cc
        test_config.cc
)
add_dependencies(test_config_exec
        ${Hermes_CLIENT_DEPS})
target_link_libraries(test_config_exec
        ${Hermes_CLIENT_DEPS} Catch2::Catch2 MPI::MPI_CXX)
jarvis_test(hermes test_hermes)

# ------------------------------------------------------------------------------
# Test Cases
# ------------------------------------------------------------------------------

# STRING TESTS
# add_test(NAME test_ipc COMMAND
# ${CMAKE_BINARY_DIR}/bin/test_messages "TestIpc")

# ------------------------------------------------------------------------------
# Install Targets
# ------------------------------------------------------------------------------
install(TARGETS
        test_config_exec
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

# -----------------------------------------------------------------------------
# Coverage
# -----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
        set_coverage_flags(test_config_exec)
endif()

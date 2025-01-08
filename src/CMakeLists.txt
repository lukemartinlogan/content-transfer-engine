#------------------------------------------------------------------------------
# Build Hermes Task Library
#------------------------------------------------------------------------------
# The client-facing library
add_library(hermes_client hermes_config_manager.cc)
target_link_libraries(hermes_client client_deps)
add_library(client INTERFACE)
target_link_libraries(client INTERFACE hermes_client)

# The runtime-facing library
add_library(hermes_runtime hermes_config_manager.cc)
target_link_libraries(hermes_runtime runtime_deps)
add_library(runtime INTERFACE)
target_link_libraries(client INTERFACE hermes_runtime)
#------------------------------------------------------------------------------
# Install Hermes library
#------------------------------------------------------------------------------
install(
        TARGETS
        hermes_client
        hermes_runtime
        client_deps
        runtime_deps
        runtime
        client
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#------------------------------------------------------------------------------
# Coverage
#------------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_client)
    set_coverage_flags(hermes_runtime)
endif()

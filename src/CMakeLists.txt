#------------------------------------------------------------------------------
# Build Hermes Task Library
#------------------------------------------------------------------------------
# The client-facing library
add_library(hermes_client INTERFACE)
target_link_libraries(hermes_client INTERFACE client_deps)
add_library(client INTERFACE)
target_link_libraries(client INTERFACE hermes_client)

# The runtime-facing library
add_library(hermes_runtime INTERFACE)
target_link_libraries(hermes_runtime INTERFACE runtime_deps)
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

#------------------------------------------------------------------------------
# Build Hermes Task Library
#------------------------------------------------------------------------------
# The client-facing library
add_library(hermes_client SHARED
        hermes_config_manager.cc
)
target_link_libraries(hermes_client ${_Hermes_CLIENT_LIBRARIES})
# The runtime-facing library
add_library(hermes_runtime SHARED
        hermes_config_manager.cc
)
target_link_libraries(hermes_runtime ${_Hermes_RUNTIME_LIBRARIES})
target_compile_definitions(hermes_runtime PUBLIC ${Hermes_RUNTIME_DEFINITIONS})
#------------------------------------------------------------------------------
# Install Hermes library
#------------------------------------------------------------------------------
install(
        TARGETS
        hermes_client
        hermes_runtime
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        DESTINATION
        ${HERMES_INSTALL_DATA_DIR}/cmake/hermes
        FILE
        ${HERMES_EXPORTED_TARGETS}.cmake
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(HERMES_EXPORTED_LIBS
        hermes_client
        hermes_runtime
        ${HERMES_EXPORTED_LIBS})
if(NOT CHIMAERA_EXTERNALLY_CONFIGURED)
    EXPORT (
            TARGETS
            ${HERMES_EXPORTED_LIBS}
            FILE
            ${HERMES_EXPORTED_TARGETS}.cmake
    )
endif()

#------------------------------------------------------------------------------
# Coverage
#------------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_client)
    set_coverage_flags(hermes_runtime)
endif()

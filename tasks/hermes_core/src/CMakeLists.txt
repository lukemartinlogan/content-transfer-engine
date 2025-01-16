# ------------------------------------------------------------------------------
# Build hermes_core Task Library
# ------------------------------------------------------------------------------
add_chimod_library(hermes core hermes_core.cc)

if(HERMES_ENABLE_CUDA)
    hshm_enable_cuda(17)
endif()

# ------------------------------------------------------------------------------
# Install hermes_core Task Library
# ------------------------------------------------------------------------------
install(
    TARGETS
    ${hermes_core_exports}
    LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
    ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
    RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

# install(FILES hermes_core_monitor.py
# DESTINATION ${HERMES_INSTALL_LIB_DIR})

# ------------------------------------------------------------------------------
# Coverage
# ------------------------------------------------------------------------------
if(CHIMAERA_ENABLE_COVERAGE)
    set_coverage_flags(hermes_core)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_cuda_library(cufile_api SHARED TRUE cufile_api.cc)
target_link_libraries(cufile_api PUBLIC hshm::cudacxx)

install(TARGETS cufile_api
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
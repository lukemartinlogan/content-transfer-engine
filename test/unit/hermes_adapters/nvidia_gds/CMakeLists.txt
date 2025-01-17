cmake_minimum_required(VERSION 3.24)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_ARCHITECTURES native)
project(MegaGPU LANGUAGES C CXX CUDA)

# HermesShm
find_package(HermesShm CONFIG REQUIRED)
message(STATUS "found hermes_shm.h at ${HermesShm_INCLUDE_DIRS}")
include_directories(${HermesShm_INCLUDE_DIRS})
link_directories(${HermesShm_LIBRARY_DIRS})


add_executable(gds_adapter_basic_test 
                gds_adapter_basic_test.cc)
target_link_libraries(gds_adapter_basic_test 
        PUBLIC  ${HermesShm_LIBRARIES} cufile ) #hermes added
set_source_files_properties(gds_adapter_basic_test.cc PROPERTIES LANGUAGE CUDA)
set_target_properties(gds_adapter_basic_test PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
#        CUDA_RUNTIME_LIBRARY Shared
#        POSITION_INDEPENDENT_CODE ON
)
jarvis_test(gds test_nvidia_gds_basic) 


add_executable(gds_adapter_simple_test 
        gds_adapter_simple_test.cc)
target_link_libraries(gds_adapter_simple_test 
        PUBLIC  ${HermesShm_LIBRARIES} cufile ) #hermes added
set_source_files_properties(gds_adapter_simple_test.cc PROPERTIES LANGUAGE CUDA)
set_target_properties(gds_adapter_simple_test PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
#        CUDA_RUNTIME_LIBRARY Shared
#        POSITION_INDEPENDENT_CODE ON
)
jarvis_test(gds test_nvidia_gds_simple) 


SET(POSIX_TESTS
  gds_adapter_basic_test
  gds_adapter_simple_test
)
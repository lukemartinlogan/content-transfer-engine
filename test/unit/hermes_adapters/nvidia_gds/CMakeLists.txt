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


add_executable(test_cufile_isa test_cufile_isa.cc)
# add_dependencies(test_cufile_isa simple_lib ${HermesShm_LIBRARIES} cufile_api cufile hermes)  #hermes added
target_link_libraries(test_cufile_isa 
        PUBLIC  ${HermesShm_LIBRARIES} cufile ) #hermes added
set_source_files_properties(test_cufile_isa.cc PROPERTIES LANGUAGE CUDA)
set_target_properties(test_cufile_isa PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
#        CUDA_RUNTIME_LIBRARY Shared
#        POSITION_INDEPENDENT_CODE ON
)
jarvis_test(gds cufile_isa_test) #added jarvis test? eventully need to add jarvis test


add_executable(test_cufile_shazzadul test_cufile_shazzadul.cc)
target_link_libraries(test_cufile_shazzadul 
        PUBLIC  ${HermesShm_LIBRARIES} cufile ) #hermes added
set_source_files_properties(test_cufile_shazzadul.cc PROPERTIES LANGUAGE CUDA)
set_target_properties(test_cufile_shazzadul PROPERTIES
        CUDA_SEPARABLE_COMPILATION ON
#        CUDA_RUNTIME_LIBRARY Shared
#        POSITION_INDEPENDENT_CODE ON
)
jarvis_test(gds cufile_shazzadul_test) #added jarvis test?  eventully need to add jarvis test

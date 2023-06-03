# CMake version
cmake_minimum_required (VERSION 3.25)
MESSAGE(STATUS "CMAKE_ROOT: " ${CMAKE_ROOT})

# Project name
project(Taro VERSION 1.0.0 LANGUAGES CXX)

# Turn on the verbose
set(CMAKE_VERBOSE_MAKEFILE ON)

# defult release build
set(TARO_DEFAULT_BUILD_TYPE "Release")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${TARO_DEFAULT_BUILD_TYPE}'")
  set(
    CMAKE_BUILD_TYPE "${TARO_DEFAULT_BUILD_TYPE}" 
    CACHE
    STRING "Choose the type of build." 
    FORCE
  )
  # Set the possible values of build type for cmake-gui
  set_property(
    CACHE 
    CMAKE_BUILD_TYPE 
    PROPERTY STRINGS
    "Debug" "Release"
  )
endif()

#not test yet
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.0")
	  message(FATAL_ERROR "\n Taro requires g++ at least v11.0")
  endif()
## clang++
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "15.0")
    message(FATAL_ERROR "\n Taro requires clang++ at least v15.0")
  endif() 
endif()

# Args
option(TARO_BUILD_TESTS "Enables build of tests" ON)
option(TARO_BUILD_BENCHMARKS "Enables build of benchmarks" ON)

# installation path
set(TARO_UTEST_DIR ${PROJECT_SOURCE_DIR}/unittests)
set(TARO_BENCHMARK_DIR ${PROJECT_SOURCE_DIR}/benchmarks)
set(TARO_3RD_PARTY_DIR ${PROJECT_SOURCE_DIR}/3rd-party)

# include test
include(CTest)

#compiler setting 
add_library(error_settings INTERFACE)
add_library(taro::error_settings ALIAS error_settings)

target_compile_options(
  error_settings
  INTERFACE
  $<$<COMPILE_LANGUAGE:Clang>:-Wall -Wextra -Wfatal-errors>
  $<$<COMPILE_LANGUAGE:GNU>:-Wall -Wextra -Wfatal-errors>
)

##optmization
add_library(optimize_settings INTERFACE)
add_library(taro::optimize_settings ALIAS optimize_settings)

target_compile_options(
  optimize_settings INTERFACE
  $<$<AND:$<CONFIG:Release>,$<COMPILE_LANGUAGE:Clang>>:-O3 -march=native>
  $<$<AND:$<CONFIG:Release>,$<COMPILE_LANGUAGE:GNU>>:-O3 -march=native>
  $<$<AND:$<CONFIG:Debug>,$<COMPILE_LANGUAGE:Clang>>:-O0 -g -fsanitize=thread>
  $<$<AND:$<CONFIG:Debug>,$<COMPILE_LANGUAGE:GNU>>:-O0 -g -fsanitize=thread>
)

#set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=thread -Wall -Wextra -Wfatal-errors")

set(CMAKE_CXX_FLAGS_DEBUG "-g -Wall -Wextra -Wfatal-errors -fsanitize=thread")
#set(CMAKE_CXX_FLAGS_DEBUG "-g -Wall -Wextra -Wfatal-errors")
#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
#set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS}" "-std=c++20 -Xcompiler -fsanitize=thread -Xcompiler -std=c++20 --extended-lambda" )
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS}" "-std=c++20 -Xcompiler -std=c++20 --extended-lambda" )
set(CUDA_NVCC_FLAGS_DEBUG "${CUDA_NVCC_FLAGS_DEBUG}" "-lineinfo -Xcompiler -fsanitize=thread")
#set(CUDA_NVCC_FLAGS_RELEASE "${CUDA_NVCC_FLAGS_RELEASE}" "-O2 -w ")
#SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread") 

#default
add_library(default_settings INTERFACE)
add_library(taro::default_settings ALIAS default_settings)
target_link_libraries(
  default_settings 
  INTERFACE 
  taro::error_settings 
  taro::optimize_settings 
)


# CXX target properties
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(CUDA QUIET REQUIRED)
# Thread
find_package(Threads REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)
#OpenMP
#find_package(OpenMP REQUIRED)
#set(OpenMP_CXX_FLAGS "-fopenmp")

# message
message(STATUS "CMAKE_HOST_SYSTEM: ${CMAKE_HOST_SYSTEM}")
message(STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE})
message(STATUS "CMAKE_CXX_COMPILER: " ${CMAKE_CXX_COMPILER})
message(STATUS "CMAKE_CXX_COMPILER_VERSION: " ${CMAKE_CXX_COMPILER_VERSION})
message(STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS})
message(STATUS "CMAKE_MODULE_PATH: " ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_CURRENT_SOURCE_DIR: " ${CMAKE_CURRENT_SOURCE_DIR})
message(STATUS "CMAKE_CURRENT_BINARY_DIR: " ${CMAKE_CURRENT_BINARY_DIR})
message(STATUS "CMAKE_EXE_LINKER_FLAGS: " ${CMAKE_EXE_LINKER_FLAGS})
message(STATUS "CMAKE_INSTALL_PREFIX: " ${CMAKE_INSTALL_PREFIX})
message(STATUS "CMAKE_MODULE_PATH: " ${CMAKE_MODULE_PATH})
message(STATUS "CMAKE_PREFIX_PATH: " ${CMAKE_PREFIX_PATH})
message(STATUS "PROJECT_NAME: " ${PROJECT_NAME})

#include directories
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/3rd-party/)

#cuda
#include_directories(/usr/local/cuda/include/)
#link_directories(/usr/local/cuda/lib64/)


#--------interface------
add_library(${PROJECT_NAME} INTERFACE)

target_compile_features(${PROJECT_NAME} INTERFACE cxx_std_20)
target_include_directories(${PROJECT_NAME} INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:include/> 
)
#-----------------------

#find -arch
include(FindCUDA)
set(CUDA_ARCH_LIST Auto CACHE STRING
    "List of CUDA architectures (e.g. Pascal, Volta, etc) or \
compute capability versions (6.1, 7.0, etc) to generate code for. \
Set to Auto for automatic detection (default)."
)
cuda_select_nvcc_arch_flags(CUDA_ARCH_FLAGS ${CUDA_ARCH_LIST})
list(APPEND CUDA_NVCC_FLAGS ${CUDA_ARCH_FLAGS})


# test
if(${TARO_BUILD_TESTS})

message(STATUS "Building unit tests ...")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TARO_UTEST_DIR})
add_subdirectory(unittests)

endif()


# benchmark
if(TARO_BUILD_BENCHMARKS)
message(STATUS "building benchmarks ...")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TARO_BENCHMARK_DIR})
add_subdirectory(benchmarks)
endif()


cmake_minimum_required(VERSION 3.16)

set(PACKAGE_NAME    "helio")
set(PROJECT_CONTACT romange@gmail.com)

project(${PACKAGE_NAME} C CXX)
enable_testing()

get_directory_property(PARENT_PROJ_NAME PARENT_DIRECTORY)

Message(STATUS "PROJECT_BINARY_DIR ${PROJECT_BINARY_DIR} GENERATOR ${CMAKE_GENERATOR}")
Message(STATUS "PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR} CMAKE_SOURCE_DIR ${CMAKE_SOURCE_DIR}")

if (APPLE)
  option (WITH_UNWIND "Enable libunwind support" OFF)
else()
  option (WITH_UNWIND "Enable libunwind support" ON)
endif()

option (HELIO_USE_SANITIZER "Use asan and usan sanitizers" OFF)
option (BUILD_DOCS "Generate documentation " ON)
option (BUILD_SHARED_LIBS "Build shared libraries" OFF)
option (USE_MOLD "whether to use mold linker" OFF)
set    (HELIO_MIMALLOC_OPTS "" CACHE STRING "additional mimalloc compile options")
set    (HELIO_MIMALLOC_LIBNAME "libmimalloc.a" CACHE STRING "name of mimalloc library")

option(WITH_GPERF "Compile with gperf" ON)

include(CheckCXXCompilerFlag)

# Can not use CHECK_CXX_COMPILER_FLAG due to linker problems.
set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_ASAN)

set(CMAKE_REQUIRED_FLAGS "-fsanitize=undefined")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_USAN)
set(CMAKE_REQUIRED_FLAGS "")


if (PARENT_PROJ_NAME)
  Message(STATUS "PARENT_PROJ_NAME ${PARENT_PROJ_NAME}")
else()
  # Define our own project
  set(CMAKE_CXX_STANDARD 17)
  list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

  if (HELIO_USE_SANITIZER AND SUPPORT_ASAN)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
  endif()

  if (HELIO_USE_SANITIZER AND SUPPORT_USAN)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined")
  endif()

  if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      set(CMAKE_CXX_FLAGS "-Wthread-safety ${CMAKE_CXX_FLAGS}")
  endif()

  include(third_party)
  include(internal)
endif()


add_definitions(-DUSE_FB2)

add_subdirectory(base)
add_subdirectory(io)
add_subdirectory(examples)
add_subdirectory(strings)
add_subdirectory(util)

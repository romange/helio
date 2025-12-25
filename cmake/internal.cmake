# Copyright 2013, Beeri 15.  All rights reserved.
# Author: Roman Gershman (romange@gmail.com)
#

# Guard
if(HELIO_INTERNAL_INCLUDED)
  return()
endif()
set(HELIO_INTERNAL_INCLUDED TRUE)

include(CTest)
include(CheckCXXCompilerFlag)
include(CheckCXXSourceCompiles)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
enable_language(CXX C)

# ---[ Environment Checks (Fail Fast) ]---
# Architecture and OS checks moved to top
if (NOT CMAKE_SIZEOF_VOID_P EQUAL 8) # Check target architecture
  message(FATAL_ERROR "Helio requires a 64bit target architecture.")
endif()
if(NOT CMAKE_SYSTEM_NAME MATCHES "Linux|Darwin|FreeBSD")
  message(FATAL_ERROR "Unsupported ${CMAKE_SYSTEM_NAME}")
endif()

# ---[ Global Project Setup ]---
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CXX_STD_VERSION "c++17")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(HELIO_MIMALLOC_OPTS "" CACHE STRING "additional mimalloc compile options")
set(HELIO_MIMALLOC_LIBNAME "libmimalloc.a" CACHE STRING "name of mimalloc library")

# Compiler detection logic
set(USING_CLANG OFF)
set(USING_GCC OFF)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(USING_CLANG ON)
endif()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(USING_GCC ON)
endif()
CHECK_CXX_COMPILER_FLAG("-std=${CXX_STD_VERSION}" COMPILER_SUPPORTS_CXX17)
if(NOT COMPILER_SUPPORTS_CXX17)
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no ${CXX_STD_VERSION} support. \
                         Please use a different C++ compiler.")
endif()

message(STATUS "Compiler ${CMAKE_CXX_COMPILER}, version: ${CMAKE_CXX_COMPILER_VERSION}, C++ standard: c++${CMAKE_CXX_STANDARD}")

# ---[ System Probing & Discovery ]---
# Can not use CHECK_CXX_COMPILER_FLAG due to linker problems.
set(_OLD_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}") # Save original flags

# Check ASAN
set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_ASAN)

# Check USAN (UBSAN)
set(CMAKE_REQUIRED_FLAGS "-fsanitize=undefined")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_USAN)

set(CMAKE_REQUIRED_FLAGS "${_OLD_REQUIRED_FLAGS}") # Restore original flags

check_cxx_source_compiles("
#include <string.h>
int main() {
 rawmemchr((const void*)8, 13);
 return 0;
}" HAS_RAWMEMCHR)

if (HAS_RAWMEMCHR)
  add_compile_definitions(HAS_RAWMEMCHR)
endif()

# ---[ Flag Configuration ]---
if (HELIO_USE_SANITIZER)
  # Shared flag for better stack traces (prevents hidden stack frames)
  set(SAN_COMMON_FLAGS "-fno-optimize-sibling-calls")

  # Address Sanitizer (ASan)
  if (SUPPORT_ASAN)
    # Enable ASan + Scope detection
    set(ASAN_FLAGS "-fsanitize=address -fsanitize-address-use-after-scope")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${ASAN_FLAGS} ${SAN_COMMON_FLAGS}")
  endif()

  # Undefined Behavior Sanitizer
  if (SUPPORT_USAN)
    # Enable USan + Force crash on error (no recover)
    set(USAN_FLAGS "-fsanitize=undefined -fno-sanitize-recover=all")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${USAN_FLAGS} ${SAN_COMMON_FLAGS}")

    # Clang-specific linker fixes for USan
    if (USING_CLANG)
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --start-no-unused-arguments --rtlib=compiler-rt --end-no-unused-arguments")
    endif()
  endif()
endif()

# ---[ Compiler Specific Flags ]---
if (USING_CLANG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wthread-safety -fcolor-diagnostics -Wno-deprecated-copy")
endif()

if(USING_GCC)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=auto")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")

  # Abseil triggers "ignored-attributes" warnings.
  # Since CI passes -Werror, we must explicitly downgrade this specific warning to non-fatal.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=ignored-attributes")

  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${HELIO_RELEASE_FLAGS}")
  set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${HELIO_RELEASE_FLAGS}")
endif()

# ---[ Architecture & Base Options ]---
if (USE_MOLD)
  find_path(MOLD_PATH ld PATHS /usr/libexec /usr/local/libexec PATH_SUFFIXES mold
            REQUIRED NO_DEFAULT_PATH)
  set(LINKER_OPTS "-B${MOLD_PATH}")
endif()

if (NOT MARCH_OPT)
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(MARCH_OPT "-march=armv8.2-a+fp16+rcpc+dotprod+crypto")
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
    # FreeBSD uses amd64.
    # Github actions use DSv2 that may use haswell cpus.
    # We will make it friendly towards older architectures so that will run on developers laptops.
    # However, we will tune it towards intel skylakes that are common in public clouds.
    set(MARCH_OPT "-march=sandybridge -mtune=skylake")
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    # MacOS on arm64 - TBD.
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "s390x")
    set(MARCH_OPT "-march=native -mzvector")
  else()
    MESSAGE(FATAL_ERROR "Unsupported architecture ${CMAKE_SYSTEM_PROCESSOR}")
  endif()
endif()

# Need -fPIC in order to link against shared libraries. For example when creating python modules.
set(COMPILE_OPTS "-Wall -Wextra -g -fPIC -fno-builtin-malloc -fno-builtin-calloc")
set(COMPILE_OPTS "${COMPILE_OPTS} -fno-builtin-realloc -fno-builtin-free")
set(COMPILE_OPTS "${COMPILE_OPTS} -fno-omit-frame-pointer -Wno-unused-parameter")
set(COMPILE_OPTS "${COMPILE_OPTS} ${MARCH_OPT}")
set(CMAKE_CXX_FLAGS "${COMPILE_OPTS} ${CMAKE_CXX_FLAGS} ${LINKER_OPTS}")
set(CMAKE_C_FLAGS "${COMPILE_OPTS} ${CMAKE_C_FLAGS} ${LINKER_OPTS}")
add_compile_definitions(USE_FB2)

IF(CMAKE_BUILD_TYPE STREQUAL "Debug")
  MESSAGE (CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}")
ELSEIF(CMAKE_BUILD_TYPE STREQUAL "Release")
  MESSAGE (CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
ELSE()
  MESSAGE(FATAL_ERROR "Unsupported build type '${CMAKE_BUILD_TYPE}' or not specified. Use Debug or Release.")
ENDIF()

# ---[ Helper Functions ]---
set(ROOT_GEN_DIR ${CMAKE_SOURCE_DIR}/genfiles)
file(MAKE_DIRECTORY ${ROOT_GEN_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${ROOT_GEN_DIR})

macro(add_include target)
  set_property(TARGET ${target}
               APPEND PROPERTY INCLUDE_DIRECTORIES ${ARGN})
endmacro()

macro(add_compile_flag target)
  set_property(TARGET ${target} APPEND PROPERTY COMPILE_FLAGS ${ARGN})
endmacro()

function(cxx_link target)
  CMAKE_PARSE_ARGUMENTS(parsed "" "" "DATA" ${ARGN})

  if (parsed_DATA)
    # symlink data files into build directory
    set(run_dir "${CMAKE_BINARY_DIR}/${target}.runfiles")
    foreach (data_file ${parsed_DATA})
      get_filename_component(src_full_path ${data_file} ABSOLUTE)
      if (NOT EXISTS ${src_full_path})
        Message(FATAL_ERROR "Can not find ${src_full_path} when processing ${target}")
      endif()
      set(target_data_full "${run_dir}/${data_file}")
      get_filename_component(target_data_folder ${target_data_full} PATH)
      file(MAKE_DIRECTORY ${target_data_folder})
      execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${src_full_path} ${target_data_full})
    endforeach(data_file)
  endif()
  set(link_depends ${parsed_UNPARSED_ARGUMENTS})
  target_link_libraries(${target} ${link_depends})
endfunction()

SET_PROPERTY(GLOBAL PROPERTY "test_list_property" "")
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

# Defines a C++ test executable, links it with GTest/Benchmark, and registers it with CTest.
#
# Arguments:
#   path   - Path to the source file (without extension), used as the target name.
#            e.g., "base/my_test" -> creates target "my_test" from "base/my_test.cc"
#   LABELS - Optional list of CTest labels (default: "unit").
#            e.g., helio_cxx_test(my_test LABELS "integration" "slow" ...)
#   ARGN   - Any remaining arguments are treated as libraries to link against.
function(helio_cxx_test path)
  get_filename_component(name ${path} NAME)
  add_executable(${name} ${path}.cc)
  CMAKE_PARSE_ARGUMENTS(parsed "" "" "LABELS" ${ARGN})

  if (NOT parsed_LABELS)
    set(parsed_LABELS "unit")
  endif()

  add_include(${name} ${GTEST_INCLUDE_DIR} ${BENCHMARK_INCLUDE_DIR})
  target_compile_definitions(${name} PRIVATE _TEST_BASE_FILE_=\"${name}.cc\")
  cxx_link(${name} gtest_main_ext ${parsed_UNPARSED_ARGUMENTS})

  add_test(NAME ${name} COMMAND $<TARGET_FILE:${name}>)
  set_tests_properties(${name} PROPERTIES LABELS "${parsed_LABELS}")
  get_property(cur_list GLOBAL PROPERTY "test_list_property")
  foreach (_label ${parsed_LABELS})
    LIST(APPEND cur_list "${_label}:${name}")
  endforeach(_label)
  SET_PROPERTY(GLOBAL PROPERTY "test_list_property" "${cur_list}")
  add_dependencies(check ${name})
endfunction()

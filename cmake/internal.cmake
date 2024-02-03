# Copyright 2013, Beeri 15.  All rights reserved.
# Author: Roman Gershman (romange@gmail.com)
#

include(CTest)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
enable_language(CXX C)

# Check target architecture
if (NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
  message(FATAL_ERROR "Async requires a 64bit target architecture.")
endif()

if(NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "Linux" AND NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin"
   AND NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
  message(FATAL_ERROR "Unsupported ${CMAKE_SYSTEM_NAME}")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})


CHECK_CXX_COMPILER_FLAG("-std=c++17" COMPILER_SUPPORTS_CXX17)
CHECK_CXX_COMPILER_FLAG("-std=c++20" COMPILER_SUPPORTS_CXX20)

if(NOT COMPILER_SUPPORTS_CXX17)
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support. \
                         Please use a different C++ compiler.")
endif()

message(STATUS "Compiler ${CMAKE_CXX_COMPILER}, version: ${CMAKE_CXX_COMPILER_VERSION}")

if (COMPILER_SUPPORTS_CXX20)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
endif()

check_cxx_source_compiles("
#include <string.h>
int main() {
 rawmemchr((const void*)8, 13);
 return 0;
}" HAS_RAWMEMCHR)

if (HAS_RAWMEMCHR)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAS_RAWMEMCHR")
endif()

# ---[ Color diagnostics
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
#  -fsanitize=address has a bug with clang: multiple definition of `operator delete(void*)
# -fsanitize=undefined has a bug with clang too (segfaults in gpertools)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics -Wno-deprecated-copy")
    # set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined -fsanitize=address")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=auto")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${HELIO_RELEASE_FLAGS}")
  set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${HELIO_RELEASE_FLAGS}")
endif()

if (USE_MOLD)
  find_path(MOLD_PATH ld PATHS /usr/libexec /usr/local/libexec PATH_SUFFIXES mold
            REQUIRED NO_DEFAULT_PATH)
  set(LINKER_OPTS "-B${MOLD_PATH}")
endif()

# Need -fPIC in order to link against shared libraries. For example when creating python modules.
set(COMPILE_OPTS "-Wall -Wextra -g -fPIC -fno-builtin-malloc -fno-builtin-calloc")
set(COMPILE_OPTS "${COMPILE_OPTS} -fno-builtin-realloc -fno-builtin-free")
set(COMPILE_OPTS "${COMPILE_OPTS} -fno-omit-frame-pointer -Wno-unused-parameter")

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


set(COMPILE_OPTS "${COMPILE_OPTS} ${MARCH_OPT}")
set(CMAKE_CXX_FLAGS "${COMPILE_OPTS} ${CMAKE_CXX_FLAGS} ${LINKER_OPTS}")
set(CMAKE_C_FLAGS "${COMPILE_OPTS} ${CMAKE_C_FLAGS} ${LINKER_OPTS}")

IF(CMAKE_BUILD_TYPE STREQUAL "Debug")
  MESSAGE (CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}")
ELSEIF(CMAKE_BUILD_TYPE STREQUAL "Release")
  MESSAGE (CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
ELSE()
  MESSAGE(FATAL_ERROR "Unsupported build type '${CMAKE_BUILD_TYPE}'")
ENDIF()

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

function(cxx_test path)
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

  # add_custom_command(TARGET ${name} POST_BUILD
  #                    COMMAND ${name} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  #                    COMMENT "Running ${name}" VERBATIM)
endfunction()




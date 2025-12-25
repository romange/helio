cmake_minimum_required(VERSION 3.16)

set(PACKAGE_NAME    "helio")
set(PROJECT_CONTACT romange@gmail.com)

project(${PACKAGE_NAME} C CXX)
enable_testing()

# ------------------------------------------------------------------------------
# Project Options
# ------------------------------------------------------------------------------

# Helio Feature options
option(HELIO_USE_SANITIZER "Use ASan and USan sanitizers" OFF)
option(BUILD_DOCS          "Generate documentation" ON)
option(BUILD_SHARED_LIBS   "Build shared libraries" OFF)

# Platform-specific defaults
set(WITH_UNWIND_DEFAULT ON)
if (APPLE)
  set(WITH_UNWIND_DEFAULT OFF)
endif()
option(WITH_UNWIND "Enable libunwind support" ${WITH_UNWIND_DEFAULT})

# Linker Configuration (Mold)
set(MOLD_DEFAULT_STATE OFF)
if(NOT DEFINED CACHE{USE_MOLD})
  find_program(MOLD_EXECUTABLE mold QUIET)
  if(MOLD_EXECUTABLE)
    set(MOLD_DEFAULT_STATE ON)
  endif()
endif()
option(USE_MOLD "whether to use mold linker" ${MOLD_DEFAULT_STATE})

# ------------------------------------------------------------------------------
# Project Metadata & setup
# ------------------------------------------------------------------------------

message(STATUS "PROJECT_BINARY_DIR ${PROJECT_BINARY_DIR} GENERATOR ${CMAKE_GENERATOR}")
message(STATUS "PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR} CMAKE_SOURCE_DIR ${CMAKE_SOURCE_DIR}")

# Load and set internal compiler logic (Flags, Sanitizers, Architecture)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(internal)    # Standards and flags first
include(third_party) # Dependencies second

# --- [ Legacy Compatibility Wrapper ] ---
# GTest defines a function named 'cxx_test', which hides our internal logic.
# We define this wrapper AFTER including third_party to intentionally shadow
# GTest's version and redirect calls back to our 'helio_cxx_test'.
#
# TODO: Remove this wrapper once all subdirectories (and Dragonfly) are
# migrated to use 'helio_cxx_test' directly.
function(cxx_test)
  message(AUTHOR_WARNING "cxx_test() is deprecated, use helio_cxx_test() instead.")
  helio_cxx_test(${ARGV})
endfunction()

# ------------------------------------------------------------------------------
# Subdirectories
# ------------------------------------------------------------------------------
add_subdirectory(base)
add_subdirectory(io)
add_subdirectory(examples)
add_subdirectory(strings)
add_subdirectory(util)

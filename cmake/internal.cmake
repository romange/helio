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
set(HELIO_STRICT_WARNINGS "" CACHE STRING "strict warning compilation flags")

# Compiler detection logic
set(USING_CLANG OFF)
set(USING_GCC OFF)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(USING_CLANG ON)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(USING_GCC ON)
else()
  message(FATAL_ERROR "Helio only supports Clang or GCC. Detected: ${CMAKE_CXX_COMPILER_ID}")
endif()
CHECK_CXX_COMPILER_FLAG("-std=${CXX_STD_VERSION}" COMPILER_SUPPORTS_CXX17)
if(NOT COMPILER_SUPPORTS_CXX17)
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no ${CXX_STD_VERSION} support. Please use a different C++ compiler.")
endif()

message(STATUS "Compiler ${CMAKE_CXX_COMPILER}, version: ${CMAKE_CXX_COMPILER_VERSION}, C++ standard: c++${CMAKE_CXX_STANDARD}")

# ---[ System Probing & Discovery ]---
# Can not use CHECK_CXX_COMPILER_FLAG due to linker problems.
set(_ORIG_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}") # Save original flags, used to restore later.

# Check ASAN
set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_ASAN)

# Check USAN (UBSAN)
set(CMAKE_REQUIRED_FLAGS "-fsanitize=undefined")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_USAN)

set(CMAKE_REQUIRED_FLAGS "${_ORIG_REQUIRED_FLAGS}")

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
  set(SANITIZERS_COMMON_FLAGS "-fno-optimize-sibling-calls")

  # Address Sanitizer (ASan)
  if (SUPPORT_ASAN)
    # Enable ASan + Scope detection
    set(ASAN_FLAGS "-fsanitize=address -fsanitize-address-use-after-scope")
    # Split flags into a list to prevent CMake from quoting them as a single invalid argument (both compile and link)
    string(REPLACE " " ";" _ASAN_LIST "${ASAN_FLAGS} ${SANITIZERS_COMMON_FLAGS}")
    foreach(_flag ${_ASAN_LIST})
      add_compile_options($<$<CONFIG:Debug>:${_flag}>)
    endforeach()
    string(REPLACE " " ";" _ASAN_LINK_LIST "${ASAN_FLAGS}")
    foreach(_flag ${_ASAN_LINK_LIST})
      add_link_options($<$<CONFIG:Debug>:${_flag}>)
    endforeach()
  endif()

  # Undefined Behavior Sanitizer (UBSan)
  if (SUPPORT_USAN)
    # Enable USan + Force crash on error (no recover)
    set(USAN_FLAGS "-fsanitize=undefined -fno-sanitize-recover=all")
    # Split flags into a list to prevent CMake from quoting them as a single invalid argument (both compile and link)
    string(REPLACE " " ";" _USAN_COMPILE_LIST "${USAN_FLAGS} ${SANITIZERS_COMMON_FLAGS}")
    foreach(_flag ${_USAN_COMPILE_LIST})
      add_compile_options($<$<CONFIG:Debug>:${_flag}>)
    endforeach()
    string(REPLACE " " ";" _USAN_LINK_LIST "${USAN_FLAGS}")
    foreach(_flag ${_USAN_LINK_LIST})
      add_link_options($<$<CONFIG:Debug>:${_flag}>)
    endforeach()

    # Clang-specific linker fixes for USan
    if (USING_CLANG)
      add_link_options($<$<CONFIG:Debug>:--start-no-unused-arguments>)
      add_link_options($<$<CONFIG:Debug>:--rtlib=compiler-rt>)
      add_link_options($<$<CONFIG:Debug>:--end-no-unused-arguments>)
    endif()
  endif()
endif()

# ---[ Compiler Specific Flags ]---
if (USING_CLANG)
    add_compile_options(-Wthread-safety -fcolor-diagnostics -Wno-deprecated-copy)
else() # must be GCC
  add_compile_options(-fdiagnostics-color=always)

  # --- [TEMPORARY MIGRATION FIX] ---
  # TODO: Remove this once CI workflows are updated to use HELIO_STRICT_BUILD=ON.
  #
  # WHY: The current CI explicitly passes global '-Werror' via CMAKE_CXX_FLAGS.
  # This causes Abseil (3rd party) to fail on GCC due to 'ignored-attributes'.
  # We suppress this warning specifically to allow the CI to pass until the
  # global '-Werror' injection is replaced by our target-based strict mode.
  add_compile_options(-Wno-error=ignored-attributes)

  if(HELIO_RELEASE_FLAGS)
    add_compile_options($<$<CONFIG:Release>:${HELIO_RELEASE_FLAGS}>)
  endif()
endif()

# ---[ Architecture & Base Options ]---
if (USE_MOLD)
  find_path(MOLD_PATH ld PATHS /usr/libexec /usr/local/libexec PATH_SUFFIXES mold REQUIRED NO_DEFAULT_PATH)
  add_link_options("-B${MOLD_PATH}")
endif()

if (NOT MARCH_OPT)
  # ARM64 (Linux/Server)
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(MARCH_OPT "-march=armv8.2-a+fp16+rcpc+dotprod+crypto")
  # Intel/AMD x86_64
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
    # Baseline: Haswell (2013).
    # Why:
    #   1. Enables AVX2 (Critical for in-memory DB performance).
    #   2. Safe for GitHub Actions (Azure DSv2 standards).
    #   3. Safe for 99% of Cloud Instances (AWS c4+, GCP N1+, Azure Dv2+).
    # TUNING: Skylake (Optimizes instruction scheduling for modern cloud fleets).
    set(MARCH_OPT "-march=haswell -mtune=skylake")
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    # Target the M1 baseline.
    # This works perfectly on M1, M2, and M3.
    # Using -mcpu handles both the Architecture (v8.5+) and Tuning.
    set(MARCH_OPT "-mcpu=apple-m1")
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "s390x")
    # IBM Mainframe (s390x)
    set(MARCH_OPT "-march=native -mzvector")
  else()
    message(FATAL_ERROR "Unsupported architecture ${CMAKE_SYSTEM_PROCESSOR}")
  endif()
endif()

# Handle March Opt (Split string into list for add_compile_options)
separate_arguments(_MARCH_LIST UNIX_COMMAND "${MARCH_OPT}")

# Core Global Compiler Options
# PURPOSE: Sets the critical baseline for debugging, memory override safety, and profiling.
add_compile_options(
  # --- Debugging & Observability ---
  -g                        # Debug Symbols: Essential for core dump analysis and attaching debuggers.
  -fno-omit-frame-pointer   # Profiling: Keeps the frame pointer. CRITICAL for low-overhead profiling (perf/eBPF) and fast stack unwinding.

  # --- Linking & Compatibility ---
  -fPIC                     # Position Independent Code: Required for linking against shared libraries (e.g., Python extensions).

  # --- Memory Management ---
  # Prevent the compiler from optimizing out standard allocator calls.
  # CRITICAL: We likely use a custom allocator (jemalloc/mimalloc). We cannot let the compiler assume libc behavior here.
  -fno-builtin-malloc
  -fno-builtin-calloc
  -fno-builtin-realloc
  -fno-builtin-free

  # --- Warnings & Hygiene ---
  -Wall -Wextra             # Hygiene: Enable standard warning sets to catch common errors.
  -Wno-unused-parameter     # Noise Reduction: Suppress warnings for unused args (common in callback-heavy network code).

  # --- Architecture ---
  ${_MARCH_LIST}            # Hardware Tuning: Apply specific CPU optimizations (e.g., -march=native).
)

add_compile_definitions(USE_FB2)

# ---[ Runtime Hardening (Security & Stability) ]---
if(HELIO_HARDENED_BUILD AND NOT HELIO_USE_SANITIZER)
    message(STATUS "Applying hardened build options (stack protection, fortify source, control flow protection)")

    # Stack Protector: The "Seatbelt"
    # prevents: The #1 most common attack (Stack Buffer Overflow).
    # why: If a buffer overflows, the program crashes immediately instead of letting an attacker hijack execution.
    # cost: Negligible (<1%). Modern CPUs predict this check perfectly.
    add_compile_options(-fstack-protector-strong)

    # Fortify Source: The "Standard Library Guard"
    # prevents: Silent memory corruption in standard functions (memcpy, strcpy, sprintf).
    # why: Turns "undefined behavior" (random crashes later) into an immediate, traceable abort at the source of the bug.
    # cost: Zero. It optimizes into safer instructions at compile-time.
    # note: Only active in optimized builds (Release/RelWithDebInfo).
    #       Ignored in Debug (-O0) to prevent warnings/errors.
    add_compile_definitions($<$<NOT:$<CONFIG:Debug>>:_FORTIFY_SOURCE=2>)

    # Control Flow Protection (CET): The "Hardware Lock"
    # prevents: Advanced ROP/JOP attacks (reusing your own code chunks against you).
    # why: Uses CPU hardware to verify function calls and returns match.
    # cost: Harmless NOPs on older CPUs; full hardware protection on modern Intel/AMD CPUs.
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64")
      add_compile_options(-fcf-protection=full)
    endif()
endif()

# ---[ Performance Optimizations ]---
if(HELIO_PERFORMANCE_OPTIMIZATIONS)
    message(STATUS "Applying Release performance optimizations (LTO, Architecture tuning)")

    # Link Time Optimization (LTO)
    # why: Breaks the "translation unit" barrier. Allows the compiler to inline functions
    #      across .cpp files (e.g., inlining a hot helper from utility.cc into main.cc).
    # result: Massively reduces function call overhead in fiber/template-heavy code.
    #         Often yields 10-20% higher throughput for CPU-bound stores.
    include(CheckIPOSupported)
    check_ipo_supported(RESULT result OUTPUT output)
    if(result)
        message(STATUS "LTO enabled: Enabling whole-program optimization")
        add_compile_options($<$<CONFIG:Release>:-flto>)
        add_link_options($<$<CONFIG:Release>:-flto>)
    else()
        message(WARNING "LTO not supported: ${output}")
    endif()
endif()

# ---[ Helper Functions ]---
set(ROOT_GEN_DIR ${CMAKE_SOURCE_DIR}/genfiles)
file(MAKE_DIRECTORY ${ROOT_GEN_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${ROOT_GEN_DIR})

macro(add_include target)
  set_property(TARGET ${target} APPEND PROPERTY INCLUDE_DIRECTORIES ${ARGN})
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
        message(FATAL_ERROR "Can not find ${src_full_path} when processing ${target}")
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

  if(HELIO_STRICT_WARNINGS)
    target_compile_options(${name} PRIVATE ${HELIO_STRICT_WARNINGS})
  endif()

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

function(helio_add_executable target_name)
  add_executable(${target_name} ${ARGN})
  if(HELIO_STRICT_WARNINGS)
    target_compile_options(${target_name} PRIVATE ${HELIO_STRICT_WARNINGS})
  endif()
endfunction()

function(helio_add_library target_name)
  add_library(${target_name} ${ARGN})
  if(HELIO_STRICT_WARNINGS)
    target_compile_options(${target_name} PRIVATE ${HELIO_STRICT_WARNINGS})
  endif()
endfunction()

# Calculates the list of strict warning flags
# Configures the "Strict Mode" warning set.
# PURPOSE: Enforces high code quality standards by turning potential bugs (shadowing,
#          implicit conversions, unsafe casts) into build errors.
# NOTE: These are applied specifically to 'helio' targets, distinct from global third-party flags.
function(helio_set_strict_warnings)

  set(_flags
      # basic warnings
      -Werror               # Zero Tolerance: Treat all warnings as build errors.
      -Wall                 # Enable most standard compiler warnings.
      -Wextra               # Enable extra warnings not covered by -Wall.
      -Wpedantic            # Enforce strict ISO C++ compliance (no non-standard extensions).
      # safety & correctness
      -Wshadow              # Critical: Prevents local variables from hiding member variables (common bug source).
      -Wnon-virtual-dtor    # OOP Safety: Warns if a class has virtual functions but a non-virtual destructor (prevents leaks).
      -Woverloaded-virtual  # OOP Safety: Warns if a function hides a virtual function in the base class.
      -Wunused              # Hygiene: Warns about unused variables, parameters, and functions.
      # type safety & casting
      -Wold-style-cast      # Modern C++: Forbids C-style `(int)x` casts. Forces explicit `static_cast<int>(x)`.
      -Wcast-align          # Hardware Safety: Warns when casting a pointer increases alignment requirements (crashes on ARM).
      # numeric safety (critical for data stores)
      -Wconversion          # Data Integrity: Warns on implicit conversions that may lose data (e.g., long to int).
      -Wsign-conversion     # Data Integrity: Warns on implicit mixed signed/unsigned math (common source of underflows).
      -Wdouble-promotion    # Performance: Warns when `float` is implicitly promoted to `double` (slower on some FPU/SIMD).
      # security
      -Wformat=2            # Security: Strong checks for printf-style format strings (prevents injection attacks).
  )

  # Sanity Check: When running Sanitizers (ASan/TSan), we disable -Werror. Sanitizers often inject code or
  # trigger warnings that are false positives during the instrumentation phase.
  # We want to see the runtime sanitizer report, not fail the build on a compiler warning.
  if(HELIO_USE_SANITIZER)
    list(REMOVE_ITEM _flags "-Werror")
  endif()

  set(HELIO_STRICT_WARNINGS "${_flags}" PARENT_SCOPE)
  message(STATUS "Strict warning flags set: ${_flags}")
endfunction()

# Guard
if(HELIO_THIRD_PARTY_INCLUDED)
  return()
endif()
set(HELIO_THIRD_PARTY_INCLUDED TRUE)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
 cmake_policy(SET CMP0135 NEW)
endif()

if(POLICY CMP0144)
  cmake_policy(SET CMP0144 NEW) # find_package() uses upper-case <PACKAGENAME>_ROOT variables
endif()

cmake_policy (SET CMP0079 NEW)
if (POLICY CMP0169)
  cmake_policy (SET CMP0169 OLD) # silence deprecation warning about FetchContent_Populate
endif()

set(THIRD_PARTY_DIR "${CMAKE_CURRENT_BINARY_DIR}/third_party")

SET_DIRECTORY_PROPERTIES(PROPERTIES EP_PREFIX ${THIRD_PARTY_DIR})

Include(ExternalProject)
Include(FetchContent)

option (WITH_UNWIND "Enable libunwind support" ON)
option (WITH_GPERF "Compile with gperf" ON)
option (WITH_AWS "Include AWS client for working with S3 files" ON)
option (WITH_GCP "Include GCP client for working with GCP" ON)
option (LEGACY_GLOG "whether to use legacy glog library" ON)


set(THIRD_PARTY_LIB_DIR "${THIRD_PARTY_DIR}/libs")
file(MAKE_DIRECTORY ${THIRD_PARTY_LIB_DIR})

set(THIRD_PARTY_CXX_FLAGS "-std=c++17 -O3 -DNDEBUG -fPIC -fno-stack-protector")

# Add platform-specific flags based on compiler
if (APPLE)
  # On macOS, always use clang-compatible flags regardless of symlinks/aliases
  # Check compiler ID first (most reliable), then fall back to real compiler check
  if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    message(STATUS "THIRD_PARTY: Using clang (ID=${CMAKE_CXX_COMPILER_ID}) - no GCC flags needed")
  else()
    # Double-check: even if ID is not Clang, see if it's actually clang under the hood
    execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version OUTPUT_VARIABLE COMPILER_VERSION)
    if (COMPILER_VERSION MATCHES "clang")
      message(STATUS "THIRD_PARTY: Detected clang via --version check - no GCC flags needed")
    else()
      message(STATUS "THIRD_PARTY: Real GCC detected - adding GCC-specific flags")
      set(THIRD_PARTY_CXX_FLAGS "${THIRD_PARTY_CXX_FLAGS} -fno-stack-clash-protection -Wl,-ld_classic")
    endif()
  endif()
else()
  # Linux/other platforms - assume GCC support
  set(THIRD_PARTY_CXX_FLAGS "${THIRD_PARTY_CXX_FLAGS} -fno-stack-clash-protection")
endif()

find_package(Threads REQUIRED)

function(add_third_party name)
  set(options SHARED)
  set(oneValueArgs CMAKE_PASS_FLAGS)
  set(multiValArgs BUILD_COMMAND INSTALL_COMMAND LIB)
  CMAKE_PARSE_ARGUMENTS(parsed "${options}" "${oneValueArgs}" "${multiValArgs}" ${ARGN})

  if (parsed_CMAKE_PASS_FLAGS)
    string(REPLACE " " ";" piped_CMAKE_ARGS ${parsed_CMAKE_PASS_FLAGS})
  endif()

  if (NOT parsed_INSTALL_COMMAND)
    set(parsed_INSTALL_COMMAND make install)
  endif()

  if (NOT parsed_BUILD_COMMAND)
    set(parsed_BUILD_COMMAND make -j4)
  endif()

  set(_DIR ${THIRD_PARTY_DIR}/${name})
  set(INSTALL_ROOT ${THIRD_PARTY_LIB_DIR}/${name})

  if (parsed_LIB)
    set(LIB_FILES "")

    foreach (_file ${parsed_LIB})
      LIST(APPEND LIB_FILES "${INSTALL_ROOT}/lib/${_file}")
      if (${_file} MATCHES ".*\.so$")
        set(LIB_TYPE SHARED)
      elseif (${_file} MATCHES ".*\.a$")
        set(LIB_TYPE STATIC)
      elseif("${_file}" STREQUAL "none")
        set(LIB_FILES "")
      else()
        MESSAGE(FATAL_ERROR "Unrecognized lib ${_file}")
      endif()
    endforeach(_file)
  else()
    set(LIB_PREFIX "${INSTALL_ROOT}/lib/lib${name}.")

    if(parsed_SHARED)
      set(LIB_TYPE SHARED)
      STRING(CONCAT LIB_FILES "${LIB_PREFIX}" "so")
    else()
      set(LIB_TYPE STATIC)
      STRING(CONCAT LIB_FILES "${LIB_PREFIX}" "a")
    endif(parsed_SHARED)
  endif()

  ExternalProject_Add(${name}_project
    DOWNLOAD_DIR ${_DIR}
    SOURCE_DIR ${_DIR}
    INSTALL_DIR ${INSTALL_ROOT}
    UPDATE_COMMAND ""

    BUILD_COMMAND ${parsed_BUILD_COMMAND}

    INSTALL_COMMAND ${parsed_INSTALL_COMMAND}

    # Wrap download, configure and build steps in a script to log output
    LOG_INSTALL ON
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_PATCH ON
    LOG_UPDATE ON

    CMAKE_GENERATOR "Unix Makefiles"
    BUILD_BYPRODUCTS ${LIB_FILES}
    # LIST_SEPARATOR | # Use the alternate list separator.
    # Can not use | because we use it inside sh/install_cmd

    # we need those CMAKE_ARGS for cmake based 3rd party projects.
    # CMAKE_ARCHIVE_OUTPUT_DIRECTORY is for static libs
    # CMAKE_LIBRARY_OUTPUT_DIRECTORY is for shared libs
    CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY:PATH=${INSTALL_ROOT}/lib
        -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:PATH=${INSTALL_ROOT}/lib
        -DCMAKE_BUILD_TYPE:STRING=Release
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DBUILD_TESTING=OFF
        "-DCMAKE_C_FLAGS:STRING=-O3" -DCMAKE_CXX_FLAGS=${THIRD_PARTY_CXX_FLAGS}
        -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_ROOT}
        ${piped_CMAKE_ARGS}
    ${parsed_UNPARSED_ARGUMENTS}
  )

  string(TOUPPER ${name} uname)
  file(MAKE_DIRECTORY ${INSTALL_ROOT}/include)

  set("${uname}_INCLUDE_DIR" "${INSTALL_ROOT}/include" PARENT_SCOPE)
  if (LIB_TYPE)
    set("${uname}_LIB_DIR" "${INSTALL_ROOT}/lib" PARENT_SCOPE)
    list(LENGTH LIB_FILES LIB_LEN)
    if (${LIB_LEN} GREATER 1)
      foreach (_file ${LIB_FILES})
        get_filename_component(base_name ${_file} NAME_WE)
        STRING(REGEX REPLACE "^lib" "" tname ${base_name})

        add_library(TRDP::${tname} ${LIB_TYPE} IMPORTED)
        add_dependencies(TRDP::${tname} ${name}_project)
        set_target_properties(TRDP::${tname} PROPERTIES IMPORTED_LOCATION ${_file}
                              INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_ROOT}/include)
      endforeach(_file)
    else()
        add_library(TRDP::${name} ${LIB_TYPE} IMPORTED)
        add_dependencies(TRDP::${name} ${name}_project)
        set_target_properties(TRDP::${name} PROPERTIES IMPORTED_LOCATION ${LIB_FILES}
                              INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_ROOT}/include)
    endif()
  endif()
endfunction()

set(GTEST_VERSION 1.15.2)
set(GTEST_RELEASE_URL "https://github.com/google/googletest/releases/download/v${GTEST_VERSION}/googletest-${GTEST_VERSION}.tar.gz")
set(GTEST_ARCHIVE_URL "https://github.com/google/googletest/archive/v${GTEST_VERSION}.tar.gz")
set(GTEST_TARBALL "${CMAKE_BINARY_DIR}/googletest-${GTEST_VERSION}.tar.gz")

function(download_and_validate url file out_status)
  file(DOWNLOAD "${url}" "${file}" STATUS st)
  list(GET st 0 rc)
  if(rc EQUAL 0)
    file(SIZE "${file}" sz)
    if(sz LESS 102400) # Check if file is < 100KB
      set(st "1;File too small (${sz} bytes)") # set error and error message
    endif()
  endif()
  set(${out_status} "${st}" PARENT_SCOPE)
endfunction()

if(NOT EXISTS "${GTEST_TARBALL}")
  download_and_validate("${GTEST_RELEASE_URL}" "${GTEST_TARBALL}" st1)
  list(GET st1 0 rc1)
  if(NOT rc1 EQUAL 0)
    file(REMOVE "${GTEST_TARBALL}")
    message(STATUS "Primary download failed: ${st1}. Trying archive URL...")
    download_and_validate("${GTEST_ARCHIVE_URL}" "${GTEST_TARBALL}" st2)
    list(GET st2 0 rc2)
    if(NOT rc2 EQUAL 0)
      file(REMOVE "${GTEST_TARBALL}")
      message(FATAL_ERROR "Failed to download googletest.\nRelease: ${st1}\nArchive: ${st2}")
    endif()
  endif()
endif()

FetchContent_Declare(
  gtest
  URL "${GTEST_TARBALL}"
)

FetchContent_GetProperties(gtest)
if (NOT gtest_POPULATED)
    FetchContent_Populate(gtest)
    add_subdirectory(${gtest_SOURCE_DIR} ${gtest_BINARY_DIR})
endif ()

FetchContent_Declare(
  benchmark
  URL https://github.com/google/benchmark/archive/v1.9.1.tar.gz
)

FetchContent_GetProperties(benchmark)
if (NOT benchmark_POPULATED)
    FetchContent_Populate(benchmark)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "")
    set(BENCHMARK_ENABLE_EXCEPTIONS OFF CACHE BOOL "")
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "")
    set(BENCHMARK_ENABLE_LIBPFM OFF CACHE BOOL "")
    set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "")
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "")
    set(HAVE_STD_REGEX ON CACHE BOOL "")
    add_subdirectory(${benchmark_SOURCE_DIR} ${benchmark_BINARY_DIR})
endif ()


FetchContent_Declare(
  abseil_cpp
  URL https://github.com/abseil/abseil-cpp/releases/download/20250512.1/abseil-cpp-20250512.1.tar.gz
  PATCH_COMMAND patch -p1 < "${CMAKE_CURRENT_LIST_DIR}/../patches/abseil-20250512.1.patch"
  COMMAND patch -p1 < "${CMAKE_CURRENT_LIST_DIR}/../patches/abseil-gcc-undefined-sanitizer-compilation-fix.patch"
)

FetchContent_GetProperties(abseil_cpp)
if(NOT abseil_cpp_POPULATED)
  FetchContent_Populate(abseil_cpp)
  set(BUILD_TESTING OFF CACHE INTERNAL "")
  set(ABSL_PROPAGATE_CXX_STD ON CACHE INTERNAL "")

  # If we want to override a variable in a subproject, we can temporary change the var
  # and then restore it if we use it ourselves.
  set(CMAKE_CXX_FLAGS_RELEASE_OLD ${CMAKE_CXX_FLAGS_RELEASE})
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
  # Treat Abseil source directory as a System Include Directory, to suppress warnings generated on strict builds when
  # abseil headers are included in our own code.
  include_directories(SYSTEM ${abseil_cpp_SOURCE_DIR})
  add_subdirectory(${abseil_cpp_SOURCE_DIR} ${abseil_cpp_BINARY_DIR})
  set(CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE_OLD})
endif()

if (LEGACY_GLOG)
  set(FETCHCONTENT_UPDATES_DISCONNECTED_GLOG ON CACHE BOOL "")

  FetchContent_Declare(
    glog
    GIT_REPOSITORY https://github.com/romange/glog
    GIT_TAG Absl

    GIT_PROGRESS    TRUE
    GIT_SHALLOW     TRUE
  )

  FetchContent_GetProperties(glog)
  if (NOT glog_POPULATED)
      FetchContent_Populate(glog)

    # There are bugs with libunwind on aarch64
    # Also there is something fishy with pthread_rw_lock on aarch64 - glog sporadically fails
    # inside pthreads code.
    if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "aarch64")
      set(WITH_UNWIND OFF  CACHE BOOL "")
      set(HAVE_RWLOCK OFF  CACHE BOOL "")
    endif()

      set(WITH_GTEST OFF CACHE BOOL "")
      set(BUILD_TESTING OFF CACHE BOOL "")

      set(HAVE_LIB_GFLAGS OFF CACHE BOOL "")
      set(WITH_GFLAGS OFF CACHE BOOL "")
      set(WITH_ABSL ON  BOOL "")
      add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})

      set_property(TARGET glog APPEND PROPERTY
                  INCLUDE_DIRECTORIES $<TARGET_PROPERTY:absl::flags,INTERFACE_INCLUDE_DIRECTORIES>)
  endif()

  target_link_libraries(glog PRIVATE $<BUILD_INTERFACE:absl::flags>)
endif()

find_package(Boost CONFIG REQUIRED)
Message(STATUS "Found Boost ${Boost_LIBRARY_DIRS} ${Boost_LIB_VERSION} ${Boost_VERSION}")

if(Boost_VERSION_MAJOR EQUAL 1 AND Boost_VERSION_MINOR LESS 89)
  find_package(Boost CONFIG REQUIRED COMPONENTS context system)
else()
  find_package(Boost CONFIG QUIET COMPONENTS context)
  find_package(Boost MODULE QUIET COMPONENTS system)
  if(NOT TARGET Boost::system)
    add_library(Boost::system INTERFACE IMPORTED)
    if(Boost_INCLUDE_DIRS)
      target_include_directories(Boost::system INTERFACE "${Boost_INCLUDE_DIRS}")
    endif()
    target_compile_definitions(Boost::system INTERFACE BOOST_ERROR_CODE_HEADER_ONLY)
    message(STATUS "Using header-only Boost::system (BOOST_ERROR_CODE_HEADER_ONLY)")
  endif()
endif()

add_definitions(-DBOOST_BEAST_SEPARATE_COMPILATION -DBOOST_ASIO_SEPARATE_COMPILATION)

# Optionally include gperf
if (WITH_GPERF)
  add_definitions(-DWITH_GPERF)
  # gperftools cmake build is broken https://github.com/gperftools/gperftools/issues/1321
  # Until it's fixed, I use the old configure based build.

  if (WITH_UNWIND AND (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64"))
    set(PERF_TOOLS_OPTS --enable-libunwind )
  else()
    set(PERF_TOOLS_OPTS --disable-libunwind)
  endif()

  if ("${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
    set(PERF_TOOLS_MAKE "gmake")
  else()
    set(PERF_TOOLS_MAKE "make")
  endif()

  add_third_party(
    gperf
    URL https://github.com/gperftools/gperftools/archive/gperftools-2.16.tar.gz

    # GIT_SHALLOW TRUE
    # Remove building the unneeded programs (they fail on macos)
    PATCH_COMMAND echo sed -i "/^noinst_PROGRAMS +=/d;/binary_trees binary_trees_shared/d"
                  <SOURCE_DIR>/Makefile.am
    COMMAND autoreconf -i   # update runs every time for some reason
    # CMAKE_PASS_FLAGS "-DGPERFTOOLS_BUILD_HEAP_PROFILER=OFF -DGPERFTOOLS_BUILD_HEAP_CHECKER=OFF \
    #                  -DGPERFTOOLS_BUILD_DEBUGALLOC=OFF -DBUILD_TESTING=OFF  \
    #                  -Dgperftools_build_benchmark=OFF"
    CONFIGURE_COMMAND <SOURCE_DIR>/configure --enable-frame-pointers
                       "CXXFLAGS=${THIRD_PARTY_CXX_FLAGS}" CPPFLAGS=-I<SOURCE_DIR>
                       --disable-heap-checker --disable-debugalloc --disable-heap-profiler
                       --disable-deprecated-pprof --disable-dependency-tracking
                       --disable-shared --enable-static
                       --prefix=${THIRD_PARTY_LIB_DIR}/gperf ${PERF_TOOLS_OPTS}
                       MAKE=${PERF_TOOLS_MAKE} CXX=${CMAKE_CXX_COMPILER}
    BUILD_COMMAND echo ${PERF_TOOLS_MAKE} -j4

    # install-data required by fedora
    INSTALL_COMMAND ${PERF_TOOLS_MAKE} install-exec install-data
    LIB libprofiler.a
  )
else()
  add_library(TRDP::gperf INTERFACE IMPORTED)
endif()


add_third_party(
  xxhash
  URL https://github.com/Cyan4973/xxHash/archive/v0.8.3.tar.gz

  # A bug in xxhash 0.8.1 that searches for a file that doesn't exist
  PATCH_COMMAND touch <SOURCE_DIR>/xxhsum.1
  SOURCE_SUBDIR cmake_unofficial
  CMAKE_PASS_FLAGS "-DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF"
)


add_third_party(
  uring
  URL https://github.com/axboe/liburing/archive/refs/tags/liburing-2.8.tar.gz

  CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=${THIRD_PARTY_LIB_DIR}/uring
  BUILD_COMMAND make -C src
  BUILD_IN_SOURCE 1
)

add_third_party(
  pugixml
  URL https://github.com/zeux/pugixml/archive/refs/tags/v1.15.tar.gz
)

if (WITH_AWS)
  set (AWS_PATCH_COMMAND patch -p1 -d ${THIRD_PARTY_DIR}/aws/ -i ${CMAKE_CURRENT_LIST_DIR}/../patches/aws-sdk-cpp-1.11.717.patch)

  add_third_party(
    aws
    GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
    GIT_TAG 1.11.717
    GIT_SHALLOW TRUE
    GIT_CONFIG      submodule.recurse=1 submodule.fetchJobs=10
    PATCH_COMMAND "${AWS_PATCH_COMMAND}"
    CMAKE_PASS_FLAGS "-DBUILD_ONLY=s3 -DNO_HTTP_CLIENT=ON -DENABLE_TESTING=OFF  -DENABLE_CURL_LOGGING=OFF -DAUTORUN_UNIT_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DAWS_SDK_WARNINGS_ARE_ERRORS=OFF -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_POLICY_VERSION_MINIMUM=3.5"
    LIB libaws-cpp-sdk-s3.a libaws-cpp-sdk-core.a libaws-crt-cpp.a libaws-c-mqtt.a libaws-c-event-stream.a libaws-c-s3.a libaws-c-auth.a  libaws-c-http.a libaws-c-io.a libs2n.a libaws-c-compression.a libaws-c-cal.a libaws-c-sdkutils.a libaws-checksums.a libaws-c-common.a
  )
endif()

if (WITH_GCP)
  add_third_party(
    rapidjson
    GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
    GIT_TAG ab1842a
    CMAKE_PASS_FLAGS "-DRAPIDJSON_BUILD_TESTS=OFF -DRAPIDJSON_BUILD_EXAMPLES=OFF \
                      -DRAPIDJSON_BUILD_DOC=OFF -DCMAKE_POLICY_VERSION_MINIMUM=3.5"
    LIB "none"
  )
endif()

add_third_party(
  cares
  URL https://codeload.github.com/c-ares/c-ares/tar.gz/refs/tags/v1.34.5
  CMAKE_PASS_FLAGS "-DCARES_SHARED:BOOL=OFF -DCARES_STATIC:BOOL=ON -DCARES_STATIC_PIC:BOOL=ON \
                    -DCMAKE_INSTALL_LIBDIR=lib"
)

add_third_party(
  zstd
  URL https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.zst
  SOURCE_SUBDIR "build/cmake"

  # for debug pass : "CFLAGS=-fPIC -O0 -ggdb"
  CMAKE_PASS_FLAGS "-DZSTD_BUILD_SHARED=OFF -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_TESTS=OFF"
)

add_third_party(
  expected
  GIT_REPOSITORY https://github.com/martinmoene/expected-lite.git
  GIT_TAG f17940fabae07063cabb67abf2c8d164d3146044 # Important fixes for monadic functions
  CMAKE_PASS_FLAGS "-DEXPECTED_LITE_OPT_BUILD_TESTS=0"
  LIB "none"
)

add_library(TRDP::rapidjson INTERFACE IMPORTED)
add_dependencies(TRDP::rapidjson rapidjson_project)
set_target_properties(TRDP::rapidjson PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${RAPIDJSON_INCLUDE_DIR}")

if (WITH_GPERF AND WITH_UNWIND AND (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64"))
  set_target_properties(TRDP::gperf PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES unwind)
endif()

target_compile_definitions(TRDP::pugixml INTERFACE PUGIXML_NO_EXCEPTIONS=1 PUGIXML_NO_XPATH=1)

if (APPLE)
  set_target_properties(TRDP::cares PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES resolv)
endif()

add_library(TRDP::expected INTERFACE IMPORTED)
add_dependencies(TRDP::expected expected_project)
set_target_properties(TRDP::expected PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${EXPECTED_INCLUDE_DIR}")
target_compile_definitions(TRDP::expected INTERFACE nsel_CONFIG_NO_NODISCARD=OFF)

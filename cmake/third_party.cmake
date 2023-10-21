# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
 cmake_policy(SET CMP0135 NEW)
endif()

set(THIRD_PARTY_DIR "${CMAKE_CURRENT_BINARY_DIR}/third_party")

SET_DIRECTORY_PROPERTIES(PROPERTIES EP_PREFIX ${THIRD_PARTY_DIR})

Include(ExternalProject)
Include(FetchContent)

option (WITH_UNWIND "Enable libunwind support" ON)

set(THIRD_PARTY_LIB_DIR "${THIRD_PARTY_DIR}/libs")
file(MAKE_DIRECTORY ${THIRD_PARTY_LIB_DIR})

set(THIRD_PARTY_CXX_FLAGS "-std=c++14 -O3 -DNDEBUG -fPIC")

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

#gflags
# FetchContent_Declare(
#   gflags
#   URL https://github.com/gflags/gflags/archive/v2.2.2.zip
#   # SOURCE_DIR gflags
#   PATCH_COMMAND patch -p1 < "${CMAKE_CURRENT_LIST_DIR}/../patches/gflags-v2.2.2.patch"
# )

# FetchContent_GetProperties(gflags)
# if (NOT gflags_POPULATED)
#     FetchContent_Populate(gflags)
#     set(BUILD_gflags_nothreads_LIB OFF)
#     set(BUILD_gflags_LIB ON)
#     set(GFLAGS_INSTALL_STATIC_LIBS ON)

#     add_subdirectory(${gflags_SOURCE_DIR} ${gflags_BINARY_DIR})
# endif ()


FetchContent_Declare(
  gtest
  URL https://github.com/google/googletest/archive/v1.13.0.tar.gz
)

FetchContent_GetProperties(gtest)
if (NOT gtest_POPULATED)
    FetchContent_Populate(gtest)
    add_subdirectory(${gtest_SOURCE_DIR} ${gtest_BINARY_DIR})
endif ()

FetchContent_Declare(
  benchmark
  URL https://github.com/google/benchmark/archive/v1.8.2.tar.gz
)

FetchContent_GetProperties(benchmark)
if (NOT benchmark_POPULATED)
    FetchContent_Populate(benchmark)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "")
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "")
    set(BENCHMARK_ENABLE_LIBPFM OFF CACHE BOOL "")
    add_subdirectory(${benchmark_SOURCE_DIR} ${benchmark_BINARY_DIR})
endif ()


FetchContent_Declare(
  abseil_cpp
  URL https://github.com/abseil/abseil-cpp/archive/20230802.1.tar.gz
  # GIT_REPOSITORY https://github.com/abseil/abseil-cpp
  # GIT_TAG 20230802.1
  PATCH_COMMAND patch -p1 < "${CMAKE_SOURCE_DIR}/patches/abseil-20230802.1.patch"
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
  add_subdirectory(${abseil_cpp_SOURCE_DIR} ${abseil_cpp_BINARY_DIR})
  set(CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE_OLD})
endif()

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
  # Also there is something fishy with pthread_rw_lock on aarch64 - glog sproadically fails
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


# 1.71 comes with ubuntu 20.04 so that's what we require.
find_package(Boost 1.71.0 REQUIRED COMPONENTS context system)
Message(STATUS "Found Boost ${Boost_LIBRARY_DIRS} ${Boost_LIB_VERSION} ${Boost_VERSION}")

add_definitions(-DBOOST_BEAST_SEPARATE_COMPILATION -DBOOST_ASIO_SEPARATE_COMPILATION)


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
  URL https://github.com/gperftools/gperftools/archive/gperftools-2.10.tar.gz

  GIT_SHALLOW TRUE
  PATCH_COMMAND autoreconf -i   # update runs every time for some reason
  # CMAKE_PASS_FLAGS "-DGPERFTOOLS_BUILD_HEAP_PROFILER=OFF -DGPERFTOOLS_BUILD_HEAP_CHECKER=OFF \
  #                   -DGPERFTOOLS_BUILD_DEBUGALLOC=OFF -DBUILD_TESTING=OFF  \
  #                   -Dgperftools_build_benchmark=OFF"
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --enable-frame-pointers --enable-static=yes
                      "CXXFLAGS=${THIRD_PARTY_CXX_FLAGS}"
                      --disable-heap-checker --disable-debugalloc --disable-heap-profiler
                      --disable-deprecated-pprof --enable-aggressive-decommit-by-default
                      --prefix=${THIRD_PARTY_LIB_DIR}/gperf ${PERF_TOOLS_OPTS}
                      MAKE=${PERF_TOOLS_MAKE} CXX=${CMAKE_CXX_COMPILER}
  # skip build step
  BUILD_COMMAND echo ${PERF_TOOLS_MAKE} -j4
  INSTALL_COMMAND ${PERF_TOOLS_MAKE} install-exec install-data
  LIB libprofiler.a
)

set(MIMALLOC_INCLUDE_DIR ${THIRD_PARTY_LIB_DIR}/mimalloc/include)

# asan interferes with mimalloc. See https://github.com/microsoft/mimalloc/issues/317

set (MIMALLOC_PATCH_COMMAND patch -p1 -d ${THIRD_PARTY_DIR}/mimalloc/ -i ${CMAKE_CURRENT_LIST_DIR}/../patches/mimalloc-v2.0.9.patch)

 add_third_party(mimalloc
   #GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
   #GIT_TAG v2.0.9
   URL https://github.com/microsoft/mimalloc/archive/refs/tags/v2.0.9.tar.gz
   PATCH_COMMAND "${MIMALLOC_PATCH_COMMAND}"
   # -DCMAKE_BUILD_TYPE=Release
   # Add -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS=-O0 to debug
   CMAKE_PASS_FLAGS "-DCMAKE_BUILD_TYPE=Release -DMI_BUILD_SHARED=OFF -DMI_BUILD_TESTS=OFF \
                    -DMI_INSTALL_TOPLEVEL=ON -DMI_OVERRIDE=OFF -DCMAKE_C_FLAGS=-g \
                    ${HELIO_MIMALLOC_OPTS}"

  BUILD_COMMAND make -j4 mimalloc-static
  INSTALL_COMMAND make install
  COMMAND cp <SOURCE_DIR>/include/mimalloc-types.h <SOURCE_DIR>/include/mimalloc-atomic.h
          ${MIMALLOC_INCLUDE_DIR}/
  LIB ${HELIO_MIMALLOC_LIBNAME}
)

add_third_party(jemalloc
  URL https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2
  PATCH_COMMAND ./autogen.sh
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=${THIRD_PARTY_LIB_DIR}/jemalloc --with-jemalloc-prefix=je_ --disable-libdl
)


add_third_party(
  xxhash
  URL https://github.com/Cyan4973/xxHash/archive/v0.8.1.tar.gz

  # A bug in xxhash 0.8.1 that searches for a file that doesn't exist
  PATCH_COMMAND touch <SOURCE_DIR>/xxhsum.1
  SOURCE_SUBDIR cmake_unofficial
  CMAKE_PASS_FLAGS "-DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF"
)


add_third_party(
  uring
  URL https://github.com/axboe/liburing/archive/refs/tags/liburing-2.4.tar.gz
  
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=${THIRD_PARTY_LIB_DIR}/uring
  BUILD_IN_SOURCE 1
)

add_third_party(
  rapidjson
  GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
  GIT_TAG 1a803826f1197b5e30703afe4b9c0e7dd48074f5
  CMAKE_PASS_FLAGS "-DRAPIDJSON_BUILD_TESTS=OFF -DRAPIDJSON_BUILD_EXAMPLES=OFF \
                    -DRAPIDJSON_BUILD_DOC=OFF"
  LIB "none"
)

add_third_party(
  pugixml
  URL https://github.com/zeux/pugixml/archive/refs/tags/v1.13.tar.gz
)

set (AWS_PATCH_COMMAND patch -p1 -d ${THIRD_PARTY_DIR}/aws/ -i ${CMAKE_CURRENT_LIST_DIR}/../patches/aws-sdk-cpp-3e51fa016655eeb6b6610bdf8fe7cf33ebbf3e00.patch)

add_third_party(
  aws
  GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG 3e51fa016655eeb6b6610bdf8fe7cf33ebbf3e00
  GIT_SHALLOW TRUE
  PATCH_COMMAND "${AWS_PATCH_COMMAND}"
  CMAKE_PASS_FLAGS "-DBUILD_ONLY=s3 -DNO_HTTP_CLIENT=ON -DENABLE_TESTING=OFF -DAUTORUN_UNIT_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib"
  LIB libaws-cpp-sdk-s3.a libaws-cpp-sdk-core.a libaws-crt-cpp.a libaws-c-mqtt.a libaws-c-event-stream.a libaws-c-s3.a libaws-c-auth.a  libaws-c-http.a libaws-c-io.a libs2n.a libaws-c-compression.a libaws-c-cal.a libaws-c-sdkutils.a libaws-checksums.a libaws-c-common.a
)

if(APPLE)
  message("On macOS, please install c-ares via homebrew: ")
  message("brew install c-ares && brew link c-ares")
  message("This shall also create cmake config file to guide find_package()")
  add_library(TRDP::cares SHARED IMPORTED)
  find_package(c-ares REQUIRED)
  set_target_properties(TRDP::cares PROPERTIES IMPORTED_LOCATION ${c-ares_DIR}/../../libcares.dylib)
else()
  add_third_party(
    cares
    URL https://c-ares.org/download/c-ares-1.19.0.tar.gz
    CMAKE_PASS_FLAGS "-DCARES_SHARED:BOOL=OFF -DCARES_STATIC:BOOL=ON -DCARES_STATIC_PIC:BOOL=ON \
                    -DCMAKE_INSTALL_LIBDIR=lib"
  )
endif()

add_library(TRDP::rapidjson INTERFACE IMPORTED)
add_dependencies(TRDP::rapidjson rapidjson_project)
set_target_properties(TRDP::rapidjson PROPERTIES
                      INTERFACE_INCLUDE_DIRECTORIES "${RAPIDJSON_INCLUDE_DIR}")


if (WITH_UNWIND AND (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64"))
  set_target_properties(TRDP::gperf PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES unwind)
endif()

cmake_policy (SET CMP0079 NEW)
target_link_libraries(glog PRIVATE $<BUILD_INTERFACE:absl::flags>)
target_compile_definitions(TRDP::pugixml INTERFACE PUGIXML_NO_EXCEPTIONS=1 PUGIXML_NO_XPATH=1)

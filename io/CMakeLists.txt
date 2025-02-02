find_library(ZSTD_LIB NAMES libzstd.a libzstdstatic.a zstd NAMES_PER_DIR REQUIRED)

if (ZSTD_LIB)
  message(STATUS "Found ZSTD: ${ZSTD_LIB}")
  get_filename_component(ZSTD_LIB_DIR ${ZSTD_LIB} DIRECTORY)
  set(ZSTD_INCLUDE_DIR ${ZSTD_LIB_DIR}/../include)
else()
  message(FATAL_ERROR "ZSTD library not found")
endif()

add_library(io file.cc file_util.cc io.cc line_reader.cc proc_reader.cc zstd_sinksource.cc)
target_include_directories(io PRIVATE ${ZSTD_INCLUDE_DIR})
cxx_link(io base ${ZSTD_LIB})

add_library(file ALIAS io)

cxx_test(io_test io LABELS CI)
cxx_test(file_test io DATA testdata/ids.txt.zst LABELS CI)

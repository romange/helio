add_library(io file.cc file_util.cc io.cc line_reader.cc proc_reader.cc zstd_sinksource.cc)
cxx_link(io base TRDP::zstd)

add_library(file ALIAS io)

cxx_test(io_test io LABELS CI)
cxx_test(file_test io DATA testdata/ids.txt.zst LABELS CI)

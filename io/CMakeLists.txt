add_library(io file.cc file_util.cc io.cc line_reader.cc proc_reader.cc)
cxx_link(io base)

add_library(file ALIAS io)

cxx_test(io_test io LABELS CI)

cxx_test(file_test io LABELS CI)

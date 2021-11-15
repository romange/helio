add_library(html_lib sorted_table.cc)
cxx_link(html_lib absl::strings)

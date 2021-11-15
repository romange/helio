add_library(strings_lib human_readable.cc)
cxx_link(strings_lib base absl::strings absl::str_format)

cxx_test(strings_test strings_lib LABELS CI)
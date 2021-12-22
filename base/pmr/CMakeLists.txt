# base does not depend on this lib
add_library(base_pmr arena.cc)
cxx_link(base_pmr absl_base)

cxx_test(pod_array_test LABELS CI)
cxx_test(arena_test base_pmr LABELS CI)

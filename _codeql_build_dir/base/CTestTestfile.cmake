# CMake generated Testfile for 
# Source directory: /home/runner/work/helio/helio/base
# Build directory: /home/runner/work/helio/helio/_codeql_build_dir/base
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(expected_test "/home/runner/work/helio/helio/_codeql_build_dir/expected_test")
set_tests_properties(expected_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;23;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(mpmc_bounded_queue_test "/home/runner/work/helio/helio/_codeql_build_dir/mpmc_bounded_queue_test")
set_tests_properties(mpmc_bounded_queue_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;24;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(mpsc_intrusive_queue_test "/home/runner/work/helio/helio/_codeql_build_dir/mpsc_intrusive_queue_test")
set_tests_properties(mpsc_intrusive_queue_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;25;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(abseil_test "/home/runner/work/helio/helio/_codeql_build_dir/abseil_test")
set_tests_properties(abseil_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;26;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(hash_test "/home/runner/work/helio/helio/_codeql_build_dir/hash_test")
set_tests_properties(hash_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;27;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(cuckoo_map_test "/home/runner/work/helio/helio/_codeql_build_dir/cuckoo_map_test")
set_tests_properties(cuckoo_map_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;28;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(histogram_test "/home/runner/work/helio/helio/_codeql_build_dir/histogram_test")
set_tests_properties(histogram_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;29;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(flit_test "/home/runner/work/helio/helio/_codeql_build_dir/flit_test")
set_tests_properties(flit_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;30;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(cxx_test "/home/runner/work/helio/helio/_codeql_build_dir/cxx_test")
set_tests_properties(cxx_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;31;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(string_view_sso_test "/home/runner/work/helio/helio/_codeql_build_dir/string_view_sso_test")
set_tests_properties(string_view_sso_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;32;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
add_test(ring_buffer_test "/home/runner/work/helio/helio/_codeql_build_dir/ring_buffer_test")
set_tests_properties(ring_buffer_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/base/CMakeLists.txt;33;helio_cxx_test;/home/runner/work/helio/helio/base/CMakeLists.txt;0;")
subdirs("pmr")

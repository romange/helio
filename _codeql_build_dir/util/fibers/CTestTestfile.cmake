# CMake generated Testfile for 
# Source directory: /home/runner/work/helio/helio/util/fibers
# Build directory: /home/runner/work/helio/helio/_codeql_build_dir/util/fibers
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(uring_file_test "/home/runner/work/helio/helio/_codeql_build_dir/uring_file_test")
set_tests_properties(uring_file_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/util/fibers/CMakeLists.txt;5;helio_cxx_test;/home/runner/work/helio/helio/util/fibers/CMakeLists.txt;0;")
add_test(fibers_test "/home/runner/work/helio/helio/_codeql_build_dir/fibers_test")
set_tests_properties(fibers_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/util/fibers/CMakeLists.txt;19;helio_cxx_test;/home/runner/work/helio/helio/util/fibers/CMakeLists.txt;0;")
add_test(fiber_socket_test "/home/runner/work/helio/helio/_codeql_build_dir/fiber_socket_test")
set_tests_properties(fiber_socket_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/helio/helio/cmake/internal.cmake;314;add_test;/home/runner/work/helio/helio/util/fibers/CMakeLists.txt;20;helio_cxx_test;/home/runner/work/helio/helio/util/fibers/CMakeLists.txt;0;")

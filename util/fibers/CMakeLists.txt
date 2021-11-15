add_library(fibers_ext fibers_ext.cc fiber_file.cc fiberqueue_threadpool.cc)
cxx_link(fibers_ext base io Boost::fiber absl_strings)

cxx_test(fibers_ext_test fibers_ext uring_fiber_lib epoll_fiber_lib LABELS CI)

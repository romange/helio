add_library(fibers2 fiber2.cc proactor_base.cc synchronization.cc uring_proactor.cc uring_pool.cc
            detail/scheduler.cc detail/fiber_interface.cc ../accept_server.cc 
            ../fiber_socket_base.cc ../listener_interface.cc
            ../prebuilt_asio.cc ../proactor_pool.cc ../uring/uring_socket.cc
            ../sliding_counter.cc ../varz.cc)
target_compile_definitions(fibers2 PRIVATE USE_FB2)
cxx_link(fibers2 base io TRDP::uring Boost::context Boost::headers)


add_library(fibers_ext fibers_ext.cc fiber_file.cc fiberqueue_threadpool.cc)
cxx_link(fibers_ext base io Boost::fiber absl_strings)

cxx_test(fibers_ext_test fibers_ext uring_fiber_lib epoll_fiber_lib LABELS CI)
cxx_test(fiber2_test fibers2 LABELS CI)

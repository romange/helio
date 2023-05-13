add_library(fibers2 fiber2.cc proactor_base.cc synchronization.cc uring_proactor.cc
            fiber_file.cc epoll_proactor.cc epoll_socket.cc pool.cc
            detail/scheduler.cc detail/fiber_interface.cc ../accept_server.cc
            ../fiber_socket_base.cc ../listener_interface.cc
            ../prebuilt_asio.cc ../proactor_pool.cc ../uring/uring_socket.cc ../uring/uring_file.cc
            ../sliding_counter.cc ../varz.cc fiberqueue_threadpool.cc dns_resolve.cc)
target_compile_definitions(fibers2 PRIVATE USE_FB2)
cxx_link(fibers2 base io TRDP::uring Boost::context Boost::headers TRDP::cares)

cxx_test(fiber2_test fibers2 LABELS CI)

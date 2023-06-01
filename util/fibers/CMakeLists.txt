if("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
  set(FB_LINUX_LIBS TRDP::uring)
  set(FB_LINUX_SRCS uring_proactor.cc ../uring/uring_socket.cc ../uring/uring_file.cc)
endif()

add_library(fibers2 fiber2.cc proactor_base.cc synchronization.cc
            fiber_file.cc epoll_proactor.cc epoll_socket.cc pool.cc
            detail/scheduler.cc detail/fiber_interface.cc ../accept_server.cc
            ../fiber_socket_base.cc ../listener_interface.cc
            ../prebuilt_asio.cc ../proactor_pool.cc
            ../sliding_counter.cc ../varz.cc fiberqueue_threadpool.cc dns_resolve.cc
            ${FB_LINUX_SRCS})

target_compile_definitions(fibers2 PRIVATE USE_FB2)
cxx_link(fibers2 base io ${FB_LINUX_LIBS} Boost::context Boost::headers TRDP::cares)

cxx_test(fiber2_test fibers2 LABELS CI)
cxx_test(fiber_socket_test fibers2 LABELS CI)

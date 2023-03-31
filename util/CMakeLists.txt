
add_library(proactor_lib accept_server.cc dns_resolve.cc fiber_sched_algo.cc fiber_socket_base.cc
            listener_interface.cc prebuilt_asio.cc proactor_base.cc proactor_pool.cc
            sliding_counter.cc varz.cc)

find_library(ANL_LIB libanl.a)  # getaddrinfo_a

if (NOT ANL_LIB)  # for some distributions it does not exist.
  set(ANL_LIB "")
endif()

cxx_link(proactor_lib base absl::flat_hash_map Boost::fiber Boost::headers ${ANL_LIB})

if (USE_FB2)
  cxx_test(accept_server_test fibers2 http_beast_prebuilt LABELS CI)
else()
  cxx_test(accept_server_test uring_fiber_lib epoll_fiber_lib http_beast_prebuilt LABELS CI)
endif()

find_package(OpenSSL)

add_subdirectory(fibers)
add_subdirectory(epoll)
add_subdirectory(uring)
add_subdirectory(html)
add_subdirectory(metrics)
add_subdirectory(tls)
add_subdirectory(http)
add_subdirectory(cloud)
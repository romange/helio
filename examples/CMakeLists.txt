add_subdirectory(pingserver)
add_subdirectory(redis_dict)

add_executable(echo_server echo_server.cc)
cxx_link(echo_server base uring_fiber_lib epoll_fiber_lib http_server_lib)

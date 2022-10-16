add_subdirectory(pingserver)
add_subdirectory(redis_dict)

add_executable(raw_echo_server raw_echo_server.cc)
cxx_link(raw_echo_server base Boost::context TRDP::uring)

add_executable(echo_server echo_server.cc)
cxx_link(echo_server base uring_fiber_lib epoll_fiber_lib http_server_lib)

add_executable(proactor_stress proactor_stress.cc)
cxx_link(proactor_stress base uring_fiber_lib http_server_lib)

add_executable(s3_demo s3_demo.cc)
cxx_link(s3_demo uring_fiber_lib http_client_lib aws_lib)

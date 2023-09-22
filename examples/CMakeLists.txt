add_subdirectory(pingserver)
add_subdirectory(redis_dict)

add_executable(raw_echo_server raw_echo_server.cc)
cxx_link(raw_echo_server base fibers2 TRDP::uring TRDP::gperf)

add_executable(echo_server echo_server.cc)
cxx_link(echo_server base fibers2 http_server_lib TRDP::gperf)

# add_executable(proactor_stress proactor_stress.cc)
# cxx_link(proactor_stress base uring_fiber_lib http_server_lib)

add_executable(s3_demo s3_demo.cc)
cxx_link(s3_demo aws_lib)

add_executable(s3v2_demo s3v2_demo.cc)
cxx_link(s3v2_demo base aws_lib awsv2_lib)

add_executable(https_client_cli https_client_cli.cc)
cxx_link(https_client_cli base fibers2 http_client_lib tls_lib)

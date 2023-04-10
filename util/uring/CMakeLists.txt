add_library(uring_fiber_lib uring_socket.cc uring_file.cc
            # we need prebuilt_asio for errrors support, consider using our own errors
            proactor.cc uring_pool.cc uring_fiber_algo.cc)
cxx_link(uring_fiber_lib proactor_lib io TRDP::uring)

cxx_test(proactor_test uring_fiber_lib LABELS CI)

if (USE_FB2)
cxx_test(uring_socket_test fibers2 LABELS CI)
else() 
cxx_test(uring_socket_test uring_fiber_lib LABELS CI)
endif()
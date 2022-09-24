add_library(epoll_fiber_lib proactor.cc epoll_fiber_scheduler.cc fiber_socket.cc epoll_pool.cc)
cxx_link(epoll_fiber_lib proactor_lib Boost::fiber)

cxx_test(epoll_proactor_test epoll_fiber_lib)

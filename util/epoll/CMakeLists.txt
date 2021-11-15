add_library(epoll_fiber_lib ev_controller.cc epoll_fiber_scheduler.cc fiber_socket.cc ev_pool.cc)
cxx_link(epoll_fiber_lib proactor_lib Boost::fiber)

cxx_test(ev_controller_test epoll_fiber_lib)

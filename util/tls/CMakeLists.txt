Message(STATUS "OpenSSL libs ${OPENSSL_SSL_LIBRARIES} ${OPENSSL_VERSION}")

add_library(tls_lib tls_engine.cc tls_socket.cc)

if (USE_FB2)
cxx_link(tls_lib fibers2 OpenSSL::SSL)
else()
cxx_link(tls_lib proactor_lib OpenSSL::SSL)
endif()

cxx_test(tls_engine_test tls_lib Boost::fiber LABELS CI)

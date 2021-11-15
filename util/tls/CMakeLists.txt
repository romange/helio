set(OPENSSL_ROOT_DIR "/opt/openssl/")
find_package(OpenSSL REQUIRED)
Message(STATUS "OpenSSL libs ${OPENSSL_SSL_LIBRARIES} ${OPENSSL_VERSION}")

add_library(tls_lib tls_engine.cc tls_socket.cc)
cxx_link(tls_lib proactor_lib OpenSSL::SSL)

cxx_test(tls_engine_test tls_lib Boost::fiber LABELS CI)

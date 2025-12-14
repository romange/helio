Message(STATUS "OpenSSL libs ${OPENSSL_SSL_LIBRARIES} ${OPENSSL_VERSION}")

add_library(tls_lib tls_engine.cc tls_socket.cc)

cxx_link(tls_lib fibers2 OpenSSL::SSL)
cxx_test(tls_engine_test tls_lib LABELS CI)
cxx_test(tls_socket_test tls_lib LABELS CI)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/certificates")
  set(TEST_CERT_PATH "${CMAKE_CURRENT_SOURCE_DIR}/certificates")
elseif(EXISTS "${CMAKE_SOURCE_DIR}/util/tls/certificates")
  set(TEST_CERT_PATH "${CMAKE_SOURCE_DIR}/util/tls/certificates")
else()
  message(FATAL_ERROR "No certificate directory found for TLS tests.")
endif()
target_compile_definitions(tls_socket_test PRIVATE TEST_CERT_PATH="${TEST_CERT_PATH}" DF_TESTING)

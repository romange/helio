find_package(OpenSSL)
find_package(ZLIB)

add_library(awsv2_lib aws.cc credentials_provider_chain.cc logger.cc
            http_client.cc http_client_factory.cc s3_endpoint_provider.cc
            s3_read_file.cc s3_write_file.cc)

if (APPLE)
  find_library(SECURITY Security)
else()
  set(S2N_LIRARY TRDP::s2n)
endif()

cxx_link(awsv2_lib base http_utils http_client_lib TRDP::aws-cpp-sdk-s3 TRDP::aws-cpp-sdk-core
         TRDP::aws-crt-cpp TRDP::aws-c-mqtt TRDP::aws-c-event-stream
         TRDP::aws-c-s3 TRDP::aws-c-auth  TRDP::aws-c-http TRDP::aws-c-io
         ${S2N_LIRARY} TRDP::aws-c-compression TRDP::aws-c-cal TRDP::aws-c-sdkutils
         TRDP::aws-checksums TRDP::aws-c-common OpenSSL::Crypto ZLIB::ZLIB ${CMAKE_DL_LIBS} ${SECURITY})

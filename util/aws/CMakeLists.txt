find_package(OpenSSL)
find_package(ZLIB)

add_library(awsv2_lib aws.cc)

if (APPLE)
  find_library(SECURITY Security)
  cxx_link(awsv2_lib TRDP::aws-cpp-sdk-s3 TRDP::aws-cpp-sdk-core
           TRDP::aws-crt-cpp TRDP::aws-c-mqtt TRDP::aws-c-event-stream
           TRDP::aws-c-s3 TRDP::aws-c-auth  TRDP::aws-c-http TRDP::aws-c-io
           TRDP::aws-c-compression TRDP::aws-c-cal TRDP::aws-c-sdkutils
           TRDP::aws-checksums TRDP::aws-c-common OpenSSL::Crypto ZLIB::ZLIB ${SECURITY})
else()
  cxx_link(awsv2_lib TRDP::aws-cpp-sdk-s3 TRDP::aws-cpp-sdk-core
           TRDP::aws-crt-cpp TRDP::aws-c-mqtt TRDP::aws-c-event-stream
           TRDP::aws-c-s3 TRDP::aws-c-auth  TRDP::aws-c-http TRDP::aws-c-io
           TRDP::s2n TRDP::aws-c-compression TRDP::aws-c-cal TRDP::aws-c-sdkutils
           TRDP::aws-checksums TRDP::aws-c-common OpenSSL::Crypto ZLIB::ZLIB)
endif()

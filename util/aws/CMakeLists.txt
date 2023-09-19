find_package(OpenSSL)
find_package(ZLIB)

add_library(awsv2_lib aws.cc logger.cc)

if (APPLE)
  find_library(SECURITY Security)
else()
  set(S2N_LIRARY TRDP::s2n)
endif()

cxx_link(awsv2_lib base TRDP::aws-cpp-sdk-s3 TRDP::aws-cpp-sdk-core
         TRDP::aws-crt-cpp TRDP::aws-c-mqtt TRDP::aws-c-event-stream
         TRDP::aws-c-s3 TRDP::aws-c-auth  TRDP::aws-c-http TRDP::aws-c-io
         ${S2N_LIRARY} TRDP::aws-c-compression TRDP::aws-c-cal TRDP::aws-c-sdkutils
         TRDP::aws-checksums TRDP::aws-c-common OpenSSL::Crypto ZLIB::ZLIB ${CMAKE_DL_LIBS} ${SECURITY})

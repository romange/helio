find_package(LibXml2)

add_library(aws_lib aws.cc s3.cc s3_file.cc)

if(TARGET LibXml2::LibXml2)
  cxx_link(aws_lib base OpenSSL::Crypto LibXml2::LibXml2 TRDP::rapidjson http_utils http_client_lib io)
else()
  Message(WARNING "LibXml2 not found, if you need aws_lib, install LibXml2 with 'sudo apt install libxml2-dev'")
endif()

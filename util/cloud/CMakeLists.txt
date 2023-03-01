find_package(LibXml2)

add_library(aws_lib aws.cc cloud_utils.cc s3.cc s3_file.cc)

if(TARGET LibXml2::LibXml2)
  cxx_link(aws_lib base crypto LibXml2::LibXml2 TRDP::rapidjson http_utils io)
else()
  Message(WARNING "LibXml2 not found, if you need aws_lib, install LibXml2 with 'sudo apt install libxml2-dev'")
endif()

find_package(LibXml2)

add_library(aws_lib aws.cc s3.cc s3_file.cc)

cxx_link(aws_lib base OpenSSL::Crypto TRDP::rapidjson http_utils 
         TRDP::pugixml http_client_lib)
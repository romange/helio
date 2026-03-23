find_package(OpenSSL)

add_library(aws_cloud_lib aws_creds_provider.cc s3_storage.cc)
cxx_link(aws_cloud_lib cloud_lib strings_lib TRDP::rapidjson TRDP::pugixml OpenSSL::Crypto)

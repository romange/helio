find_package(LibXml2)

add_library(aws_lib aws.cc s3.cc)
cxx_link(aws_lib base crypto ${LIBXML2_LIBRARIES})
target_include_directories(aws_lib PRIVATE ${LIBXML2_INCLUDE_DIR})
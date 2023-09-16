add_library(awsv2_lib aws.cc http_client.cc http_client_factory.cc)

cxx_link(awsv2_lib base http_utils http_client_lib ${AWSSDK_LINK_LIBRARIES})

add_subdirectory(s3)

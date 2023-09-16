add_library(awss3_lib write_file.cc)

cxx_link(awss3_lib base ${AWSSDK_LINK_LIBRARIES})

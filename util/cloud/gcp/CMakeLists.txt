add_library(gcp_lib gcs.cc gcs_file.cc gcp_utils.cc)

cxx_link(gcp_lib cloud_lib strings_lib TRDP::rapidjson)

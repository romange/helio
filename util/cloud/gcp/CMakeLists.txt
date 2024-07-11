add_library(gcp_lib gcs.cc)

cxx_link(gcp_lib http_client_lib strings_lib TRDP::rapidjson)

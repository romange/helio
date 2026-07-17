add_library(azure_lib azure.cc storage.cc)
cxx_link(azure_lib cloud_lib http_client_lib strings_lib TRDP::pugixml TRDP::rapidjson)
helio_cxx_test(azure_creds_provider_test azure_lib LABELS CI)

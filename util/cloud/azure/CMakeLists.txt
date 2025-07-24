add_library(azure_lib azure.cc storage.cc)
cxx_link(azure_lib cloud_lib http_client_lib strings_lib TRDP::pugixml)

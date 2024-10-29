add_library(azure_lib azure.cc)
cxx_link(azure_lib http_client_lib strings_lib TRDP::pugixml)

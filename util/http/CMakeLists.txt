add_library(http_beast_prebuilt prebuilt_beast.cc)
cxx_link(http_beast_prebuilt Boost::system)

add_library(http_utils encoding.cc http_common.cc)
cxx_link(http_utils base http_beast_prebuilt)

add_library(http_server_lib status_page.cc profilez_handler.cc http_handler.cc)
cxx_link(http_server_lib absl::strings absl::time base http_beast_prebuilt http_utils 
         metrics TRDP::gperf)

add_executable(http_main http_main.cc)

add_library(http_client_lib http_client.cc https_client_pool.cc)

cxx_link(http_client_lib fibers2 http_beast_prebuilt http_utils tls_lib)
cxx_link(http_main fibers2 html_lib http_server_lib TRDP::mimalloc)

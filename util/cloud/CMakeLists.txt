
add_library(cloud_lib utils.cc)
cxx_link(cloud_lib http_client_lib strings_lib)

add_subdirectory(azure)
if (WITH_GCP)
  add_subdirectory(gcp)
endif()

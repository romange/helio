
add_library(cloud_lib utils.cc platform_info.cc)
cxx_link(cloud_lib io http_client_lib strings_lib)

helio_cxx_test(platform_info_test cloud_lib LABELS CI)

add_subdirectory(azure)
if (WITH_GCP)
  add_subdirectory(gcp)
endif()
if (WITH_AWS_CLOUD)
  add_subdirectory(aws)
endif()

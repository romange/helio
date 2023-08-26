cxx_test(accept_server_test fibers2 http_beast_prebuilt LABELS CI)


find_package(OpenSSL)

add_subdirectory(fibers)
add_subdirectory(html)
add_subdirectory(metrics)
add_subdirectory(tls)
add_subdirectory(http)
add_subdirectory(cloud)

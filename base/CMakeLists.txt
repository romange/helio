add_library(base hash.cc histogram.cc init.cc logging.cc proc_util.cc
            pthread_utils.cc varz_node.cc cuckoo_map.cc)

cxx_link(base glog::glog absl::flags_parse atomic rt # rt for timer_create etc.
         absl::strings absl::symbolize absl::time absl::failure_signal_handler TRDP::xxhash)

# Define default gtest_main for tests.
add_library(gtest_main_ext gtest_main.cc)
target_link_libraries(gtest_main_ext gmock base benchmark TRDP::gperf)

add_subdirectory(pmr)

cxx_test(mpmc_bounded_queue_test base LABELS CI)
cxx_test(abseil_test base absl::str_format LABELS CI)
cxx_test(hash_test base absl::random_random LABELS CI)
cxx_test(cuckoo_map_test base absl::flat_hash_map LABELS CI)
cxx_test(histogram_test base LABELS CI)
cxx_test(malloc_test base TRDP::mimalloc TRDP::jemalloc  LABELS CI)
cxx_test(flit_test base LABELS CI)
cxx_test(cxx_test base absl::flat_hash_map LABELS CI)

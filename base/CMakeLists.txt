add_library(base cpu_features.cc hash.cc histogram.cc init.cc logging.cc proc_util.cc
    pthread_utils.cc varz_node.cc cuckoo_map.cc io_buf.cc segment_pool.cc)

if (LEGACY_GLOG) 
  set(LOG_LIBS glog::glog)
else()

  target_compile_definitions(base PUBLIC -DUSE_ABSL_LOG=1)
  set(LOG_LIBS absl::check absl::log -Wl,--whole-archive absl::log_flags
      absl::log_initialize)
endif()

cxx_link(base ${LOG_LIBS} absl::flags_parse 
    absl::strings absl::symbolize absl::time absl::failure_signal_handler 
    TRDP::xxhash TRDP::expected)

# Define default gtest_main for tests.
add_library(gtest_main_ext gtest_main.cc)
target_link_libraries(gtest_main_ext gmock base benchmark TRDP::gperf)

add_subdirectory(pmr)

helio_cxx_test(expected_test base LABELS CI)
helio_cxx_test(mpmc_bounded_queue_test base LABELS CI)
helio_cxx_test(mpsc_intrusive_queue_test base LABELS CI)
helio_cxx_test(abseil_test base absl::str_format LABELS CI)
helio_cxx_test(hash_test base absl::random_random LABELS CI)
helio_cxx_test(cuckoo_map_test base absl::flat_hash_map LABELS CI)
helio_cxx_test(histogram_test base LABELS CI)
helio_cxx_test(flit_test base LABELS CI)
helio_cxx_test(cxx_test base absl::flat_hash_map LABELS CI)
helio_cxx_test(string_view_sso_test base LABELS CI)
helio_cxx_test(ring_buffer_test base LABELS CI)

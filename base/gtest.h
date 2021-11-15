// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

namespace base {

// Used to avoid compiler optimizations for these benchmarks.
// Just call it with the return value of the function.
template <typename T> void sink_result(const T& t0) {
  volatile T t = t0;
  (void)t;
}

// Returns unique test dir - the same for the run of the process.
// The directory is cleaned automatically if all the tests finish succesfully.
std::string GetTestTempDir();
std::string GetTestTempPath(const std::string& base_name);

std::string RandStr(const unsigned len);

std::string ProgramRunfile(const std::string& relative_path);  // relative to runtime dir.

}  // namespace base

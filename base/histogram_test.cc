// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/histogram.h"

#include <gtest/gtest.h>

#include "base/integral_types.h"
#include "base/logging.h"

namespace base {

class HistogramTest : public testing::Test {
 protected:
  Histogram hist_;
};

TEST_F(HistogramTest, Basic) {
  EXPECT_LT(sizeof(hist_), 100);
  for (int i = 0; i < 100; ++i) {
    hist_.Add(10);
  }
  EXPECT_EQ(10, hist_.Average());
  EXPECT_EQ(1000, hist_.Sum());
}

TEST_F(HistogramTest, Grow) {
  for (int i = 0; i < 100; ++i) {
    hist_.Add(i * i);
  }
  for (int i = 0; i < 20; ++i)
    hist_.Add(1);
  LOG(INFO) << hist_.ToString();
}

TEST_F(HistogramTest, MultiplePercentiles) {
  // Add some test data
  for (int i = 1; i <= 100; ++i) {
    hist_.Add(i);
  }

  // Test single pass computation of multiple percentiles
  auto [p25, p50, p75, p90, p99] = hist_.Percentiles(25, 50, 75, 90, 99);

  // Verify they match individual Percentile() calls
  EXPECT_NEAR(p25, hist_.Percentile(25), 0.5);
  EXPECT_NEAR(p50, hist_.Percentile(50), 0.5);
  EXPECT_NEAR(p75, hist_.Percentile(75), 0.5);
  EXPECT_NEAR(p90, hist_.Percentile(90), 0.5);
  EXPECT_NEAR(p99, hist_.Percentile(99), 0.5);

  // Test with different subset (must be sorted)
  auto [p50_b, p90_b, p99_b] = hist_.Percentiles(50, 90, 99);
  EXPECT_NEAR(p50_b, p50, 0.01);
  EXPECT_NEAR(p90_b, p90, 0.01);
  EXPECT_NEAR(p99_b, p99, 0.01);
}

}  // namespace base

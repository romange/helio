// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _BASE_HISTOGRAM_H_
#define _BASE_HISTOGRAM_H_

#include <array>
#include <string>
#include <vector>

#include "base/integral_types.h"

namespace base {

class Histogram {
 public:
  Histogram();
  ~Histogram();

  Histogram(const Histogram&) = default;
  Histogram(Histogram&&) = default;

  Histogram& operator=(const Histogram&) = default;
  Histogram& operator=(Histogram&&) = default;

  void Clear();

  void Add(double value, uint64_t count = 1);
  void Merge(const Histogram& other);

  std::string ToString() const;

  uint64_t count() const {
    return num_;
  }

  double Median() const {
    return Percentile(50.0);
  }

  // p in [0, 100].
  double Percentile(double p) const;

  // Compute multiple percentiles in a single pass. More efficient than calling
  // Percentile() multiple times.
  // IMPORTANT: Percentiles must be provided in ascending order (e.g., 50, 90, 99).
  // Usage: auto [p50, p90, p99] = hist.Percentiles(50, 90, 99);
  template <typename... Args>
  std::array<double, sizeof...(Args)> Percentiles(Args... percentiles) const {
    static_assert((std::is_convertible_v<Args, double> && ...), "All arguments must be convertible to double");
    return PercentilesImpl(std::array<double, sizeof...(Args)>{static_cast<double>(percentiles)...});
  }

  double Average() const;
  double StdDev() const;
  double max() const {
    return max_;
  }

  double min() const {
    return min_;
  }

 private:
  // Implementation for Percentiles() - computes multiple percentiles in one pass.
  template <size_t N>
  std::array<double, N> PercentilesImpl(const std::array<double, N>& percentiles) const;

  // returns the interpolated value based on its position inside the bucket and
  // the number of items in the bucket. position should be valid
  // (i.e. in range [1, buckets_[bucket]])
  double InterpolateVal(unsigned bucket, uint64_t position) const;

  static std::pair<double, double> BucketLimits(unsigned bucket);
  static double BucketAverage(unsigned b);

  double min_;
  double max_;
  double sum_;
  double sum_squares_;
  uint64_t num_;

  enum { kNumBuckets = 154 };
  static const double kBucketLimit[kNumBuckets];

  std::vector<uint64_t> buckets_;
};

}  // namespace base

#endif  // _BASE_HISTOGRAM_H_

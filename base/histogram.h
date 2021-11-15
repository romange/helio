// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _BASE_HISTOGRAM_H_
#define _BASE_HISTOGRAM_H_

#include "base/integral_types.h"

#include <string>
#include <vector>

namespace base {

class Histogram {
 public:
  Histogram();
  ~Histogram();

  void Clear();
  void Add(double value) { Add(value, 1);}
  void Add(double value, uint32 count);
  void Merge(const Histogram& other);

  std::string ToString() const;

  unsigned long count() const { return num_; }

  double Median() const {
    return Percentile(50.0);
  }

  // p in [0, 100].
  double Percentile(double p) const;
  double Average() const;
  double StdDev() const;
  double max() const { return max_;}
  double min() const { return min_;}

  // trim_low_percentile, trim_high_percentile in [0, 100].
  // Returns truncated mean according to http://en.wikipedia.org/wiki/Truncated_mean
  // Do not use it - it's broken! See the failing test.
  // double TruncatedMean(double trim_low_percentile, double trim_high_percentile) const;

 private:
  typedef unsigned long ulong;

  // returns the interpolated value based on its position inside the bucket and
  // the number of items in the bucket. position should be valid
  // (i.e. in range [1, buckets_[bucket]])
  double InterpolateVal(unsigned bucket, ulong position) const;
  double SumFirstK(unsigned bucket, ulong position) const;

  static std::pair<double, double> BucketLimits(unsigned bucket);
  static double BucketAverage(unsigned b);

  double min_;
  double max_;
  double sum_;
  double sum_squares_;
  uint32 num_;

  enum { kNumBuckets = 154 };
  static const double kBucketLimit[kNumBuckets];

  std::vector<uint32> buckets_;
};

}  // namespace base

#endif  // _BASE_HISTOGRAM_H_

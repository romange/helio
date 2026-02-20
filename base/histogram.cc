// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_FORMAT_MACROS 1

#include "base/histogram.h"

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <cstring>

#include "base/bits.h"
#include "base/logging.h"

namespace base {

using namespace std;

const double Histogram::kBucketLimit[kNumBuckets] = {
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    12,
    14,
    16,
    18,
    20,
    25,
    30,
    35,
    40,
    45,
    50,
    60,
    70,
    80,
    90,
    100,
    120,
    140,
    160,
    180,
    200,
    250,
    300,
    350,
    400,
    450,
    500,
    600,
    700,
    800,
    900,
    1000,
    1200,
    1400,
    1600,
    1800,
    2000,
    2500,
    3000,
    3500,
    4000,
    4500,
    5000,
    6000,
    7000,
    8000,
    9000,
    10000,
    12000,
    14000,
    16000,
    18000,
    20000,
    25000,
    30000,
    35000,
    40000,
    45000,
    50000,
    60000,
    70000,
    80000,
    90000,
    100000,
    120000,
    140000,
    160000,
    180000,
    200000,
    250000,
    300000,
    350000,
    400000,
    450000,
    500000,
    600000,
    700000,
    800000,
    900000,
    1000000,
    1200000,
    1400000,
    1600000,
    1800000,
    2000000,
    2500000,
    3000000,
    3500000,
    4000000,
    4500000,
    5000000,
    6000000,
    7000000,
    8000000,
    9000000,
    10000000,
    12000000,
    14000000,
    16000000,
    18000000,
    20000000,
    25000000,
    30000000,
    35000000,
    40000000,
    45000000,
    50000000,
    60000000,
    70000000,
    80000000,
    90000000,
    100000000,
    120000000,
    140000000,
    160000000,
    180000000,
    200000000,
    250000000,
    300000000,
    350000000,
    400000000,
    450000000,
    500000000,
    600000000,
    700000000,
    800000000,
    900000000,
    1000000000,
    1200000000,
    1400000000,
    1600000000,
    1800000000,
    2000000000,
    2500000000.0,
    3000000000.0,
    3500000000.0,
    4000000000.0,
    4500000000.0,
    5000000000.0,
    6000000000.0,
    7000000000.0,
    8000000000.0,
    9000000000.0,
    1e200,
};

inline pair<double, double> Histogram::BucketLimits(unsigned b) {
  double low = (b == 0) ? 0.0 : kBucketLimit[b - 1];
  return pair<double, double>(low, kBucketLimit[b]);
}

inline double Histogram::BucketAverage(unsigned b) {
  auto limits = BucketLimits(b);
  return (limits.first + limits.second) / 2;
}

Histogram::Histogram() {
  Clear();
}

Histogram::~Histogram() {
}

void Histogram::Clear() {
  min_ = kBucketLimit[kNumBuckets - 1];
  max_ = 0;
  num_ = 0;
  sum_ = 0;
  buckets_.clear();
}

void Histogram::Add(double value, uint64_t count) {
  auto it = upper_bound(kBucketLimit, kBucketLimit + kNumBuckets - 1, value);
  size_t b = it - kBucketLimit;
  if (buckets_.size() <= b) {
    buckets_.resize(b + 1);
    if (buckets_.capacity() > kNumBuckets) {
      buckets_.shrink_to_fit();
    }
  }

  buckets_[b] += count;
  if (min_ > value)
    min_ = value;
  if (max_ < value)
    max_ = value;
  num_ += count;
  sum_ += value * count;
}

void Histogram::Merge(const Histogram& other) {
  if (other.min_ < min_)
    min_ = other.min_;
  if (other.max_ > max_)
    max_ = other.max_;
  num_ += other.num_;
  sum_ += other.sum_;

  if (other.buckets_.size() > buckets_.size()) {
    buckets_.resize(other.buckets_.size());
  }

  for (unsigned b = 0; b < other.buckets_.size(); ++b) {
    buckets_[b] += other.buckets_[b];
  }
}

double Histogram::Percentile(double p) const {
  return PercentilesImpl(std::array<double, 1>{p})[0];
}

template <size_t N>
std::array<double, N> Histogram::PercentilesImpl(const std::array<double, N>& percentiles) const {
  std::array<double, N> result;

  for (size_t i = 1; i < percentiles.size(); ++i) {
    DCHECK_GE(percentiles[i], percentiles[i - 1]) << "Percentiles must be in ascending order.";
  }

  if (num_ == 0) {
    result.fill(0.0);
    return result;
  }

  // Percentiles must be sorted in ascending order
  size_t next_idx = 0;
  uint64_t sum = 0;

  for (unsigned b = 0; b < buckets_.size() && next_idx < N; b++) {
    sum += buckets_[b];

    // Process all percentiles whose threshold falls in the accumulated sum
    while (next_idx < N) {
      uint64_t threshold = num_ * (percentiles[next_idx] / 100.0);

      if (sum >= threshold) {
        uint64_t left_sum = sum - buckets_[b];
        result[next_idx] = InterpolateVal(b, threshold - left_sum);
        ++next_idx;
      } else {
        break;
      }
    }
  }

  // Handle any remaining percentiles (should all map to max_)
  while (next_idx < N) {
    result[next_idx] = max_;
    ++next_idx;
  }

  return result;
}

// Explicit template instantiations for common cases to reduce code bloat
template std::array<double, 1> Histogram::PercentilesImpl(const std::array<double, 1>&) const;
template std::array<double, 2> Histogram::PercentilesImpl(const std::array<double, 2>&) const;
template std::array<double, 3> Histogram::PercentilesImpl(const std::array<double, 3>&) const;
template std::array<double, 4> Histogram::PercentilesImpl(const std::array<double, 4>&) const;
template std::array<double, 5> Histogram::PercentilesImpl(const std::array<double, 5>&) const;

double Histogram::Average() const {
  if (num_ == 0)
    return 0;
  return sum_ / num_;
}

void Histogram::Decay() {
  for (auto& b : buckets_)
    b >>= 1;
  num_ >>= 1;
  sum_ *= 0.5;
}

string Histogram::ToString() const {
  string r;
  char buf[300];
  snprintf(buf, sizeof(buf), "Count: %" PRIu64 " Average: %.4f\n", num_, Average());
  r.append(buf);
  snprintf(buf, sizeof(buf), "Min: %.4f  Median: %.4f  Max: %.4f\n", (num_ == 0 ? 0.0 : min_),
           Median(), max_);
  r.append(buf);
  r.append("------------------------------------------------------\n");
  const double mult = 100.0 / num_;
  double sum = 0;

  char to_buf[100];
  for (unsigned b = 0; b < buckets_.size(); b++) {
    if (buckets_[b] <= 0.01)
      continue;
    sum += buckets_[b];
    double from = (b == 0) ? 0.0 : kBucketLimit[b - 1];
    if (b == kNumBuckets - 1) {
      strcpy(to_buf, "INF");
    } else {
      snprintf(to_buf, sizeof(to_buf), "%7.0f", kBucketLimit[b]);
    }
    snprintf(buf, sizeof(buf), "[ %7.0f, %s ) %" PRIu64 " %7.3f%% %7.3f%% ",
             from,                // left
             to_buf,              // right
             buckets_[b],         // count
             mult * buckets_[b],  // percentage
             mult * sum);         // cumulative percentage
    r.append(buf);

    // Add hash marks based on percentage; 20 marks for 100%.
    int marks = static_cast<int>(20 * (double(buckets_[b]) / num_) + 0.5);
    r.append(marks, '#');
    r.push_back('\n');
  }
  return r;
}

double Histogram::InterpolateVal(unsigned bucket, uint64_t position) const {
  auto limits = BucketLimits(bucket);

  // We divide by n+1 according to
  // http://en.wikipedia.org/wiki/Uniform_distribution_(continuous)#Order_statistics
  double pos = double(position) / double(buckets_[bucket] + 1);
  double r = limits.first + (limits.second - limits.first) * pos;
  if (r < min_)
    r = min_;
  if (r > max_)
    r = max_;
  return r;
}

}  // namespace base

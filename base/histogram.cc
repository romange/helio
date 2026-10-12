// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_FORMAT_MACROS 1

#include "base/histogram.h"

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cmath>
#include <cstring>

#include "base/bits.h"
#include "base/logging.h"

namespace base {

using namespace std;

namespace {

// Above 10, every decade [10^d, 10^(d+1)) is split by the same 16 limits:
// 10^(d-1) * {12, 14, 16, 18, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100}.
// Since all limits are integers and multiples of 10^(d-1), the bucket of v is determined by its
// two leading digits p = v / 10^(d-1), which lets us compute the index instead of searching.
constexpr unsigned kBucketsPerDecade = 16;
constexpr unsigned kNumBucketsTotal = 154;
constexpr uint8_t kDecadeLimits[] = {12, 14, 16, 18, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90};

constexpr auto kPow10 = [] {
  std::array<uint64_t, 20> res{};
  uint64_t p = 1;
  for (auto& v : res) {
    v = p;
    p *= 10;  // wraps after the last element, which is fine for unsigned
  }
  return res;
}();

// kTwoDigitOffset[p] is the number of kDecadeLimits that are <= p, for p in [10, 99].
constexpr auto kTwoDigitOffset = [] {
  std::array<uint8_t, 100> res{};
  for (unsigned p = 0; p < res.size(); ++p) {
    for (uint8_t limit : kDecadeLimits)
      res[p] += (limit <= p);
  }
  return res;
}();

// Exact floor(log10(v)) for v > 0.
constexpr unsigned Log10Floor(uint64_t v) {
  unsigned bit_width = 64 - __builtin_clzll(v);
  unsigned t = ((bit_width * 1233) >> 12);  // floor(log10(2^bits)) approximation
  return t - (v < kPow10[t]);
}

constexpr unsigned ComputeBucketIndex(uint64_t v) {
  if (v < 10)
    return v;
  unsigned d = Log10Floor(v);
  if (d >= 10)
    return kNumBucketsTotal - 1;
  return 10 + kBucketsPerDecade * (d - 1) + kTwoDigitOffset[v / kPow10[d - 1]];
}

// Upper limit of the last bucket, which catches everything >= 9e9.
constexpr double kLastLimit = 1e200;

// Exclusive upper limits of the buckets: 1..10, then kDecadeLimits and 100 scaled to each decade.
constexpr auto kBucketLimit = [] {
  std::array<double, kNumBucketsTotal> res{};
  unsigned b = 0;
  for (unsigned v = 1; v <= 10; ++v)
    res[b++] = v;
  for (unsigned d = 1; b + 1 < res.size(); ++d) {
    for (uint8_t limit : kDecadeLimits) {
      if (b + 1 < res.size())
        res[b++] = limit * double(kPow10[d - 1]);
    }
    if (b + 1 < res.size())
      res[b++] = 100 * double(kPow10[d - 1]);
  }
  res[b] = kLastLimit;
  return res;
}();

// Lookup table for small values which cover typical latencies in usec. Avoids the division.
constexpr unsigned kSmallValues = 1024;
constexpr auto kSmallBucket = [] {
  std::array<uint8_t, kSmallValues> res{};
  for (unsigned v = 0; v < res.size(); ++v)
    res[v] = ComputeBucketIndex(v);
  return res;
}();

}  // namespace

static_assert(Histogram::kNumBuckets == kNumBucketsTotal);
static_assert(kBucketLimit[152] == 9e9);
static_assert(ComputeBucketIndex(9'000'000'000) == kNumBucketsTotal - 1);
static_assert(ComputeBucketIndex(8'999'999'999) == kNumBucketsTotal - 2);

unsigned Histogram::BucketIndex(double value) {
  if (value >= 0 && value < kSmallValues)
    return kSmallBucket[static_cast<unsigned>(value)];
  if (value < 0)
    return 0;
  // Negated so that NaN lands in the last bucket, same as upper_bound would place it.
  if (!(value < 1e10))
    return kNumBuckets - 1;

  // All limits are integers, so truncation does not change the bucket.
  return ComputeBucketIndex(static_cast<uint64_t>(value));
}

double Histogram::BucketLimit(unsigned b) {
  return kBucketLimit[b];
}

inline pair<double, double> Histogram::BucketLimits(unsigned b) {
  double low = (b == 0) ? 0.0 : kBucketLimit[b - 1];
  return pair<double, double>(low, kBucketLimit[b]);
}

Histogram::Histogram() {
  Clear();
}

Histogram::~Histogram() {
}

void Histogram::Clear() {
  min_ = kLastLimit;
  max_ = 0;
  num_ = 0;
  sum_ = 0;
  buckets_.clear();
}

void Histogram::Add(double value, uint64_t count) {
  DCHECK_LT(count, uint64_t(1) << (64 - kFracBits)) << "count overflows fixed point";
  unsigned b = BucketIndex(value);
  if (buckets_.size() <= b) {
    buckets_.resize(b + 1);
    if (buckets_.capacity() > kNumBuckets) {
      buckets_.shrink_to_fit();
    }
  }

  buckets_[b] += count << kFracBits;
  if (min_ > value)
    min_ = value;
  if (max_ < value)
    max_ = value;
  num_ += count << kFracBits;
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
      // Rank of the percentile, rounded up and at least 1 so that it always falls inside
      // a non-empty bucket. Multiply first: num_ * p is exact for integral p.
      uint64_t threshold =
          std::max<uint64_t>(1, std::ceil(double(num_) * percentiles[next_idx] / 100.0));

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
  return sum_ * kOne / num_;
}

void Histogram::Decay() {
  uint64_t new_num = 0;
  for (auto& b : buckets_) {
    b >>= 1;
    new_num += b;
  }
  num_ = new_num;
  sum_ *= 0.5;
}

string Histogram::ToString() const {
  // Weights become fractional after Decay(). Print them exactly so that bucket weights
  // add up to the total, and keep the integer format for the common undecayed case.
  auto format_weight = [](uint64_t w) {
    static_assert(kFracBits == 8, "fraction digits below assume 1/256 = 0.00390625");
    char buf[48];
    int len = snprintf(buf, sizeof(buf), "%" PRIu64, w >> kFracBits);
    if (uint64_t frac = w % kOne; frac != 0) {
      // k/256 always has exactly 8 decimal digits: k * 0.00390625.
      len += snprintf(buf + len, sizeof(buf) - len, ".%08" PRIu64, frac * 390625);
      while (buf[len - 1] == '0')
        --len;
    }
    return string(buf, len);
  };

  string r;
  char buf[300];
  snprintf(buf, sizeof(buf), "Count: %s Average: %.4f\n", format_weight(num_).c_str(), Average());
  r.append(buf);
  snprintf(buf, sizeof(buf), "Min: %.4f  Median: %.4f  Max: %.4f\n", (num_ == 0 ? 0.0 : min_),
           Median(), max_);
  r.append(buf);
  r.append("------------------------------------------------------\n");
  const double mult = 100.0 / num_;
  double sum = 0;

  char to_buf[100];
  for (unsigned b = 0; b < buckets_.size(); b++) {
    if (buckets_[b] == 0)
      continue;
    sum += buckets_[b];
    double from = (b == 0) ? 0.0 : kBucketLimit[b - 1];
    if (b == kNumBuckets - 1) {
      strcpy(to_buf, "INF");
    } else {
      snprintf(to_buf, sizeof(to_buf), "%7.0f", kBucketLimit[b]);
    }
    snprintf(buf, sizeof(buf), "[ %7.0f, %s ) %s %7.3f%% %7.3f%% ",
             from,                                // left
             to_buf,                              // right
             format_weight(buckets_[b]).c_str(),  // count
             mult * buckets_[b],                  // percentage
             mult * sum);                         // cumulative percentage
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
  // position and buckets_ are in fixed point, hence kOne instead of 1.
  double pos = double(position) / double(buckets_[bucket] + kOne);
  double r = limits.first + (limits.second - limits.first) * pos;
  if (r < min_)
    r = min_;
  if (r > max_)
    r = max_;
  return r;
}

}  // namespace base

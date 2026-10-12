// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/histogram.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <random>

#include "base/gtest.h"
#include "base/integral_types.h"
#include "base/logging.h"

namespace base {
using namespace std;

namespace {

// The original bucket lookup, used as the reference for BucketIndex().
unsigned UpperBoundIndex(double value) {
  static const auto limits = [] {
    array<double, Histogram::kNumBuckets> res;
    for (unsigned b = 0; b < res.size(); ++b)
      res[b] = Histogram::BucketLimit(b);
    return res;
  }();
  return upper_bound(limits.begin(), limits.end() - 1, value) - limits.begin();
}

}  // namespace

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
  EXPECT_EQ(1000, hist_.sum());
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

TEST_F(HistogramTest, Decay) {
  for (int i = 0; i < 100; ++i) {
    hist_.Add(10);
  }
  EXPECT_EQ(100u, hist_.count());
  EXPECT_EQ(1000.0, hist_.sum());

  hist_.Decay();
  EXPECT_EQ(50u, hist_.count());
  EXPECT_EQ(500.0, hist_.sum());

  // Percentiles should still be computable after decay.
  EXPECT_NEAR(10.0, hist_.Percentile(50), 1.0);

  // Buckets with a single sample keep half of their weight instead of being rounded down
  // to zero, so the total weight halves as well.
  Histogram h2;
  h2.Add(1);
  h2.Add(100);
  EXPECT_EQ(2u, h2.count());
  h2.Decay();
  EXPECT_EQ(1u, h2.count());
  EXPECT_EQ(100, h2.Percentile(100));

  // Decayed weights are printed exactly, so bucket weights add up to the total.
  string str = h2.ToString();
  EXPECT_NE(string::npos, str.find("Count: 1 ")) << str;
  EXPECT_NE(string::npos, str.find(") 0.50 ")) << str;
}

TEST_F(HistogramTest, BucketIndex) {
  // Exhaustive over small integers, which covers the lookup table and the first decades.
  for (uint64_t v = 0; v < 2'000'000; ++v) {
    ASSERT_EQ(UpperBoundIndex(v), Histogram::BucketIndex(v)) << v;
  }

  // Around each bucket limit.
  for (unsigned b = 0; b + 1 < Histogram::kNumBuckets; ++b) {
    double limit = Histogram::BucketLimit(b);
    for (double v : {limit - 1, limit - 0.5, nextafter(limit, 0.0), limit, nextafter(limit, 1e300),
                     limit + 0.5, limit + 1}) {
      ASSERT_EQ(UpperBoundIndex(v), Histogram::BucketIndex(v)) << v;
    }
  }

  // Random values spread across the whole range, including fractions.
  mt19937_64 rng(42);
  uniform_real_distribution<double> exponent(-1, 12);
  for (unsigned i = 0; i < 1'000'000; ++i) {
    double v = pow(10.0, exponent(rng));
    ASSERT_EQ(UpperBoundIndex(v), Histogram::BucketIndex(v)) << v;
  }

  for (double v : {-1e300, -1.0, -0.5, -0.0, 0.0, 0.5, 1e10, 1e19, 1e20, 1e200, 1e300,
                   numeric_limits<double>::infinity(), numeric_limits<double>::quiet_NaN()}) {
    EXPECT_EQ(UpperBoundIndex(v), Histogram::BucketIndex(v)) << v;
  }
}

TEST_F(HistogramTest, PercentileRank) {
  // The median of three samples is the second one. Truncating the rank used to pick the first.
  hist_.Add(1);
  hist_.Add(1000);
  hist_.Add(1000000);
  EXPECT_NEAR(1000, hist_.Percentile(50), 200);
  EXPECT_NEAR(1, hist_.Percentile(0), 0.01);
  EXPECT_EQ(1000000, hist_.Percentile(100));
}

TEST_F(HistogramTest, DecayKeepsTail) {
  // ~0.1% of the samples are slow and spread over many buckets in [1ms, 100ms), so each tail
  // bucket holds a single sample or none at decay time. With integer counts, Decay() rounded all
  // of them down to zero and the tail vanished from the percentiles.
  constexpr unsigned kPeriod = 1 << 14;
  vector<double> tail;
  for (unsigned b = 0; b < Histogram::kNumBuckets; ++b) {
    if (Histogram::BucketLimit(b) >= 1000 && Histogram::BucketLimit(b) < 100000)
      tail.push_back(Histogram::BucketLimit(b));
  }
  ASSERT_GT(tail.size(), 20u);

  size_t next_tail = 0;
  for (unsigned round = 0; round < 50; ++round) {
    for (unsigned i = 0; i < kPeriod; ++i) {
      hist_.Add(i % 1024 == 0 ? tail[next_tail++ % tail.size()] : 100);
    }
    hist_.Decay();
  }
  EXPECT_GE(hist_.Percentile(99.95), 1000);
  EXPECT_NEAR(100, hist_.Percentile(50), 20);
}

namespace {

enum Distribution { kNarrow = 0, kWide = 1 };

// kNarrow: log-normal latencies around 100us, typical for a healthy server.
// kWide: log-uniform over [1, 1e9), defeats branch prediction in the bucket search.
vector<double> GenValues(int dist) {
  mt19937_64 rng(7);
  vector<double> res(1 << 16);
  lognormal_distribution<double> narrow(log(100.0), 0.7);
  uniform_real_distribution<double> exponent(0, 9);
  for (auto& v : res) {
    v = round(dist == kNarrow ? narrow(rng) : pow(10.0, exponent(rng)));
  }
  return res;
}

}  // namespace

static void BM_BucketIndexUpperBound(benchmark::State& state) {
  vector<double> values = GenValues(state.range(0));
  size_t i = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(UpperBoundIndex(values[i++ & (values.size() - 1)]));
  }
}
BENCHMARK(BM_BucketIndexUpperBound)->Arg(kNarrow)->Arg(kWide);

static void BM_BucketIndex(benchmark::State& state) {
  vector<double> values = GenValues(state.range(0));
  size_t i = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(Histogram::BucketIndex(values[i++ & (values.size() - 1)]));
  }
}
BENCHMARK(BM_BucketIndex)->Arg(kNarrow)->Arg(kWide);

static void BM_Add(benchmark::State& state) {
  vector<double> values = GenValues(state.range(0));
  Histogram hist;
  size_t i = 0;
  for (auto _ : state) {
    hist.Add(values[i++ & (values.size() - 1)]);
  }
  benchmark::DoNotOptimize(hist.count());
}
BENCHMARK(BM_Add)->Arg(kNarrow)->Arg(kWide);

static void BM_Percentiles(benchmark::State& state) {
  Histogram hist;
  for (double v : GenValues(state.range(0)))
    hist.Add(v);
  for (auto _ : state) {
    benchmark::DoNotOptimize(hist.Percentiles(50, 90, 99, 99.9));
  }
}
BENCHMARK(BM_Percentiles)->Arg(kNarrow)->Arg(kWide);

}  // namespace base

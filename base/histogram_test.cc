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

// The original hand-written bucket limits. Kept here as an independent reference for the
// limits and the index that the histogram computes.
// clang-format off
constexpr double kReferenceLimits[] = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    12, 14, 16, 18, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100,
    120, 140, 160, 180, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000,
    1.2e3, 1.4e3, 1.6e3, 1.8e3, 2e3, 2.5e3, 3e3, 3.5e3, 4e3, 4.5e3, 5e3, 6e3, 7e3, 8e3, 9e3, 1e4,
    1.2e4, 1.4e4, 1.6e4, 1.8e4, 2e4, 2.5e4, 3e4, 3.5e4, 4e4, 4.5e4, 5e4, 6e4, 7e4, 8e4, 9e4, 1e5,
    1.2e5, 1.4e5, 1.6e5, 1.8e5, 2e5, 2.5e5, 3e5, 3.5e5, 4e5, 4.5e5, 5e5, 6e5, 7e5, 8e5, 9e5, 1e6,
    1.2e6, 1.4e6, 1.6e6, 1.8e6, 2e6, 2.5e6, 3e6, 3.5e6, 4e6, 4.5e6, 5e6, 6e6, 7e6, 8e6, 9e6, 1e7,
    1.2e7, 1.4e7, 1.6e7, 1.8e7, 2e7, 2.5e7, 3e7, 3.5e7, 4e7, 4.5e7, 5e7, 6e7, 7e7, 8e7, 9e7, 1e8,
    1.2e8, 1.4e8, 1.6e8, 1.8e8, 2e8, 2.5e8, 3e8, 3.5e8, 4e8, 4.5e8, 5e8, 6e8, 7e8, 8e8, 9e8, 1e9,
    1.2e9, 1.4e9, 1.6e9, 1.8e9, 2e9, 2.5e9, 3e9, 3.5e9, 4e9, 4.5e9, 5e9, 6e9, 7e9, 8e9, 9e9, 1e200,
};
// clang-format on
static_assert(size(kReferenceLimits) == Histogram::kNumBuckets);

// The original bucket lookup, used as the reference for BucketIndex().
unsigned UpperBoundIndex(double value) {
  return upper_bound(begin(kReferenceLimits), end(kReferenceLimits) - 1, value) -
         begin(kReferenceLimits);
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
  EXPECT_NE(string::npos, str.find(") 0.5 ")) << str;

  // The smallest weight, 1/256, is still printed rather than rounded to zero.
  for (unsigned i = 0; i < 7; ++i)
    h2.Decay();
  str = h2.ToString();
  EXPECT_NE(string::npos, str.find("Count: 0.0078125 ")) << str;
  EXPECT_NE(string::npos, str.find(") 0.00390625 ")) << str;
}

TEST_F(HistogramTest, BucketLimits) {
  for (unsigned b = 0; b < Histogram::kNumBuckets; ++b) {
    EXPECT_EQ(kReferenceLimits[b], Histogram::BucketLimit(b)) << b;
  }
}

TEST_F(HistogramTest, BucketIndex) {
  // Exhaustive over small integers, which covers the lookup table and the first decades.
  for (uint64_t v = 0; v < 2'000'000; ++v) {
    ASSERT_EQ(UpperBoundIndex(v), Histogram::BucketIndex(v)) << v;
  }

  // Around each bucket limit.
  for (unsigned b = 0; b + 1 < Histogram::kNumBuckets; ++b) {
    double limit = kReferenceLimits[b];
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

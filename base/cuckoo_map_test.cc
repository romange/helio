// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/cuckoo_map.h"

#include <absl/container/flat_hash_set.h>

#include <random>
#include <unordered_set>

#include "base/gtest.h"

DEFINE_int32(shrink_items, 200, "");

namespace base {

struct cityhash32 {
  size_t operator()(uint64 val) const {
    return uint32(val >> 32) ^ uint32(val);
  }
};

class CuckooMapTest : public testing::Test {};

using namespace std;

TEST_F(CuckooMapTest, BasicMapSeq) {
  CuckooMap<int> m;
  EXPECT_EQ(CuckooMapTable::npos, m.find(200));

  m.SetEmptyKey(0);
  uint32 kLength = 100000;
  for (uint32 k = 1; k <= kLength; ++k) {
    int data = k + 117;
    pair<CuckooMapTable::dense_id, bool> res = m.Insert(k, data);
    EXPECT_TRUE(res.second);
    ASSERT_EQ(k, m.FromDenseId(res.first).first);
    int* val = m.FromDenseId(res.first).second;
    ASSERT_TRUE(val != nullptr);
    ASSERT_EQ(data, *val);
    ASSERT_EQ(res.first, m.find(k));
  }
  auto res = m.Insert(1, 10);
  EXPECT_FALSE(res.second);
  EXPECT_EQ(1, m.FromDenseId(res.first).first);
  for (uint32 k = 1; k <= kLength; ++k) {
    ASSERT_NE(CuckooMapTable::npos, m.find(k));
  }
  for (uint32 k = kLength + 1; k <= kLength * 2; ++k) {
    ASSERT_EQ(CuckooMapTable::npos, m.find(k));
  }

  EXPECT_EQ(kLength, m.size());
  LOG(INFO) << "Utilization " << m.utilization();

  m.Clear();
  EXPECT_TRUE(m.empty());
  EXPECT_EQ(0, m.size());
}

TEST_F(CuckooMapTest, RandomInput) {
  std::mt19937_64 dre;

  CuckooMap<uint64> m;
  m.SetEmptyKey(0);
  const uint32 kLength = 100000;
  for (uint32_t k = 0; k < kLength; ++k) {
    uint64 v = dre();
    while (v == 0 || m.find(v) != CuckooMapTable::npos) {
      v = dre();
    }
    uint64 data = v * 2;
    pair<CuckooMapTable::dense_id, bool> res = m.Insert(v, data);
    EXPECT_TRUE(res.second);
    ASSERT_EQ(res.first, m.find(v));
    auto key_val = m.FromDenseId(res.first);
    ASSERT_EQ(v, key_val.first);
    ASSERT_TRUE(key_val.second);
    ASSERT_EQ(data, *key_val.second);
  }
  EXPECT_EQ(kLength, m.size());
}

TEST_F(CuckooMapTest, ReserveSizes) {
  std::default_random_engine dre;
  std::uniform_int_distribution<unsigned> di(1, 22000);
  for (unsigned i = 0; i < 10000; ++i) {
    CuckooSet set1(di(dre));

    ASSERT_EQ(0, set1.value_size());

    size_t cap1 = set1.Capacity();
    CuckooSet set2(cap1);
    ASSERT_EQ(cap1, set2.Capacity());
  }
}

TEST_F(CuckooMapTest, Compact) {
  for (uint64_t iter = 17; iter <= unsigned(FLAGS_shrink_items); ++iter) {
    LOG(INFO) << "Iter " << iter;
    CuckooMap<uint64> m;
    m.SetEmptyKey(0);
    for (uint64_t k = 1; k < iter; ++k) {
      m.Insert(k * k, k);
    }

    m.Compact(1.05);
    uint32 count = 0;
    for (CuckooMapTable::dense_id i = 0; i < m.Capacity(); ++i) {
      auto res_pair = m.FromDenseId(i);
      uint64 key = res_pair.first;
      if (key == 0)
        continue;  // empty value.
      ++count;
      ASSERT_EQ(i, m.find(key)) << "Can not finde consistent dense_id for " << key;
      ASSERT_EQ(key, (*res_pair.second) * (*res_pair.second));
    }
    EXPECT_EQ(count, m.size());
    for (uint64 k = 1; k < iter; ++k) {
      CuckooMapTable::dense_id id = m.find(k * k);
      ASSERT_NE(CuckooMapTable::npos, id);
      ASSERT_EQ(k * k, m.FromDenseId(id).first);
    }
  }
}

// Crash at Compact() if has less than 4 elements.
TEST_F(CuckooMapTest, CompactBug) {
  CuckooMap<int> m(2000);

  m.SetEmptyKey(0);
  m.Insert(1, 1);
  m.Insert(2, 1);
  m.Insert(3, 1);
  m.Compact(1.1);
}

constexpr int kLevel1 = 1000;
constexpr int kLevel2 = 10000;
constexpr int kLevel3 = 100000;

static void BM_InsertDenseSet(benchmark::State& state) {
  unsigned iters = state.range(0);

  while (state.KeepRunning()) {
    ::absl::flat_hash_set<uint64, cityhash32> set;
    for (unsigned i = 0; i < iters; ++i) {
      set.insert(1 + (i + 1) * i);
    }
  }
}
BENCHMARK(BM_InsertDenseSet)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

class StringWrapper {
 public:
  string s;
  StringWrapper() {
  }
  StringWrapper(const StringWrapper& a) : s(a.s) {
    CHECK(a.s.empty());
  }

  StringWrapper(StringWrapper&& a) : s(std::move(a.s)) {
  }

  StringWrapper& operator=(const StringWrapper& o) = default;
  bool operator==(const StringWrapper& a) const {
    return a.s == this->s;
  }
};

struct TestHash {
  size_t operator()(const StringWrapper& w) const {
    return std::hash<string>()(w.s);
  }
};

static void BM_InsertDenseString(benchmark::State& state) {
  unsigned iters = state.range(0);
  std::vector<StringWrapper> vals(iters);

  while (state.KeepRunning()) {
    ::absl::flat_hash_set<StringWrapper, TestHash> string_set;
    state.PauseTiming();
    for (unsigned i = 0; i < iters; ++i) {
      vals[i].s = std::to_string(i + 100);
    }
    state.ResumeTiming();

    for (unsigned i = 0; i < iters; ++i) {
      string_set.insert(std::move(vals[i]));
    }
  }
}
BENCHMARK(BM_InsertDenseString)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_InsertCuckoo(benchmark::State& state) {
  unsigned iters = state.range(0);
  while (state.KeepRunning()) {
    CuckooMapTable m(0, int(iters * 1.3));
    m.SetEmptyKey(0);
    m.SetGrowth(1.5);

    for (uint64 i = 0; i < iters; ++i) {
      m.Insert(1 + (i + 1) * i, nullptr);
    }
  }
}
BENCHMARK(BM_InsertCuckoo)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_InsertUnorderedSet(benchmark::State& state) {
  unsigned iters = state.range(0);
  while (state.KeepRunning()) {
    std::unordered_set<uint32, cityhash32> set;
    for (uint64 i = 0; i < iters; ++i) {
      sink_result(set.insert(i + 1));
    }
  }
}
BENCHMARK(BM_InsertUnorderedSet)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_FindCuckooSetSeq(benchmark::State& state) {
  unsigned iters = state.range(0);
  CuckooSet m(unsigned(iters * 1.3));
  m.SetEmptyKey(0);
  for (unsigned i = 0; i < iters; ++i) {
    m.Insert(i + 1);
  }
  LOG(INFO) << "BM_FindCuckooSetSeq: " << iters << " " << m.bytes_allocated();

  while (state.KeepRunning()) {
    for (uint32 i = 1; i <= iters; ++i) {
      CuckooMapTable::dense_id d = m.find(i);
      DCHECK_NE(CuckooMapTable::npos, d);
      sink_result(d);
      sink_result(m.find(iters + i + 1));
      sink_result(m.find(2 * iters + i + 1));
    }
  }
}
BENCHMARK(BM_FindCuckooSetSeq)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_FindUnorderedSet(benchmark::State& state) {
  std::unordered_set<uint64, cityhash32> set;
  unsigned iters = state.range(0);
  for (unsigned i = 0; i < iters; ++i) {
    set.insert(i + 1);
  }
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < iters; ++i) {
      sink_result(set.find(i + 1));
    }
  }
}
BENCHMARK(BM_FindUnorderedSet)->Arg(kLevel1)->Arg(kLevel2);

static void BM_FindDenseSetSeq(benchmark::State& state) {
  absl::flat_hash_set<uint64, cityhash32> set;
  unsigned iters = state.range(0);
  for (unsigned i = 0; i < iters; ++i) {
    set.insert(i + 1);
  }
  set.rehash(0);
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < iters; ++i) {
      sink_result(set.find(i + 1));
      sink_result(set.find(iters + i + 1));
      sink_result(set.find(2 * iters + i + 1));
    }
  }
}
BENCHMARK(BM_FindDenseSetSeq)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_FindDenseSetRandom(benchmark::State& state) {
  absl::flat_hash_set<uint64, cityhash32> set;

  std::mt19937_64 dre(10);
  unsigned iters = state.range(0);
  std::vector<uint64> vals(iters, 0);
  for (unsigned i = 0; i < iters; ++i) {
    vals[i] = dre();
    if (vals[i] == 0)
      vals[i] = 1;
    set.insert(vals[i]);
  }
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < iters; ++i) {
      sink_result(set.find(vals[i]));
      sink_result(set.find(i + 1));
      sink_result(set.find(iters + i + 1));
    }
  }
}
BENCHMARK(BM_FindDenseSetRandom)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_FindCuckooRandom(benchmark::State& state) {
  unsigned iters = state.range(0);
  CuckooMapTable m(0, unsigned(iters * 1.3));
  m.SetEmptyKey(0);
  std::mt19937_64 dre(20);

  std::vector<uint64> vals(iters, 0);
  for (unsigned i = 0; i < iters; ++i) {
    vals[i] = dre();
    if (vals[i] == 0)
      vals[i] = 1;
    m.Insert(vals[i], nullptr);
  }
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < iters; ++i) {
      sink_result(m.find(vals[i]));  // to simulate hits
      // to simulate 66% of misses
      sink_result(m.find(i + 1));
      sink_result(m.find(iters + i + 1));
    }
  }
}
BENCHMARK(BM_FindCuckooRandom)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_FindCuckooRandomAfterCompact(benchmark::State& state) {
  unsigned iters = state.range(0);
  CuckooMapTable m(0, unsigned(iters * 1.3));
  m.SetEmptyKey(0);
  std::mt19937_64 dre(20);

  std::vector<uint64> vals(iters, 0);
  for (unsigned i = 0; i < iters; ++i) {
    vals[i] = dre();
    if (vals[i] == 0)
      vals[i] = 1;
    m.Insert(vals[i], nullptr);
  }
  CHECK(m.Compact(1.05));
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < iters; ++i) {
      sink_result(m.find(vals[i]));  // to simulate hits
      sink_result(m.find(i + 1));    // to simulate misses
    }
  }
}
BENCHMARK(BM_FindCuckooRandomAfterCompact)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

static void BM_CuckooCompact(benchmark::State& state) {
  unsigned iters = state.range(0);
  CuckooMapTable m(0, unsigned(iters * 1.3));
  m.SetEmptyKey(0);
  std::mt19937_64 dre(20);
  std::vector<uint64> vals(iters, 0);
  for (unsigned i = 0; i < iters; ++i) {
    vals[i] = dre();
    if (vals[i] == 0)
      vals[i] = 1;
    m.Insert(vals[i], nullptr);
  }
  LOG(INFO) << "BM_CuckooCompact before compact: " << iters << " " << m.BytesAllocated();
  while (state.KeepRunning()) {
    CHECK(m.Compact(1.05));
    LOG(INFO) << "BM_CuckooCompact after compact: " << iters << " " << m.BytesAllocated();
  }
}
BENCHMARK(BM_CuckooCompact)->Arg(kLevel1)->Arg(kLevel2)->Arg(kLevel3);

}  // namespace base

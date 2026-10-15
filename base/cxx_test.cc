// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <absl/container/flat_hash_set.h>

#include <chrono>
#include <thread>

#include "base/cpu_features.h"
#include "base/cycle_clock.h"
#include "base/gtest.h"
#include "base/iterator.h"
#include "base/logging.h"
#include "base/random.h"
#include "base/string_view_sso.h"
#include "iterator.h"

using namespace std;

namespace base {

class CxxTest : public testing::Test {
 protected:
};

struct T {
  T(const T&) = delete;
  T(T&&) {
  }
};

T fn(T t) {
  return t;  // move used implicitly
}

struct A {};

A fn() {
  A t;
  return t;
}

static unsigned creations = 0;
static unsigned dtors = 0;
static unsigned move_ctors = 0;
static unsigned copy_ctors = 0;
static unsigned moves_ops = 0;
static unsigned copy_ops = 0;

class MyType {
 public:
  MyType(std::string str) : mName(std::move(str)) {
    creations++;
  }

  ~MyType() {
    dtors++;
  }

  MyType(const MyType& other) : mName(other.mName) {
    copy_ctors++;
  }

  // without noexcept vector does not use move c'tor.
  MyType(MyType&& other) noexcept : mName(std::move(other.mName)) {
    move_ctors++;
  }

  MyType& operator=(const MyType& other) {
    if (this != &other)
      mName = other.mName;
    copy_ops++;
    return *this;
  }

  MyType& operator=(MyType&& other) noexcept {
    if (this != &other)
      mName = std::move(other.mName);
    moves_ops++;
    return *this;
  }

 private:
  std::string mName;
};

template <typename T> class Wrapper {
 public:
  Wrapper(T s) : t(std::move(s)) {
  }

  T t;
};

Wrapper<MyType> GetWrapper() {
  return MyType{"bar"};
};

struct HasVector {
  vector<int> vals;
};

class Pointer1 {
  int ival_;
  string sval_;

 public:
  Pointer1(int i, string s) : ival_(i), sval_(s) {
  }

  class Wrapper {
   public:
    Wrapper(int& a, string& b) : first(a), second(b) {
    }

    Wrapper* operator->() {
      return this;
    }

    int& first;
    string& second;
  };

  Wrapper operator->() {
    return Wrapper{ival_, sval_};
  }
};

TEST_F(CxxTest, Move) {
  MyType t{"foo"};
  MyType a = std::move(t);
  EXPECT_EQ(0, copy_ctors);

  vector<MyType> v;
  v.emplace_back("b1");
  v.emplace_back("b2");
  v.emplace_back("b3");
  v.emplace_back("b4");
  EXPECT_EQ(0, copy_ctors);
  EXPECT_EQ(0, copy_ops);
  EXPECT_GT(move_ctors, 2);

  auto w = GetWrapper();
  EXPECT_EQ(0, copy_ctors);
}

TEST_F(CxxTest, UnderDebugger) {
  vector<HasVector> table;

  vector<int> ints(1024);
  table.emplace_back(HasVector{.vals = std::move(ints)});
  table.emplace_back();  // verified that HasVector was moved without copying the array.
}

TEST_F(CxxTest, Arrow) {
  Pointer1 p1{5, "roman"};
  EXPECT_EQ(5, p1->first);
  EXPECT_EQ("roman", p1->second);
}

TEST_F(CxxTest, Random) {
  SplitMix64 rand1;
  Xoroshiro128p rand2;
}

TEST_F(CxxTest, Iterator) {
  vector<string> v1 = {"a", "bb", "ccc"};
  vector<string_view> v2 = {"a", "bb", "ccc"};

  variant<vector<string>, vector<string_view>> curv = v2;

  auto views = base::it::Wrap([](const auto& str) { return std::string_view(str); }, curv);
  auto lengths = base::it::Transform([](std::string_view str) { return str.length(); }, views);
  auto sum = std::reduce(lengths.begin(), lengths.end());

  EXPECT_EQ(sum, 6);
}

TEST_F(CxxTest, CPUFeatures) {
  CpuFeatures features = GetCpuFeatures();
  (void)features;
}

TEST_F(CxxTest, CycleClock) {
  const auto start = chrono::steady_clock::now();
  const uint64_t start_cycles = CycleClock::Now();
  this_thread::sleep_for(100ms);
  const uint64_t cycle_usec = CycleClock::ToUsec(CycleClock::Now() - start_cycles);
  const auto real_usec =
      chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();

  // CycleClock durations must match real time in every build mode.
  EXPECT_NEAR(cycle_usec, real_usec, real_usec / 5);
}

TEST_F(CxxTest, CycleClockConversionRange) {
  const uint64_t freq = CycleClock::Frequency();
  ASSERT_GT(freq, 62'500u);
  auto ref_to_usec = [freq](uint64_t c) {
    return uint64_t((static_cast<unsigned __int128>(c) * 1000'000u) / freq);
  };
  auto ref_from_usec = [freq](uint64_t u) {
    return (static_cast<unsigned __int128>(u) * freq) / 1000'000u;
  };

  // First cycle count whose product with 1e6 does not fit into 64 bits.
  constexpr uint64_t kBoundary = UINT64_MAX / 1000'000ULL + 1;
  for (uint64_t c : {uint64_t(0), uint64_t(1), freq, kBoundary - 1, kBoundary, kBoundary + 1,
                     kBoundary + freq, UINT64_MAX / 2, UINT64_MAX}) {
    const uint64_t ref = ref_to_usec(c), res = CycleClock::ToUsec(c);
    EXPECT_GE(res, ref) << c;
    EXPECT_LE(res - ref, 1 + (c >> 60)) << c;
  }
  EXPECT_EQ(CycleClock::ToUsec(freq), 1000'000u);
  EXPECT_EQ(CycleClock::ToUsec(freq * 3600), uint64_t(3600) * 1000'000);
  EXPECT_GT(CycleClock::ToUsec(kBoundary), 0u);
  for (uint64_t c = kBoundary - 10; c <= kBoundary + 10; ++c) {
    EXPECT_GE(CycleClock::ToUsec(c + 1), CycleClock::ToUsec(c)) << c;
  }

  for (uint64_t u : {uint64_t(0), uint64_t(1), uint64_t(999'999), uint64_t(1000'000),
                     uint64_t(36000) * 1000'000, UINT64_MAX / freq}) {
    const uint64_t ref = uint64_t(ref_from_usec(u)), res = CycleClock::FromUsec(u);
    EXPECT_GE(res, ref) << u;
    EXPECT_LE(res - ref, 1 + (u >> 32)) << u;
  }
  EXPECT_EQ(CycleClock::FromUsec(1000'000), freq);
  if (freq > 1000'000) {
    EXPECT_EQ(CycleClock::FromUsec(UINT64_MAX), UINT64_MAX);
  }
  EXPECT_EQ(CycleClock::ToUsec(CycleClock::FromUsec(1000'000)), 1000'000u);
}

}  // namespace base

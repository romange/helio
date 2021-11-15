// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <random>

#include "base/flit.h"
#include "base/logging.h"
#include "base/gtest.h"


namespace base {
using namespace std;

static_assert(flit::Trait<uint32_t>::kMaxSize == 5, "");
static_assert(flit::Trait<uint16_t>::kMaxSize == 3, "");
static_assert(flit::Trait<uint64_t>::kMaxSize == 9, "");

inline int flit64enc(void* buf, uint64_t v) {
  int lzc = 64;
  if (v) lzc = __builtin_clzll(v);
  if (lzc > 56) {
    *(uint8_t*)buf = (uint8_t)v << 1 | 1;
    return 1;
  }
  if (lzc < 8) {
    uint8_t* p = (uint8_t*)buf;
    *p++ = 0;
    *(uint64_t*)p = v;
    return 9;
  }

  // count extra bytes
  unsigned e = ((63 - lzc) * 2454267027) >> 34;  // (63 - lzc) / 7

  v <<= 1;
  v |= 1;
  v <<= e;
  *(uint64_t*)buf = v;

  return e + 1;
}

inline int flit64dec(uint64_t* v, const void* buf) {
  uint64_t x = *(uint64_t*)buf;

  int tzc = 8;
  if (x) tzc = __builtin_ctzll(x);
  if (tzc > 7) {
   const uint8_t* cp = (const uint8_t*)buf + 1;
   *v = *(const uint64_t*)cp;
   return 9;
  }

  static const uint64_t mask[8] = {
    0xff,
    0xffff,
    0xffffff,
    0xffffffff,
    0xffffffffff,
    0xffffffffffff,
    0xffffffffffffff,
    0xffffffffffffffff,
  };
  x &= mask[tzc];

  // const here seems to ensure that 'size' is not aliased by '*v'
  const int size = tzc + 1;

  *v = x >> size;

  return size;
}

class FlitTest : public testing::Test {

protected:
  unsigned Flit64(uint64_t val) {
    return flit::Encode64(val, buf_);
  }

  unsigned UnFlit64(uint64_t* val) {
    return flit::ParseT(buf_, val);
  }

  uint8_t buf_[9];
};

#define TEST_CONST(x, y) \
  ASSERT_EQ(y, Flit64(x)); \
  ASSERT_EQ(y, flit::ParseLengthT<uint64_t>(buf_)); \
  ASSERT_EQ(y, UnFlit64(&val)); \
  EXPECT_EQ((x), val)

TEST_F(FlitTest, Flit) {
  uint64_t val = 0;

  TEST_CONST(0, 1);
  TEST_CONST(127, 1);
  TEST_CONST(128, 2);
  TEST_CONST(255, 2);
  TEST_CONST((1<<14) - 1, 2);
  TEST_CONST((1<<21) - 1, 3);
  TEST_CONST((1ULL << 32), 5);
  TEST_CONST((1ULL << 31), 5);
  TEST_CONST((1ULL<<56), 9);
  TEST_CONST((1ULL<<63), 9);
  TEST_CONST(85039090594426347ULL, 9);
}


static std::mt19937_64 rnd_engine;

uint64_t RandUint64() {
  unsigned bit_len = (rnd_engine() % 64) + 1;
  return rnd_engine() & ((1ULL << bit_len) - 1);
}

static void FillEncoded(uint8_t* buf, unsigned num) {
  for (unsigned i = 0; i < num; ++i) {
    volatile uint64_t val = RandUint64();
    buf += flit::Encode64(val, buf);
  }
}

constexpr unsigned kBatchLen = 1000;

template <typename T> void BM_FlitEncode(benchmark::State& state) {
  T input[kBatchLen];
  std::generate(input, input + ABSL_ARRAYSIZE(input), RandUint64);
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBatchLen * 10]);

  while (state.KeepRunning()) {
    uint8_t* next = buf.get();
    for (unsigned i = 0; i < ABSL_ARRAYSIZE(input); i +=4) {
      next += flit::EncodeT<T>(input[i], next);
      next += flit::EncodeT<T>(input[i] + 1, next);
      next += flit::EncodeT<T>(input[i] + 2, next);
      next += flit::EncodeT<T>(input[i] + 3, next);
    }
  }
}
BENCHMARK_TEMPLATE(BM_FlitEncode, uint32_t);
BENCHMARK_TEMPLATE(BM_FlitEncode, uint64_t);


static void BM_FlitEncodeGold(benchmark::State& state) {
  uint64_t input[kBatchLen];
  std::generate(input, input + ABSL_ARRAYSIZE(input), RandUint64);
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBatchLen * 10]);

  while (state.KeepRunning()) {
    uint8_t* next = buf.get();
    for (unsigned i = 0; i < ABSL_ARRAYSIZE(input); i +=4) {
      next += flit64enc(next, input[i]);
      next += flit64enc(next, input[i] + 1);
      next += flit64enc(next, input[i] + 2);
      next += flit64enc(next, input[i] + 3);
    }
    benchmark::DoNotOptimize(next);
  }
}
BENCHMARK(BM_FlitEncodeGold);


static void BM_FlitDecode(benchmark::State& state) {
  uint8_t buf[kBatchLen * 9];
  FillEncoded(buf, kBatchLen);

  while (state.KeepRunning()) {
    uint64_t val = 0;
    const uint8_t* rn = buf;
    for (unsigned i = 0; i < kBatchLen /4; ++i) {
      rn += flit::Parse64Fast(rn, &val);
      rn += flit::Parse64Fast(rn, &val);
      rn += flit::Parse64Fast(rn, &val);
      rn += flit::Parse64Fast(rn, &val);
      sink_result(val);
    }
  }
}
BENCHMARK(BM_FlitDecode);

static void BM_FlitDecodeGold(benchmark::State& state) {
  uint8_t buf[kBatchLen * 9];
  FillEncoded(buf, kBatchLen);

  while (state.KeepRunning()) {
    uint64_t val = 0;
    const uint8_t* rn = buf;
    for (unsigned i = 0; i < kBatchLen /4; ++i) {
      rn += flit64dec(&val, rn);
      rn += flit64dec(&val, rn);
      rn += flit64dec(&val, rn);
      rn += flit64dec(&val, rn);
      sink_result(val);
    }
  }
}
BENCHMARK(BM_FlitDecodeGold);

}  // namespace util

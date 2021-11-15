// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/hash.h"

#include <absl/random/random.h>
#include "base/gtest.h"
#include "base/logging.h"
#include "base/aquahash.h"
#include "base/zipf_gen.h"

using namespace std;

namespace base {


class HashTest : public testing::Test {
 protected:
};

TEST_F(HashTest, Basic) {
  EXPECT_EQ(187264267u, XXHash32(32));

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>("foo");
  __m128i res = AquaHash::SmallKeyAlgorithm(ptr, 3);
  __int128 val;
  _mm_store_si128((__m128i*)&val, res);
  EXPECT_NE(0, val);
}

TEST_F(HashTest, Zipf) {
  ZipfianGenerator zipf(0, 9, 0.99);
  absl::BitGen gen;

  for (unsigned i = 0; i < 100; ++i) {
    LOG(INFO) << zipf.Next(gen);
  }
}

}  // namespace base

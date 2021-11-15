// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/pod_array.h"

#include "base/gtest.h"

namespace base {
class PodArrayTest {
};


TEST(BitsTest, Padded) {
  PODArray<uint16_t, 16> arr;
  typedef decltype(arr)::value_type value_t;
  arr.push_back(0);
  EXPECT_EQ(0, (ptrdiff_t)arr.data() % arr.alignment_v ) << arr.data();
  EXPECT_EQ(arr.alignment_v / sizeof(value_t), arr.capacity());

  for (unsigned i = 1; i < 1024; ++i)
    arr.push_back(i);
  for (unsigned i = 0; i < 1024; ++i) {
    ASSERT_EQ(i, arr[i]);
  }
  EXPECT_EQ(1024, arr.allocated_size() / sizeof(value_t));

  arr.emplace_back(0);

  EXPECT_EQ(0, arr.back());
  EXPECT_EQ(1025, arr.size());
  EXPECT_EQ(2048, arr.capacity());
}

constexpr size_t kStrSz = sizeof(std::string);
constexpr size_t kPmrStrSz = sizeof(std::pmr::string);

}  // namespace base

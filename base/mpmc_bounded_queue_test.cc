// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/mpmc_bounded_queue.h"

#include <memory>
#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace base {

class MPMCTest : public testing::Test {};

struct A {
  static int ref;

  A() { ++ref; }
  A(const A&) { ++ref; }
  A(A&&) { ++ref; }

  ~A() { --ref; }
};

int A::ref = 0;

TEST_F(MPMCTest, Enqueue) {
  mpmc_bounded_queue<int> q(2);
  ASSERT_TRUE(q.try_enqueue(5));
  const int val = 6;
  ASSERT_TRUE(q.try_enqueue(val));
  ASSERT_FALSE(q.try_enqueue(val));

  int tmp = 0;
  ASSERT_TRUE(q.try_dequeue(tmp));
  EXPECT_EQ(5, tmp);
  ASSERT_TRUE(q.try_dequeue(tmp));
  EXPECT_EQ(6, tmp);
  ASSERT_FALSE(q.try_dequeue(tmp));

  mpmc_bounded_queue<std::shared_ptr<int>> sh_q(2);
  int* const ptr = new int(5);
  ASSERT_TRUE(sh_q.try_enqueue(ptr));

  auto ptr2 = std::make_unique<int>(3);
  ASSERT_TRUE(sh_q.try_enqueue(std::move(ptr2)));
  ASSERT_FALSE(ptr2);
  ptr2 = std::make_unique<int>(3);

  ASSERT_FALSE(sh_q.try_enqueue(std::move(ptr2)));
  ASSERT_TRUE(ptr2);
}

TEST_F(MPMCTest, Dtor) {
  for (unsigned i = 1; i <= 8; ++i) {
    mpmc_bounded_queue<A> tst(8);
    EXPECT_EQ(0, A::ref);

    for (unsigned j = 0; j < i; ++j) {
      EXPECT_TRUE(tst.try_enqueue(A{}));
      EXPECT_EQ(j + 1, A::ref);
    }
  }
}

}  // namespace base

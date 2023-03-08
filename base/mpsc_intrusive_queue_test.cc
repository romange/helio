// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "base/mpsc_intrusive_queue.h"

#include <atomic>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace base {

struct TestNode {
  std::atomic<TestNode*> next{nullptr};
};

void MPSC_intrusive_store_next(TestNode* dest, TestNode* next_node) {
  dest->next.store(next_node, std::memory_order_relaxed);
}

TestNode* MPSC_intrusive_load_next(const TestNode& src) {
  return src.next.load(std::memory_order_acquire);
}

class MPSCTest : public testing::Test {
 protected:
  MPSCIntrusiveQueue<TestNode> q_;
};

TEST_F(MPSCTest, Basic) {
  EXPECT_TRUE(q_.Pop() == nullptr);
  TestNode a, b, c;
  q_.Push(&a);
  q_.Push(&b);
  q_.Push(&c);

  TestNode* res = q_.Pop();
  EXPECT_EQ(&a, res);

  res = q_.Pop();
  EXPECT_EQ(&b, res);

  res = q_.Pop();
  EXPECT_EQ(&c, res);

  EXPECT_TRUE(q_.Pop() == nullptr);
}

}  // namespace base
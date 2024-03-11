// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "base/mpsc_intrusive_queue.h"

#include <absl/cleanup/cleanup.h>

#include <atomic>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/spinlock.h"

using namespace std;

namespace base {

struct TestNode;
TestNode* const kEmpty = (TestNode*)1;

struct TestNode {
  atomic<TestNode*> next{kEmpty};
};

void MPSC_intrusive_store_next(TestNode* dest, TestNode* next_node) {
  dest->next.store(next_node, memory_order_relaxed);
}

TestNode* MPSC_intrusive_load_next(const TestNode& src) {
  return src.next.load(memory_order_relaxed);
}

using Q = MPSCIntrusiveQueue<TestNode>;
class MPSCTest : public testing::Test {
 protected:
  Q q_;
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

struct State {
  SpinLock lock;
  unsigned seq = 0;

  bool consumer_finished = false;
};

void Consumer(unsigned limit, Q* q, State* state) {
  unsigned expected = 1;
  absl::Cleanup cleanup([&] { state->consumer_finished = true; });

  while (true) {
    unique_lock<SpinLock> lock(state->lock);
    if (state->seq < expected) {
      continue;
    }
    expected = state->seq + 1;
    lock.unlock();

    unsigned iteration = 0;
    while (true) {
      // expect to have a node in the queue.
      auto [node, isempty] = q->PopWeak();
      if (node) {
        node->next.store(kEmpty, memory_order_release);
        if (expected > limit)
          return;
        break;
      }
      ++iteration;

      ASSERT_FALSE(isempty) << state->seq << " " << node << " "
                            << iteration;
    }
  }
}

TEST_F(MPSCTest, ProducerConsumer) {
  State state;
  TestNode node;
  constexpr unsigned kLimit = 10000;
  thread consumer([&] { Consumer(kLimit, &q_, &state); });
  for (unsigned i = 0; i < kLimit && !state.consumer_finished; ++i) {
    unique_lock lock(state.lock);
    ++state.seq;
    q_.Push(&node);
    lock.unlock();

    while (node.next.load(memory_order_acquire) != kEmpty && !state.consumer_finished) {
      this_thread::yield();
    }
  }
  consumer.join();
}

}  // namespace base

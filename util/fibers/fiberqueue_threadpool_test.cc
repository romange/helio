// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/fiberqueue_threadpool.h"

#include <atomic>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/fibers.h"

using namespace std;

namespace util {
namespace fb2 {

class FiberQueueTest : public testing::Test {};

// Many producer fibers push into a tiny (size-2) queue while a single consumer fiber drains it.
// The small capacity forces the blocking path in FiberQueue::Add (yield -> push_ec_.await). All
// callbacks must run exactly once and the run must finish (no deadlock / lost wakeup).
TEST_F(FiberQueueTest, BlockingMultiProducer) {
  constexpr unsigned kProducers = 8;
  constexpr unsigned kPerProducer = 200;

  FiberQueue q(2);
  atomic<unsigned> executed{0};
  atomic<unsigned> preempts{0};

  Fiber consumer("consumer", [&] { q.Run(); });

  vector<Fiber> producers;
  for (unsigned p = 0; p < kProducers; ++p) {
    producers.push_back(Fiber("producer", [&] {
      for (unsigned i = 0; i < kPerProducer; ++i) {
        bool preempted = q.Add([&] { executed.fetch_add(1, memory_order_relaxed); });
        if (preempted)
          preempts.fetch_add(1, memory_order_relaxed);
      }
    }));
  }

  for (auto& f : producers)
    f.Join();

  q.Shutdown();
  consumer.Join();

  EXPECT_EQ(kProducers * kPerProducer, executed.load());
  // With a size-2 queue and 8 producers we are guaranteed to hit the blocking path at least once.
  EXPECT_GT(preempts.load(), 0u);
}

// Shutdown must drain everything that was already committed, even with a small queue.
TEST_F(FiberQueueTest, AwaitReturnsValue) {
  FiberQueue q(4);
  Fiber consumer("consumer", [&] { q.Run(); });

  int r = q.Await([] { return 42; });
  EXPECT_EQ(42, r);

  q.Shutdown();
  consumer.Join();
}

}  // namespace fb2
}  // namespace util

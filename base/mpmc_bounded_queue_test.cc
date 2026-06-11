// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/mpmc_bounded_queue.h"

#include <absl/strings/str_cat.h>

#include <memory>
#include <thread>
#include <vector>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"

ABSL_FLAG(uint32_t, producers, 1, "Number of producer threads for the benchmark");
ABSL_FLAG(uint32_t, queue_size, 1024, "Queue size, must be power of 2");

using namespace std;
using benchmark::DoNotOptimize;

namespace base {

class MPMCTest : public testing::Test {};

struct A {
  static int ref;

  A() {
    ++ref;
  }
  A(const A&) {
    ++ref;
  }
  A(A&&) {
    ++ref;
  }

  ~A() {
    --ref;
  }
};

struct Moveable {
  static int ref;

  Moveable() {
    ++ref;
  }

  ~Moveable() {
    --ref;
  }

  Moveable(const Moveable&) = delete;
  void operator=(const Moveable&) = delete;

  Moveable(Moveable&&) {
    ++ref;
  }

  Moveable& operator=(Moveable&&) = default;
};

int A::ref = 0;
int Moveable::ref = 0;

TEST_F(MPMCTest, Enqueue) {
  mpmc_bounded_queue<int> q(2);
  ASSERT_TRUE(q.try_enqueue(5));
  const int val = 6;
  ASSERT_FALSE(q.is_full());
  ASSERT_TRUE(q.try_enqueue(val));
  ASSERT_TRUE(q.is_full());
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

TEST_F(MPMCTest, Moveable) {
  mpmc_bounded_queue<Moveable> queue(16);

  EXPECT_TRUE(queue.try_enqueue(Moveable{}));
  EXPECT_EQ(1, Moveable::ref);
  {
    Moveable dest;
    EXPECT_EQ(2, Moveable::ref);

    EXPECT_TRUE(queue.try_dequeue(dest));
    EXPECT_EQ(1, Moveable::ref);
  }
  EXPECT_EQ(0, Moveable::ref);
}

TEST_F(MPMCTest, SingleConsumerDequeue) {
  mpmc_bounded_queue<int> q(2);

  ASSERT_TRUE(q.try_enqueue(1));
  ASSERT_TRUE(q.try_enqueue(2));

  int tmp = 0;
  ASSERT_TRUE(q.try_dequeue_sc(tmp));
  EXPECT_EQ(1, tmp);
  ASSERT_TRUE(q.try_enqueue(3));

  ASSERT_TRUE(q.try_dequeue_sc(tmp));
  EXPECT_EQ(2, tmp);
  ASSERT_TRUE(q.try_dequeue_sc(tmp));
  EXPECT_EQ(3, tmp);
  ASSERT_FALSE(q.try_dequeue_sc(tmp));
}

// To see hotspots:
// perf c2c record ./mpmc_bounded_queue_test --bench --producers=8
// perf c2c report --stdio
static void BM_MPMCContention(benchmark::State& state) {
  const unsigned num_producers = absl::GetFlag(FLAGS_producers);
  const size_t queue_size = absl::GetFlag(FLAGS_queue_size);
  const size_t kOpsPerProducer = 1 << 20;
  const size_t total_ops = num_producers * kOpsPerProducer;

  for (auto _ : state) {
    mpmc_bounded_queue<uint64_t> queue(queue_size);
    atomic<bool> start{false};
    atomic<uint64_t> enqueue_failures{0};

    thread consumer([&] {
      while (!start.load(memory_order_acquire)) {
      }

      size_t consumed = 0;
      uint64_t val;
      while (consumed < total_ops) {
        if (queue.try_dequeue_sc(val)) {
          DoNotOptimize(val);
          ++consumed;
        }
      }
    });

    vector<thread> producers;
    producers.reserve(num_producers);
    for (unsigned i = 0; i < num_producers; ++i) {
      producers.emplace_back([&] {
        while (!start.load(memory_order_acquire)) {
        }

        uint64_t fails = 0;
        for (size_t j = 0; j < kOpsPerProducer; ++j) {
          while (!queue.try_enqueue(j)) {
            ++fails;
          }
        }
        enqueue_failures.fetch_add(fails, memory_order_relaxed);
      });
    }

    start.store(true, memory_order_release);

    for (auto& t : producers)
      t.join();
    consumer.join();

    state.counters["enq_fail/op"] =
        double(enqueue_failures.load()) / total_ops;
  }

  state.SetItemsProcessed(state.iterations() * total_ops);
  state.SetLabel(absl::StrCat(num_producers, "P/1C q=", queue_size));
}
BENCHMARK(BM_MPMCContention)->MinTime(2.0)->UseRealTime();

}  // namespace base

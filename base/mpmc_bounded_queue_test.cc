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

// Fetch-and-add enqueue discipline: claim_slot / slot_ready / commit_slot.
TEST_F(MPMCTest, ClaimCommit) {
  mpmc_bounded_queue<int> q(4);

  size_t p0 = q.claim_slot();
  ASSERT_TRUE(q.slot_ready(p0));
  q.commit_slot(p0, 10);

  size_t p1 = q.claim_slot();
  ASSERT_TRUE(q.slot_ready(p1));
  q.commit_slot(p1, 20);

  int v = 0;
  ASSERT_TRUE(q.try_dequeue_sc(v));
  EXPECT_EQ(10, v);
  ASSERT_TRUE(q.try_dequeue_sc(v));
  EXPECT_EQ(20, v);
  ASSERT_FALSE(q.try_dequeue_sc(v));
}

// A claimed slot stays "not ready" while its cell still holds an item from the previous lap,
// and becomes ready as soon as that item is dequeued.
TEST_F(MPMCTest, ClaimSlotFullUntilDequeued) {
  mpmc_bounded_queue<int> q(2);  // capacity 2, cells {0, 1}

  size_t p0 = q.claim_slot();  // -> cell 0
  size_t p1 = q.claim_slot();  // -> cell 1
  ASSERT_TRUE(q.slot_ready(p0));
  ASSERT_TRUE(q.slot_ready(p1));
  q.commit_slot(p0, 1);
  q.commit_slot(p1, 2);

  // Third ticket wraps back to cell 0; not free until p0 is dequeued.
  size_t p2 = q.claim_slot();
  EXPECT_FALSE(q.slot_ready(p2));

  int v = 0;
  ASSERT_TRUE(q.try_dequeue_sc(v));
  EXPECT_EQ(1, v);
  EXPECT_TRUE(q.slot_ready(p2));  // dequeuing p0 freed cell 0 for p2
  q.commit_slot(p2, 3);

  ASSERT_TRUE(q.try_dequeue_sc(v));
  EXPECT_EQ(2, v);
  ASSERT_TRUE(q.try_dequeue_sc(v));
  EXPECT_EQ(3, v);
  ASSERT_FALSE(q.try_dequeue_sc(v));
}

TEST_F(MPMCTest, HasInflightSlots) {
  mpmc_bounded_queue<int> q(2);
  EXPECT_FALSE(q.has_inflight_slots());

  // A claimed-but-uncommitted slot is invisible to try_dequeue_sc, yet must register as inflight:
  // this is what stops a single-consumer shutdown from exiting before the producer commits.
  size_t pos = q.claim_slot();
  EXPECT_TRUE(q.has_inflight_slots());

  int v = 0;
  EXPECT_FALSE(q.try_dequeue_sc(v));  // gap is not yet visible as an item
  EXPECT_TRUE(q.has_inflight_slots());

  q.commit_slot(pos, 7);
  EXPECT_TRUE(q.has_inflight_slots());  // committed but not yet dequeued

  ASSERT_TRUE(q.try_dequeue_sc(v));
  EXPECT_EQ(7, v);
  EXPECT_FALSE(q.has_inflight_slots());  // drained: enqueue and dequeue positions converge
}

// FIFO preserved across many laps of the ring.
TEST_F(MPMCTest, ClaimCommitLaps) {
  mpmc_bounded_queue<int> q(4);
  int produced = 0, consumed = 0;
  for (int lap = 0; lap < 1000; ++lap) {
    for (int i = 0; i < 4; ++i) {
      size_t pos = q.claim_slot();
      ASSERT_TRUE(q.slot_ready(pos));
      q.commit_slot(pos, produced++);
    }
    for (int i = 0; i < 4; ++i) {
      int v = -1;
      ASSERT_TRUE(q.try_dequeue_sc(v));
      EXPECT_EQ(consumed++, v);
    }
  }
}

// commit_slot must construct exactly one T and try_dequeue must destroy it - no leaks.
TEST_F(MPMCTest, ClaimCommitMoveable) {
  {
    mpmc_bounded_queue<Moveable> q(4);
    size_t pos = q.claim_slot();
    ASSERT_TRUE(q.slot_ready(pos));
    q.commit_slot(pos, Moveable{});
    EXPECT_EQ(1, Moveable::ref);

    Moveable dest;
    ASSERT_TRUE(q.try_dequeue_sc(dest));
    EXPECT_EQ(1, Moveable::ref);
  }
  EXPECT_EQ(0, Moveable::ref);
}

// The CAS (try_enqueue) and FAA (claim/commit) enqueue disciplines may be mixed on the same queue.
// Half the producers use each; a single consumer drains. All items must arrive intact.
TEST_F(MPMCTest, MixedCasFaaStress) {
  constexpr unsigned kProducers = 8;
  constexpr size_t kOps = 50000;
  const size_t total = size_t(kProducers) * kOps;

  mpmc_bounded_queue<uint64_t> q(64);
  atomic<bool> start{false};
  atomic<uint64_t> sum_produced{0};
  uint64_t sum_consumed = 0;
  size_t consumed = 0;

  thread consumer([&] {
    while (!start.load(memory_order_acquire)) {
    }
    uint64_t v;
    while (consumed < total) {
      if (q.try_dequeue_sc(v)) {
        sum_consumed += v;
        ++consumed;
      }
    }
  });

  vector<thread> producers;
  producers.reserve(kProducers);
  for (unsigned p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      while (!start.load(memory_order_acquire)) {
      }
      const bool use_faa = (p % 2 == 0);
      uint64_t local_sum = 0;
      for (size_t j = 0; j < kOps; ++j) {
        uint64_t val = (uint64_t(p) << 40) | j;
        local_sum += val;
        if (use_faa) {
          size_t pos = q.claim_slot();
          while (!q.slot_ready(pos)) {
          }
          q.commit_slot(pos, val);
        } else {
          while (!q.try_enqueue(val)) {
          }
        }
      }
      sum_produced.fetch_add(local_sum, memory_order_relaxed);
    });
  }

  start.store(true, memory_order_release);
  for (auto& t : producers)
    t.join();
  consumer.join();

  EXPECT_EQ(total, consumed);
  EXPECT_EQ(sum_produced.load(), sum_consumed);
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

    state.counters["enq_fail/op"] = double(enqueue_failures.load()) / total_ops;
  }

  state.SetItemsProcessed(state.iterations() * total_ops);
  state.SetLabel(absl::StrCat(num_producers, "P/1C q=", queue_size, " CAS"));
}
BENCHMARK(BM_MPMCContention)->MinTime(2.0)->UseRealTime();

// Same workload as BM_MPMCContention but using the fetch-and-add enqueue discipline (claim_slot +
// spin on slot_ready + commit_slot) instead of the CAS try_enqueue loop. Compares producer-side
// contention on enqueue_pos_ between the two approaches ("with and without CAS").
static void BM_MPMCContentionFAA(benchmark::State& state) {
  const unsigned num_producers = absl::GetFlag(FLAGS_producers);
  const size_t queue_size = absl::GetFlag(FLAGS_queue_size);
  const size_t kOpsPerProducer = 1 << 20;
  const size_t total_ops = num_producers * kOpsPerProducer;

  for (auto _ : state) {
    mpmc_bounded_queue<uint64_t> queue(queue_size);
    atomic<bool> start{false};
    atomic<uint64_t> enqueue_spins{0};

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

        uint64_t spins = 0;
        for (size_t j = 0; j < kOpsPerProducer; ++j) {
          size_t pos = queue.claim_slot();
          while (!queue.slot_ready(pos)) {
            ++spins;
          }
          queue.commit_slot(pos, j);
        }
        enqueue_spins.fetch_add(spins, memory_order_relaxed);
      });
    }

    start.store(true, memory_order_release);

    for (auto& t : producers)
      t.join();
    consumer.join();

    state.counters["enq_spin/op"] = double(enqueue_spins.load()) / total_ops;
  }

  state.SetItemsProcessed(state.iterations() * total_ops);
  state.SetLabel(absl::StrCat(num_producers, "P/1C q=", queue_size, " FAA"));
}
BENCHMARK(BM_MPMCContentionFAA)->MinTime(2.0)->UseRealTime();

}  // namespace base

// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/spinlock.h"

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "base/gtest.h"

namespace base {
namespace {

// Clang cannot track acquisitions through standard guards or GoogleTest assertions.
TEST(RWSpinLockTest, SharedAndExclusiveExclusion) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  RWSpinLock lock;
  {
    std::shared_lock first(lock);
    EXPECT_EQ(lock.ReaderCount(), 1);
    EXPECT_FALSE(lock.IsLockedExclusive());
    EXPECT_TRUE(lock.try_lock_shared());
    EXPECT_EQ(lock.ReaderCount(), 2);
    EXPECT_FALSE(lock.try_lock());
    lock.unlock_shared();
    EXPECT_EQ(lock.ReaderCount(), 1);
  }

  ASSERT_TRUE(lock.try_lock());
  EXPECT_TRUE(lock.IsLockedExclusive());
  EXPECT_EQ(lock.ReaderCount(), 0);
  EXPECT_FALSE(lock.try_lock_shared());
  EXPECT_FALSE(lock.try_lock());
  lock.unlock();

  ASSERT_TRUE(lock.try_lock_shared());
  lock.unlock_shared();
}

// Probing reader admission while retaining a shared lock is intentional.
TEST(RWSpinLockTest, WaitingWriterBlocksNewReaders) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  RWSpinLock lock;
  int value = 0;
  lock.lock_shared();
  std::thread writer([&] {
    std::unique_lock guard(lock);
    value = 42;
  });

  // Keep the original reader until a writer announces itself, without relying on scheduling delays.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  bool writer_waiting = false;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!lock.try_lock_shared()) {
      writer_waiting = true;
      break;
    }
    lock.unlock_shared();
    std::this_thread::yield();
  }
  EXPECT_TRUE(writer_waiting);
  EXPECT_FALSE(lock.try_lock());
  std::thread reader([&] {
    std::shared_lock guard(lock);
    EXPECT_EQ(value, 42);
  });

  lock.unlock_shared();
  writer.join();
  reader.join();
}

TEST(RWSpinLockTest, ConcurrentReadersAndWriters) {
  RWSpinLock lock;
  constexpr unsigned kWriters = 4;
  constexpr unsigned kIterations = 1000;
  uint64_t value = 0;
  uint64_t complement = ~value;
  std::atomic<bool> start{false};
  std::vector<std::thread> threads;
  for (unsigned i = 0; i < kWriters; ++i) {
    threads.emplace_back([&] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (unsigned j = 0; j < kIterations; ++j) {
        std::unique_lock guard(lock);
        complement = ~++value;
      }
    });
    threads.emplace_back([&] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (unsigned j = 0; j < kIterations; ++j) {
        std::shared_lock guard(lock);
        EXPECT_EQ(complement, ~value);
      }
    });
  }

  start.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(value, kWriters * kIterations);
}

}  // namespace
}  // namespace base

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>
#include <absl/base/optimization.h>
#include <sched.h>

#include <atomic>
#include <cassert>
#include <cstdint>

#if defined(__x86_64__) || defined(__i386__)
#include <emmintrin.h>
#endif

// Non-yielding, nonrecursive locks. Holders must not cooperatively suspend as a fiber suspending
// with a spinlock held will cause a livelock for other fibers attempting to acquire the lock.
//
// Choosing a lock:
// - SpinLock: Abseil's spinlock. Under contention it eventually sleeps on a futex, which blocks
//   the waiter's OS thread and every fiber multiplexed on it. Prefer it where contention is rare.
// - TTASSpinLock: exclusive-only, 4 bytes. Waiters spin on plain loads and never sleep on a
//   futex. Prefer it for short critical sections on data that many threads contend on.
// - RWSpinLock: shared/exclusive, writer-preferring, 8 bytes. Never sleeps or yields the CPU.
// base/RWSpinLock.h also provides folly::RWSpinLock, which retries the CAS on every spin.

namespace base {

namespace detail {

inline void CpuPause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#elif defined(__aarch64__)
  asm volatile("yield");
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

}  // namespace detail

class ABSL_LOCKABLE SpinLock {
 public:
  SpinLock() = default;
  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    lock_.Lock();
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    lock_.Unlock();
  }

  bool try_lock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    return lock_.TryLock();
  }

 private:
  absl::base_internal::SpinLock lock_;
};

// Exclusive-only test-and-test-and-set spinlock in 4 bytes. The acquisition is not fair.
//
// A waiter that fails the initial CAS spins on relaxed loads with PAUSE until the lock word reads
// free, and only then retries the CAS. Load-only spinning keeps the cache line shared among the
// waiters, so they do not keep pulling it away from the holder with read-modify-write operations,
// and the holder's unlock store does not queue behind them.
//
// How it differs from the other spinlocks under base/:
// - SpinLock eventually sleeps on a futex. TTASSpinLock never sleeps on a futex.
// - RWSpinLock::lock() registers each contended writer with a fetch_add on the lock word, which is
//   one more RMW on the contended line, and it never yields the CPU. TTASSpinLock has no waiter
//   count and no shared mode.
//
class ABSL_LOCKABLE TTASSpinLock {
 public:
  TTASSpinLock() = default;
  TTASSpinLock(const TTASSpinLock&) = delete;
  TTASSpinLock& operator=(const TTASSpinLock&) = delete;

  void lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    if (ABSL_PREDICT_FALSE(!try_lock())) {
      LockSlow();
    }
  }

  void unlock() noexcept ABSL_UNLOCK_FUNCTION() {
    word_.store(0, std::memory_order_release);
  }

  bool try_lock() noexcept ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    uint32_t expected = 0;
    return word_.compare_exchange_strong(expected, 1, std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }

 private:
  void LockSlow() noexcept ABSL_NO_THREAD_SAFETY_ANALYSIS {
    unsigned spins = 0;
    do {
      while (word_.load(std::memory_order_relaxed) != 0) {
        detail::CpuPause();
        if (++spins > 2000) {
          // This is relevant only for oversubscribed setups where the thread holding the lock
          // is preempted and we need to yield to give it a chance to run. Never happens on
          // proper thread-per-core setups but does not hurt to have here.
          sched_yield();
          spins = 0;
        }
      }
    } while (!try_lock());
  }

  std::atomic<uint32_t> word_{0};
};

static_assert(sizeof(TTASSpinLock) == 4);

// Non-yielding, nonrecursive reader-writer spinlock.
// Waiting writers block new readers, but acquisition is not FIFO:
// sustained writers can delay readers.
class ABSL_LOCKABLE RWSpinLock {
 public:
  RWSpinLock() = default;
  RWSpinLock(const RWSpinLock&) = delete;
  RWSpinLock& operator=(const RWSpinLock&) = delete;

  void lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    if (try_lock()) {
      return;
    }
    state_.fetch_add(kWaitingWriterStep, std::memory_order_relaxed);
    while (true) {
      uint64_t s = state_.load(std::memory_order_relaxed);
      if ((s & (kWriterBit | kReadersMask)) == 0) {
        assert((s & kWaitingWritersMask) >= kWaitingWriterStep);
        uint64_t desired = (s - kWaitingWriterStep) | kWriterBit;
        if (state_.compare_exchange_weak(s, desired, std::memory_order_acquire,
                                         std::memory_order_relaxed)) {
          return;
        }
      }
      detail::CpuPause();
    }
  }

  void unlock() noexcept ABSL_UNLOCK_FUNCTION() {
    uint64_t prev = state_.fetch_and(~kWriterBit, std::memory_order_release);
    assert((prev & kWriterBit) != 0);
    (void)prev;
  }

  bool try_lock() noexcept ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    uint64_t expected = 0;
    return state_.compare_exchange_strong(expected, kWriterBit, std::memory_order_acquire,
                                          std::memory_order_relaxed);
  }

  void lock_shared() noexcept ABSL_SHARED_LOCK_FUNCTION() {
    while (!try_lock_shared()) {
      detail::CpuPause();
    }
  }

  void unlock_shared() noexcept ABSL_UNLOCK_FUNCTION() {
    uint64_t prev = state_.fetch_sub(kReaderStep, std::memory_order_release);
    assert((prev >> 32) >= 1);
    (void)prev;
  }

  bool try_lock_shared() noexcept ABSL_SHARED_TRYLOCK_FUNCTION(true) {
    uint64_t s = state_.load(std::memory_order_relaxed);
    while ((s & (kWriterBit | kWaitingWritersMask)) == 0) {
      if (state_.compare_exchange_weak(s, s + kReaderStep, std::memory_order_acquire,
                                       std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  // Diagnostic snapshots only; neither query establishes synchronization or proves ownership.
  bool IsLockedExclusive() const noexcept {
    return (state_.load(std::memory_order_relaxed) & kWriterBit) != 0;
  }

  uint32_t ReaderCount() const noexcept {
    return static_cast<uint32_t>(state_.load(std::memory_order_relaxed) >> 32);
  }

 private:
  // Bit 0: exclusive owner; bits 1..31: waiting writers; bits 32..63: active readers.
  static constexpr uint64_t kWriterBit = 1ULL;
  static constexpr uint64_t kWaitingWriterStep = 2ULL;
  static constexpr uint64_t kWaitingWritersMask = ((1ULL << 31) - 1) << 1;
  static constexpr uint64_t kReaderStep = 1ULL << 32;
  static constexpr uint64_t kReadersMask = ~((1ULL << 32) - 1);

  std::atomic<uint64_t> state_{0};
};

}  // namespace base

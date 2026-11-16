// Copyright 2026, Roman Gershman. All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <absl/base/optimization.h>
#include <absl/base/thread_annotations.h>

#include <atomic>
#include <cstdint>

#include "util/fibers/proactor_base.h"

namespace util {
namespace fb2 {

// Nonrecursive, non-yielding test-and-test-and-set lock. Waiters on a proactor thread process
// bounded I/O work every 32 spin rounds, using CPU pause hints between attempts. Other threads
// only use CPU pause hints. Holders and completion callbacks must never suspend.
// Acquire with no other spinlock (or other lock needed by a callback) held and no unprepared
// SubmitEntry: completion callbacks run inline on the acquiring fiber's stack, and queued SQEs
// may be submitted. Acquisition is not fair.
class ABSL_LOCKABLE TTASSpinLock {
 public:
  TTASSpinLock() = default;
  TTASSpinLock(const TTASSpinLock&) = delete;
  TTASSpinLock& operator=(const TTASSpinLock&) = delete;

  void lock() noexcept ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    if (ABSL_PREDICT_FALSE(!try_lock()))
      LockSlow();
  }

  bool try_lock() noexcept ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    uint32_t expected = 0;
    return word_.compare_exchange_strong(expected, 1, std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }

  void unlock() noexcept ABSL_UNLOCK_FUNCTION() {
    word_.store(0, std::memory_order_release);
  }

 private:
  void LockSlow() noexcept ABSL_NO_THREAD_SAFETY_ANALYSIS {
    // Acquisition cannot switch fibers, so the cached proactor remains valid for this wait.
    auto* proactor = ProactorBase::me();
    unsigned rounds = 0;
    do {
      while (word_.load(std::memory_order_relaxed) != 0) {
        if (++rounds == 32) {
          rounds = 0;
          if (proactor && proactor->RunBoundedWork())
            continue;
        }
        ProactorBase::Pause();
      }
    } while (!try_lock());
  }

  std::atomic<uint32_t> word_{0};
};

static_assert(sizeof(TTASSpinLock) == 4);

}  // namespace fb2
}  // namespace util

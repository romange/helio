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

// IoSpinLock is a nonrecursive, non-yielding spinlock that uses the test-and-test-and-set (TTAS)
// algorithm, the same one as base::TTASSpinLock. The difference is how waiters spin: a waiter on
// a proactor thread runs bounded I/O work (ProactorBase::RunBoundedWork) every 32 spin rounds, so
// completions owned by the waiting thread keep progressing while it waits. Waiters on other
// threads only use CPU pause hints. Acquisition is not fair.
//
// Restrictions, because completion callbacks run inline on the waiting fiber's stack:
// - Holders and completion callbacks must never suspend.
// - Do not acquire it in the dispatcher fiber, including tasklets and completion callbacks that
//   run there. Debug builds DCHECK; release builds spin without running I/O work.
// - Do not acquire it in a FiberSwitchHook. On SUSPEND the suspending fiber is still active, so a
//   completion that wakes it would be dropped and the fiber would never resume.
// - Do not hold another lock that a completion callback may need.
// - Do not hold an unprepared SubmitEntry, since queued SQEs may be submitted.
class ABSL_LOCKABLE IoSpinLock {
 public:
  IoSpinLock() = default;
  IoSpinLock(const IoSpinLock&) = delete;
  IoSpinLock& operator=(const IoSpinLock&) = delete;

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

static_assert(sizeof(IoSpinLock) == 4);

}  // namespace fb2
}  // namespace util

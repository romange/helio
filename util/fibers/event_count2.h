// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "absl/base/internal/spinlock.h"
#include "util/fibers/detail/scheduler.h"

namespace util {
namespace fb2 {

// This class is all about reducing the contention on the producer side (notifications).
// We want notifications to be as light as possible, while waits are less important
// since they on the path of being suspended anyway. However, we also want to reduce number of
// spurious waits on the consumer side.
// This class has another wonderful property: notification thread does not need to lock mutex,
// which means it can be used from the io_context (ring0) fiber.
class EventCount {
 public:
  EventCount() noexcept : val_(0) {
  }

  using cv_status = std::cv_status;

  class Key {
    friend class EventCount;
    EventCount* me_;
    uint32_t epoch_;

    explicit Key(EventCount* me, uint32_t e) noexcept : me_(me), epoch_(e) {
    }

    Key(const Key&) = delete;

   public:
    Key(Key&&) noexcept = default;

    ~Key() {
      me_->val_.fetch_sub(kAddWaiter, std::memory_order_relaxed);
    }

    uint32_t epoch() const {
      return epoch_;
    }
  };

  // Return true if a notification was made, false if no notification was issued.
  bool notify() noexcept;

  bool notifyAll() noexcept;

  /**
   * Wait for condition() to become true.  Will clean up appropriately if
   * condition() throws. Returns true if had to preempt using wait_queue.
   */
  template <typename Condition> bool await(Condition condition);
  template <typename Condition>
  cv_status await_until(Condition condition, const std::chrono::steady_clock::time_point& tp);

  // Advanced API, most use-cases will requie await function.
  Key prepareWait() noexcept {
    uint64_t prev = val_.fetch_add(kAddWaiter, std::memory_order_acq_rel);
    return Key(this, prev >> kEpochShift);
  }

  void wait(uint32_t epoch) noexcept;

  cv_status wait_until(uint32_t epoch, const std::chrono::steady_clock::time_point& tp) noexcept;

 private:
  friend class Key;

  EventCount(const EventCount&) = delete;
  EventCount(EventCount&&) = delete;
  EventCount& operator=(const EventCount&) = delete;
  EventCount& operator=(EventCount&&) = delete;

  // This requires 64-bit
  static_assert(sizeof(uint32_t) == 4);
  static_assert(sizeof(uint64_t) == 8);

  // val_ stores the epoch in the most significant 32 bits and the
  // waiter count in the least significant 32 bits.
  std::atomic_uint64_t val_;

  using SpinLockType = ::absl::base_internal::SpinLock;
  using SpinLockHolder = ::absl::base_internal::SpinLockHolder;

  using FI_Queue = boost::intrusive::slist<
      detail::FiberInterface,
      boost::intrusive::member_hook<detail::FiberInterface, detail::FI_ListHook,
                                    &detail::FiberInterface::list_hook>,
      boost::intrusive::constant_time_size<false>, boost::intrusive::cache_last<true>>;

  SpinLockType lock_;
  FI_Queue wait_queue_;

  static constexpr uint64_t kAddWaiter = 1ULL;
  static constexpr size_t kEpochShift = 32;
  static constexpr uint64_t kAddEpoch = 1ULL << kEpochShift;
  static constexpr uint64_t kWaiterMask = kAddEpoch - 1;
};

inline bool EventCount::notify() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);

  if (prev & kWaiterMask) {
    detail::FiberInterface* active = detail::FiberActive();
    SpinLockHolder holder(&lock_);

    if (!wait_queue_.empty()) {
      detail::FiberInterface* fi = &wait_queue_.front();
      wait_queue_.pop_front();
      active->ActivateOther(fi);
      return true;
    }
  }
  return false;
}

// Atomically checks for epoch and waits on cond_var.
inline void EventCount::wait(uint32_t epoch) noexcept {
  detail::FiberInterface* active = detail::FiberActive();

  lock_.Lock();
  if ((val_.load(std::memory_order_relaxed) >> kEpochShift) == epoch) {
    wait_queue_.push_back(*active);
    lock_.Unlock();
    active->scheduler()->Preempt();
  } else {
    lock_.Unlock();
  }
}

// Returns true if had to preempt, false if no preemption happenned.
template <typename Condition> bool EventCount::await(Condition condition) {
  if (condition())
    return false;  // fast path

  // condition() is the only thing that may throw, everything else is
  // noexcept, Key destructor makes sure to cancelWait state when exiting the function.
  bool preempt = false;
  while (true) {
    Key key = prepareWait();  // Key destructor restores back the sequence counter.
    if (condition()) {
      break;
    }
    preempt = true;
    wait(key.epoch());
  }

  return preempt;
}

}  // namespace fb2
}  // namespace util

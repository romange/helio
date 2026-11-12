// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>  // for cv_status
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>

#include "base/spinlock.h"
#include "util/fibers/detail/wait_queue.h"

namespace util {
namespace fb2 {

// This class is all about reducing the contention on the producer side (notifications).
// We want notifications to be as light as possible, while waits are less important
// since they on the path of being suspended anyway. However, we also want to reduce number of
// spurious waits on the consumer side.
// This class has another wonderful property: notification thread does not need to lock mutex,
// which means it can be used from the io_context (ring0) fiber.
//
// Supports two subscription modes:
// - One-shot (default): Waiter unlinked after first notification. Use when waiting for a
//   single event or condition to become true once (see await(), check_or_subscribe()).
// - Persistent: Waiter remains linked across multiple notifications. Use when you need to
//   receive all future notifications until explicit unsubscription (see subscribe_persistent()).
//   Useful for long-lived monitoring or repeated event handling.
class EventCount {
 public:
  EventCount() noexcept : val_(0) {
  }

  using cv_status = std::cv_status;

  // Stores epoch and decrements waiter count when dropped
  class Key {
    friend class EventCount;
    EventCount* me_;
    uint32_t epoch_;

    explicit Key(EventCount* me, uint32_t e) noexcept : me_(me), epoch_(e) {
    }

    Key(const Key&) = delete;

   public:
    Key(Key&& o) noexcept : me_{o.me_}, epoch_{o.epoch_} {
      o.me_ = nullptr;
    };

    ~Key() {
      if (me_ != nullptr)
        me_->val_.fetch_sub(kAddWaiter, std::memory_order_relaxed);
    }

    uint32_t epoch() const {
      return epoch_;
    }
  };

  // RAII guard for waiter subscription. Automatically unlinks when destroyed.
  // For persistent waiters (waiter_->is_persistent() is true): also resets the waiter's persistent
  // flag on destruction so the waiter can be reused for a different subscription mode.
  class SubKey : public Key {
    friend class EventCount;
    detail::Waiter* waiter_;

    SubKey(Key&& key, detail::Waiter* w) noexcept : Key{std::move(key)}, waiter_{w} {
    }

    SubKey(const SubKey&) = delete;

   public:
    SubKey(SubKey&& other) noexcept = default;

    ~SubKey() {
      if (me_ != nullptr) {
        me_->unsubscribe(waiter_);
        waiter_->set_persistent(false);
      }
    }

    // Silently drop this subkey if the waiter is already unlinked.
    void Drop() {
      // Reading IsLinked is not thread safe if it really is linked, but the caller guarantees it
      assert(!waiter_->IsLinked());
      me_ = nullptr;
    }
  };

  // Notify one waiter, return if any was notified
  bool notify() noexcept {
    return NotifyInternal(&detail::WaitQueue::NotifyOne);
  }

  // Notify all waiters, return if any were notified
  bool notifyAll() noexcept {
    return NotifyInternal(&detail::WaitQueue::NotifyAll);
  }

  /**
   * Wait for condition() to become true.  Will clean up appropriately if
   * condition() throws. Returns true if had to preempt using wait_queue.
   */
  template <typename Condition> bool await(Condition condition);

  // Wait with an already-owned lock. Evaluates condition while locked; prepares the event wait
  // before unlocking, then reacquires the lock before rechecking. All changes to the condition's
  // state must use the same mutex and notify this EventCount. Returns with the lock held, also
  // if condition throws. A satisfied condition does not unlock or register a waiter.
  // Returns true if the event wait suspended (not counting suspension while reacquiring the lock).
  // The caller must hold no other lock or fiber-atomic scope that prohibits suspension.
  template <typename Mutex, typename Condition>
  bool await(std::unique_lock<Mutex>& lock, Condition condition);

  template <typename Condition>
  cv_status await_until(Condition condition, const std::chrono::steady_clock::time_point& tp);

  // If condition() is false, subscribe with waiter to updates.
  // Waiter call does NOT guarantee truthiness of condition and just delivers notify() action.
  // Returns special SubKey that should be dropped before the waiter can be re-used.
  template <typename Condition>
  std::optional<SubKey> check_or_subscribe(Condition condition, detail::Waiter* w);

  // Subscribe a callback-based waiter persistently: both notify() and notifyAll()
  // fire its callback but do NOT unlink it. Returns a RAII guard that unlinks on
  // destruction.
  //
  // IMPORTANT: The callback must NOT call unsubscribe() or destroy SubKey, because
  // notification runs under EventCount's internal spinlock and re-entering it would
  // deadlock. The callback should only set a flag or perform a non-blocking signal.
  inline SubKey subscribe_persistent(detail::Waiter* w) noexcept;

  // Advanced API, most use-cases will require await function.
  Key prepareWait() noexcept {
    uint64_t prev = val_.fetch_add(kAddWaiter, std::memory_order_acq_rel);
    return Key(this, prev >> kEpochShift);
  }

  // The inline members are declared inline here too, so that the compiler reports a translation
  // unit that uses them without their definition (see event_count_test.cc).
  inline void finishWait() noexcept;

  bool wait(uint32_t epoch) noexcept;
  cv_status wait_until(uint32_t epoch, const std::chrono::steady_clock::time_point& tp) noexcept;

  inline bool subscribe(uint32_t epoch, detail::Waiter* w) noexcept;
  inline void unsubscribe(detail::Waiter* w) noexcept;

 private:
  friend class Key;

  EventCount(const EventCount&) = delete;
  EventCount(EventCount&&) = delete;
  EventCount& operator=(const EventCount&) = delete;
  EventCount& operator=(EventCount&&) = delete;

  // Run notify function on wait queue if any waiter is active
  bool NotifyInternal(bool (detail::WaitQueue::*f)(detail::FiberInterface*)) noexcept {
    uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);
    return (prev & kWaiterMask) && NotifyWaiters(f);
  }

  // Runs `f` on the wait queue under lock_. Defined in synchronization.cc, like wait(), because it
  // needs FiberInterface, whose header includes this one.
  bool NotifyWaiters(bool (detail::WaitQueue::*f)(detail::FiberInterface*)) noexcept;

  // val_ stores the epoch in the most significant 32 bits and the
  // waiter count in the least significant 32 bits.
  std::atomic_uint64_t val_;

  base::SpinLock lock_;  // protects wait_queue
  detail::WaitQueue wait_queue_;

  static constexpr uint64_t kAddWaiter = 1ULL;
  static constexpr size_t kEpochShift = 32;
  static constexpr uint64_t kAddEpoch = 1ULL << kEpochShift;
  static constexpr uint64_t kWaiterMask = kAddEpoch - 1;
};

// Defined here, not in synchronization.h, so that every translation unit that uses the inline and
// template members above also sees their definitions.
inline bool EventCount::subscribe(uint32_t epoch, detail::Waiter* w) noexcept {
  std::unique_lock lk(lock_);
  if ((val_.load(std::memory_order_relaxed) >> kEpochShift) == epoch) {
    wait_queue_.Link(w);
    return true;
  }
  return false;
}

inline void EventCount::unsubscribe(detail::Waiter* w) noexcept {
  std::unique_lock lk(lock_);
  if (w->IsLinked())
    wait_queue_.Unlink(w);
}

inline void EventCount::finishWait() noexcept {
  // We need this barrier to ensure that notify()/notifyAll() has finished before we return.
  // This is necessary because we want to avoid the case where continue to wait for
  // another eventcount/condition_variable and have two notify functions waking up the same
  // fiber at the same time.
  lock_.lock();
  lock_.unlock();
}

template <typename Condition> bool EventCount::await(Condition condition) {
  if (condition()) {
    std::atomic_thread_fence(std::memory_order_acquire);
    return false;  // fast path
  }

  // condition() is the only thing that may throw; Key cleans up the waiter state on exit.
  bool preempt = false;
  while (true) {
    Key key = prepareWait();
    if (condition()) {
      std::atomic_thread_fence(std::memory_order_acquire);
      break;
    }
    preempt |= wait(key.epoch());
  }

  return preempt;
}

template <typename Mutex, typename Condition>
bool EventCount::await(std::unique_lock<Mutex>& lock, Condition condition) {
  assert(lock.owns_lock());
  bool preempt = false;
  while (!condition()) {
    {
      // Drop the waiter registration before relocking, so notifiers do not see a waiter while
      // we block on the caller's mutex. The condition is rechecked under the lock anyway.
      Key key = prepareWait();
      lock.unlock();
      preempt |= wait(key.epoch());
    }
    lock.lock();
  }
  // No acquire fence needed: the condition is evaluated under the caller's mutex.
  return preempt;
}

template <typename Condition>
std::optional<EventCount::SubKey> EventCount::check_or_subscribe(Condition condition,
                                                                 detail::Waiter* w) {
  if (condition()) {
    std::atomic_thread_fence(std::memory_order_acquire);
    return {};
  }

  while (true) {
    auto key = prepareWait();
    if (condition()) {
      std::atomic_thread_fence(std::memory_order_acquire);
      break;
    }

    if (subscribe(key.epoch(), w))
      return SubKey{std::move(key), w};
  }
  return {};
}

inline EventCount::SubKey EventCount::subscribe_persistent(detail::Waiter* w) noexcept {
  assert(!w->IsLinked());
  w->set_persistent(true);
  Key key = prepareWait();
  std::unique_lock lk(lock_);

  // No epoch check needed here (unlike one-shot subscribe()). A notification firing in the
  // prepareWait()-to-Link() gap IS missed by this waiter, but that is safe because:
  // 1. Persistent waiters stay linked - the next notify/notifyAll will reach them.
  // 2. The caller MUST check its predicate after subscribing (standard EventCount contract),
  //    which catches any state change from the missed notification immediately.
  wait_queue_.Link(w);
  return SubKey{std::move(key), w};
}

template <typename Condition>
std::cv_status EventCount::await_until(Condition condition,
                                       const std::chrono::steady_clock::time_point& tp) {
  if (condition())
    return std::cv_status::no_timeout;  // fast path

  cv_status status = std::cv_status::no_timeout;
  while (true) {
    Key key = prepareWait();
    if (condition()) {
      break;
    }
    status = wait_until(key.epoch(), tp);
    if (status == std::cv_status::timeout)
      break;
  }
  return status;
}

// Use `BlockingCounter` unless certain that the counters lifetime is managed properly.
// Because the decrement of Dec() can be observed before notify is called, the counter can be still
// in use even after Wait() unblocked.
class EmbeddedBlockingCounter {
  const uint64_t kCancelFlag = (1ULL << 63);

  // Re-usable functor for wait condition, stores result in provided pointer
  auto WaitCondition(uint64_t* cnt) const {
    return [this, cnt]() -> bool {
      *cnt = count_.load(std::memory_order_relaxed);  // EventCount provides acquire
      return *cnt == 0 || (*cnt & kCancelFlag);
    };
  }

 public:
  EmbeddedBlockingCounter(unsigned start_count = 0) : ec_{}, count_{start_count} {
  }

  // Returns true on success (reaching 0), false when cancelled. Acquire semantics
  inline bool Wait();

  // Same as Wait(), but with timeout
  bool WaitFor(const std::chrono::steady_clock::duration& duration) {
    return WaitUntil(std::chrono::steady_clock::now() + duration);
  }

  bool WaitUntil(const std::chrono::steady_clock::time_point tp);

  // Start with specified count. Current value must be strictly zero (not cancelled).
  void Start(unsigned cnt);

  // Add to blocking counter
  void Add(unsigned cnt = 1) {
    count_.fetch_add(cnt, std::memory_order_relaxed);
  }

  // Decrement from blocking counter. Release semantics.
  void Dec();

  // Cancel blocking counter, unblock wait. Release semantics.
  void Cancel();

  // Notify waiter when completed. Return null if already completed (no registration happens).
  // Caller must hold the returned key to keep registered and drop it to re-use the waiter.
  std::optional<EventCount::SubKey> OnCompletion(detail::Waiter* w);

  // Return true if count is zero or cancelled. Has acquire semantics to be used in if checks
  bool IsCompleted() const;

  uint64_t DEBUG_Count() const;

 private:
  EventCount ec_;
  std::atomic<uint64_t> count_;
};

inline bool EmbeddedBlockingCounter::Wait() {
  uint64_t cnt;
  ec_.await(WaitCondition(&cnt));
  return (cnt & kCancelFlag) == 0;
}

}  // namespace fb2
}  // namespace util

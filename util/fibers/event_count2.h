// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <condition_variable>  // for cv_status

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

class Done {
  class Impl;

 public:
  enum DoneWaitDirective { AND_NOTHING = 0, AND_RESET = 1 };

  Done() : impl_(new Impl) {
  }
  ~Done() {
  }

  void Notify() {
    impl_->Notify();
  }
  bool Wait(DoneWaitDirective reset = AND_NOTHING) {
    return impl_->Wait(reset);
  }

  bool WaitFor(const std::chrono::steady_clock::duration& duration) {
    return impl_->WaitFor(duration);
  }

  void Reset() {
    impl_->Reset();
  }

 private:
  class Impl {
   public:
    Impl() : ready_(false) {
    }
    Impl(const Impl&) = delete;
    void operator=(const Impl&) = delete;

    friend void intrusive_ptr_add_ref(Impl* done) noexcept {
      done->use_count_.fetch_add(1, std::memory_order_relaxed);
    }

    friend void intrusive_ptr_release(Impl* impl) noexcept {
      if (1 == impl->use_count_.fetch_sub(1, std::memory_order_release)) {
        std::atomic_thread_fence(std::memory_order_acquire);
        delete impl;
      }
    }

    bool Wait(DoneWaitDirective reset) {
      bool res = ec_.await([this] { return ready_.load(std::memory_order_acquire); });
      if (reset == AND_RESET)
        ready_.store(false, std::memory_order_release);
      return res;
    }

    // Returns true if predicate became true, false if timeout reached.
    bool WaitFor(const std::chrono::steady_clock::duration& duration) {
      auto tp = std::chrono::steady_clock::now() + duration;
      std::cv_status status =
          ec_.await_until([this] { return ready_.load(std::memory_order_acquire); }, tp);
      return status == std::cv_status::no_timeout;
    }

    // We use EventCount to wake threads without blocking.
    void Notify() {
      ready_.store(true, std::memory_order_release);
      ec_.notify();
    }

    void Reset() {
      ready_ = false;
    }

    bool IsReady() const {
      return ready_.load(std::memory_order_acquire);
    }

   private:
    EventCount ec_;
    std::atomic<std::uint32_t> use_count_{0};
    std::atomic_bool ready_;
  };

  using ptr_t = ::boost::intrusive_ptr<Impl>;
  ptr_t impl_;
};

inline bool EventCount::notify() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);

  if (prev & kWaiterMask) {
    detail::FiberInterface* active = detail::FiberActive();
    lock_.Lock();

    if (!wait_queue_.empty()) {
      detail::FiberInterface* fi = &wait_queue_.front();
      wait_queue_.pop_front();
      lock_.Unlock();

      active->ActivateOther(fi);
      return true;
    }
    lock_.Unlock();
  }
  return false;
}


inline bool EventCount::notifyAll() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);

  if (prev & kWaiterMask) {
    FI_Queue tmp_queue;
    {
      SpinLockHolder holder(&lock_);
      tmp_queue.swap(wait_queue_);
    }
    detail::FiberInterface* active = detail::FiberActive();

    while (!tmp_queue.empty()) {
      detail::FiberInterface* fi = &tmp_queue.front();
      tmp_queue.pop_front();
      active->ActivateOther(fi);
    }
  }

  return false;
};

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


template <typename Condition>
std::cv_status EventCount::await_until(Condition condition,
                                       const std::chrono::steady_clock::time_point& tp) {
  if (condition())
    return std::cv_status::no_timeout;  // fast path

  cv_status status = std::cv_status::no_timeout;
  while (true) {
    Key key = prepareWait();  // Key destructor restores back the sequence counter.
    if (condition()) {
      break;
    }
    status = wait_until(key.epoch(), tp);
    if (status == std::cv_status::timeout)
      break;
  }
  return status;
}

}  // namespace fb2
}  // namespace util

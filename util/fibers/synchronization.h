// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <condition_variable>  // for cv_status

#include "base/spinlock.h"
#include "util/fibers/detail/scheduler.h"

namespace util {
namespace fb2 {

namespace detail {

using WaitQueue = boost::intrusive::slist<
    detail::FiberInterface,
    boost::intrusive::member_hook<detail::FiberInterface, detail::FI_ListHook,
                                  &detail::FiberInterface::wait_hook>,
    boost::intrusive::constant_time_size<false>, boost::intrusive::cache_last<true>>;

}  // namespace detail

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

  // return true if was suspended.
  bool wait(uint32_t epoch) noexcept;

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

  base::SpinLock lock_;
  detail::WaitQueue wait_queue_;

  static constexpr uint64_t kAddWaiter = 1ULL;
  static constexpr size_t kEpochShift = 32;
  static constexpr uint64_t kAddEpoch = 1ULL << kEpochShift;
  static constexpr uint64_t kWaiterMask = kAddEpoch - 1;
};

class CondVar;

class Mutex {
 private:
  friend class CondVar;

  base::SpinLock wait_queue_splk_;
  detail::WaitQueue wait_queue_;
  detail::FiberInterface* owner_{nullptr};

 public:
  Mutex() = default;

  ~Mutex() {
    assert(!owner_);
  }

  Mutex(Mutex const&) = delete;
  Mutex& operator=(Mutex const&) = delete;

  void lock();

  bool try_lock();

  void unlock();
};

class CondVarAny {
 private:
  base::SpinLock wait_queue_splk_;
  detail::WaitQueue wait_queue_;

 public:
  CondVarAny() = default;

  ~CondVarAny() {
    assert(wait_queue_.empty());
  }

  CondVarAny(CondVarAny const&) = delete;
  CondVarAny& operator=(CondVarAny const&) = delete;

  void notify_one() noexcept;

  void notify_all() noexcept;

  template <typename LockType> void wait(LockType& lt) {
    detail::FiberInterface* active = detail::FiberActive();

    // atomically call lt.unlock() and block on *this
    // store this fiber in waiting-queue
    wait_queue_splk_.lock();
    lt.unlock();
    wait_queue_.push_back(*active);
    wait_queue_splk_.unlock();
    active->scheduler()->Preempt();

    // relock external again before returning
    try {
      lt.lock();
    } catch (...) {
      std::terminate();
    }
  }

  template <typename LockType, typename Pred> void wait(LockType& lt, Pred pred) {
    while (!pred()) {
      wait(lt);
    }
  }

  template <typename LockType>
  std::cv_status wait_until(LockType& lt, std::chrono::steady_clock::time_point tp) {
    detail::FiberInterface* active = detail::FiberActive();
    std::cv_status status = std::cv_status::no_timeout;
    std::chrono::steady_clock::time_point timeout_time = tp;

    // atomically call lt.unlock() and block on *this
    // store this fiber in waiting-queue
    wait_queue_splk_.lock();
    lt.unlock();
    wait_queue_.push_back(*active);
    wait_queue_splk_.unlock();
    active->WaitUntil(tp);

    wait_queue_splk_.lock();
    if (active->wait_hook.is_linked()) {
      auto it = detail::WaitQueue::s_iterator_to(*active);
      wait_queue_.erase(it);
      status = std::cv_status::timeout;
    }
    wait_queue_splk_.unlock();

    lt.lock();
    return status;
  }

  template <typename LockType, typename Pred>
  bool wait_until(LockType& lt, std::chrono::steady_clock::time_point tp, Pred pred) {
    while (!pred()) {
      if (std::cv_status::timeout == wait_until(lt, tp)) {
        return pred();
      }
    }
    return true;
  }

  template <typename LockType>
  std::cv_status wait_for(LockType& lt, std::chrono::steady_clock::duration dur) {
    return wait_until(lt, std::chrono::steady_clock::now() + dur);
  }

  template <typename LockType, typename Pred>
  bool wait_for(LockType& lt, std::chrono::steady_clock::duration dur, Pred pred) {
    return wait_until(lt, std::chrono::steady_clock::now() + dur, pred);
  }
};

class CondVar {
 private:
  CondVarAny cnd_;

 public:
  CondVar() = default;

  CondVar(CondVar const&) = delete;
  CondVar& operator=(CondVar const&) = delete;

  void notify_one() noexcept {
    cnd_.notify_one();
  }

  void notify_all() noexcept {
    cnd_.notify_all();
  }

  void wait(std::unique_lock<Mutex>& lt) {
    // pre-condition
    cnd_.wait(lt);
  }

  template <typename Pred> void wait(std::unique_lock<Mutex>& lt, Pred pred) {
    cnd_.wait(lt, pred);
  }

  template <typename Clock, typename Duration>
  std::cv_status wait_until(std::unique_lock<Mutex>& lt,
                            std::chrono::time_point<Clock, Duration> const& timeout_time) {
    // pre-condition
    std::cv_status result = cnd_.wait_until(lt, timeout_time);
    // post-condition
    return result;
  }

  template <typename Clock, typename Duration, typename Pred>
  bool wait_until(std::unique_lock<Mutex>& lt,
                  std::chrono::time_point<Clock, Duration> const& timeout_time, Pred pred) {
    bool result = cnd_.wait_until(lt, timeout_time, pred);
    return result;
  }

  template <typename Rep, typename Period>
  std::cv_status wait_for(std::unique_lock<Mutex>& lt,
                          std::chrono::duration<Rep, Period> const& timeout_duration) {
    std::cv_status result = cnd_.wait_for(lt, timeout_duration);
    return result;
  }

  template <typename Rep, typename Period, typename Pred>
  bool wait_for(std::unique_lock<Mutex>& lt,
                std::chrono::duration<Rep, Period> const& timeout_duration, Pred pred) {
    bool result = cnd_.wait_for(lt, timeout_duration, pred);
    return result;
  }
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

// Callbacks must capture BlockingCounter object by value if they call Dec() function.
// The reason is that if a thread preempts right after count_.fetch_sub returns 1 but before
// ec_.notify was called, the object may be destroyed before ec_.notify is called.
class BlockingCounter {
  class Impl {
   public:
    Impl(unsigned count) : count_{count} {
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

    // I suspect all memory order accesses here could be "relaxed" but I do not bother.
    void Wait() {
      ec_.await([this] {
        auto cnt = count_.load(std::memory_order_acquire);
        return cnt == 0 || (cnt & (1ULL << 63));
      });
    }

    void Cancel() {
      count_.fetch_or(1ULL << 63, std::memory_order_acquire);
      ec_.notifyAll();
    }

    void Dec() {
      if (1 == count_.fetch_sub(1, std::memory_order_acq_rel))
        ec_.notifyAll();
    }

   private:
    friend class BlockingCounter;
    EventCount ec_;
    std::atomic<std::uint32_t> use_count_{0};
    std::atomic<uint64_t> count_;
  };
  using ptr_t = ::boost::intrusive_ptr<Impl>;

 public:
  explicit BlockingCounter(unsigned count) : impl_(new Impl(count)) {
  }

  void Inc() {
    Add(1);
  }

  void Dec() {
    impl_->Dec();
  }

  void Add(unsigned delta) {
    impl_->count_.fetch_add(delta, std::memory_order_acq_rel);
  }

  void Cancel() {
    impl_->Cancel();
  }

  void Wait() {
    impl_->Wait();
  }

 private:
  ptr_t impl_;
};

class SharedMutex {
 public:
  bool try_lock() {
    uint32_t expect = 0;
    return state_.compare_exchange_strong(expect, WRITER, std::memory_order_acq_rel);
  }

  void lock() {
    ec_.await([this] { return try_lock(); });
  }

  bool try_lock_shared() {
    uint32_t value = state_.fetch_add(READER, std::memory_order_acquire);
    if (value & WRITER) {
      state_.fetch_add(-READER, std::memory_order_release);
      return false;
    }
    return true;
  }

  void lock_shared() {
    ec_.await([this] { return try_lock_shared(); });
  }

  void unlock() {
    state_.fetch_and(~(WRITER), std::memory_order_relaxed);
    ec_.notifyAll();
  }

  void unlock_shared() {
    state_.fetch_add(-READER, std::memory_order_relaxed);
    ec_.notifyAll();
  }

 private:
  enum : int32_t { READER = 4, WRITER = 1 };
  EventCount ec_;
  std::atomic_uint32_t state_{0};
};

inline bool EventCount::notify() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);

  if (prev & kWaiterMask) {
    detail::FiberInterface* active = detail::FiberActive();
#if 1
    std::unique_lock lk(lock_);

    if (!wait_queue_.empty()) {
      detail::FiberInterface* fi = &wait_queue_.front();
      wait_queue_.pop_front();
      // At this point we know that fi exists still exists.
      // but if fi is remote and we cross unlock, it can timeout and destroy itself.
      // Therefore, we call ActivateOther under lock.
      active->ActivateOther(fi);
      lk.unlock();

      return true;
    }

#else
    detail::FiberInterface* dest = active->NotifyParked(uintptr_t(this));
    if (dest) {
      return true;
    }

#endif
  }
  return false;
}

inline bool EventCount::notifyAll() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);

  if (prev & kWaiterMask) {
    detail::FiberInterface* active = detail::FiberActive();
#if 1
    decltype(wait_queue_) tmp_queue;
    {
      std::unique_lock holder(lock_);
      tmp_queue.swap(wait_queue_);

      // fi may be remote and when we cross unlock, it can timeout and destroy itself.
      // therefore we call ActivateOther under lock.
      while (!tmp_queue.empty()) {
        detail::FiberInterface* fi = &tmp_queue.front();
        tmp_queue.pop_front();
        active->ActivateOther(fi);
      }
    }

#else
    active->NotifyAllParked(uintptr_t(this));
    return true;
#endif
  }

  return false;
};

// Atomically checks for epoch and waits on cond_var.
inline bool EventCount::wait(uint32_t epoch) noexcept {
  detail::FiberInterface* active = detail::FiberActive();

#if 1
  std::unique_lock lk(lock_);
  if ((val_.load(std::memory_order_relaxed) >> kEpochShift) == epoch) {
    wait_queue_.push_back(*active);
    lk.unlock();
    active->scheduler()->Preempt();
    return true;
  }
  return false;
#else
  return active->SuspendConditionally(uintptr_t(this), [&] {
    return (val_.load(std::memory_order_relaxed) >> kEpochShift) != epoch;
  });
#endif
}

inline std::cv_status EventCount::wait_until(
    uint32_t epoch, const std::chrono::steady_clock::time_point& tp) noexcept {
  detail::FiberInterface* active = detail::FiberActive();
  cv_status status = cv_status::no_timeout;

  std::unique_lock lk(lock_);
  if ((val_.load(std::memory_order_relaxed) >> kEpochShift) == epoch) {
    wait_queue_.push_back(*active);
    lk.unlock();

    active->WaitUntil(tp);

    lk.lock();
    if (active->wait_hook.is_linked()) {
      // We were woken up by timeout, lets remove ourselves from the queue.
      auto it = detail::WaitQueue::s_iterator_to(*active);
      wait_queue_.erase(it);
      status = cv_status::timeout;
    }
  }
  return status;
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
    preempt |= wait(key.epoch());
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

// For synchronizing fibers in single-threaded environment.
struct NoOpLock {
  void lock() {
  }
  void unlock() {
  }
};

class Barrier {
 public:
  explicit Barrier(std::size_t initial);

  Barrier(Barrier const&) = delete;
  Barrier& operator=(Barrier const&) = delete;

  bool Wait();
  void Cancel();

 private:
  std::size_t initial_;
  std::size_t current_;
  std::size_t cycle_{0};
  Mutex mtx_;
  CondVar cond_;
};

}  // namespace fb2

using fb2::BlockingCounter;

template <typename Pred> void Await(fb2::CondVarAny& cv, Pred&& pred) {
  fb2::NoOpLock lock;
  cv.wait(lock, std::forward<Pred>(pred));
}

}  // namespace util

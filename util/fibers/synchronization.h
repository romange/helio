// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <condition_variable>  // for cv_status
#include <optional>

#include "base/spinlock.h"
#include "util/fibers/detail/fiber_interface.h"

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

  // Automatically unlinks waiter when dropped
  class SubKey : public Key {
    friend class EventCount;
    detail::Waiter* waiter;

    SubKey(Key&& key, detail::Waiter* w) : Key{std::move(key)}, waiter{w} {
    }

   public:
    SubKey(SubKey&& other) noexcept = default;

    ~SubKey() {
      if (me_ != nullptr)
        me_->unsubscribe(waiter);
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
  template <typename Condition>
  cv_status await_until(Condition condition, const std::chrono::steady_clock::time_point& tp);

  // If condition() is false, subscribe with waiter to updates.
  // Waiter call does NOT guarantee truthiness of condition and just delivers notify() action.
  template <typename Condition>
  std::optional<SubKey> check_or_subscribe(Condition condition, detail::Waiter* w);

  // Advanced API, most use-cases will requie await function.
  Key prepareWait() noexcept {
    uint64_t prev = val_.fetch_add(kAddWaiter, std::memory_order_acq_rel);
    return Key(this, prev >> kEpochShift);
  }

  void finishWait() noexcept;

  bool wait(uint32_t epoch) noexcept;
  cv_status wait_until(uint32_t epoch, const std::chrono::steady_clock::time_point& tp) noexcept;

  bool subscribe(uint32_t epoch, detail::Waiter* w) noexcept;
  void unsubscribe(detail::Waiter* w) noexcept;
  
 private:
  friend class Key;

  EventCount(const EventCount&) = delete;
  EventCount(EventCount&&) = delete;
  EventCount& operator=(const EventCount&) = delete;
  EventCount& operator=(EventCount&&) = delete;

  // Run notify function on wait queue if any waiter is active
  bool NotifyInternal(bool (detail::WaitQueue::*f)(detail::FiberInterface*)) noexcept;

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

class CondVar;

// Replacement of std::LockGuard that allows -Wthread-safety
template <typename Mutex> class ABSL_SCOPED_LOCKABLE LockGuard {
 public:
  explicit LockGuard(Mutex& m) ABSL_EXCLUSIVE_LOCK_FUNCTION(m) : m_(m) {
    m_.lock();
  }

  ~LockGuard() ABSL_UNLOCK_FUNCTION() {
    m_.unlock();
  }

 private:
  Mutex& m_;
};

class ABSL_LOCKABLE Mutex {
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

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION();

  bool try_lock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true);

  void unlock() ABSL_UNLOCK_FUNCTION();
};

class CondVarAny {
  detail::WaitQueue wait_queue_;

  std::cv_status PostWaitTimeout(detail::Waiter waiter, bool clean_remote,
                                 detail::FiberInterface* active);

 public:
  CondVarAny() = default;

  ~CondVarAny() {
    assert(wait_queue_.empty());
  }

  CondVarAny(CondVarAny const&) = delete;
  CondVarAny& operator=(CondVarAny const&) = delete;

  // in contrast to std::condition_variable::notify_one() isn't thread-safe and should be called
  // under the mutex
  void notify_one() noexcept {
    if (!wait_queue_.empty())
      wait_queue_.NotifyOne(detail::FiberActive());
  }

  // in contrast to std::condition_variable::notify_all() isn't thread-safe and should be called
  // under the mutex
  void notify_all() noexcept {
    if (!wait_queue_.empty())
      wait_queue_.NotifyAll(detail::FiberActive());
  }

  template <typename LockType> void wait(LockType& lt);

  template <typename LockType, typename Pred> void wait(LockType& lt, Pred pred) {
    while (!pred()) {
      wait(lt);
    }
  }

  template <typename LockType>
  std::cv_status wait_until(LockType& lt, std::chrono::steady_clock::time_point tp);

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

  // in contrast to std::condition_variable::notify_one() isn't thread-safe and should be called
  // under the mutex
  void notify_one() noexcept {
    cnd_.notify_one();
  }

  // in contrast to std::condition_variable::notify_all() isn't thread-safe and should be called
  // under the mutex
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

  // Returns true if Done was notified, false if timeout reached.
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
  bool Wait();

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

  // Notify waiter when count reaches zero (including immediately if already zero).
  // Caller must hold the returned key while waiting and drop it after the waiter was notified.
  std::optional<EventCount::SubKey> OnCompletion(detail::Waiter* w);

  // Return true if count is zero or cancelled
  bool IsCompleted() const;

  uint64_t DEBUG_Count() const;

 private:
  EventCount ec_;
  std::atomic<uint64_t> count_;
};

// A barrier similar to Go's WaitGroup for tracking remote tasks.
// Internal smart pointer for easier lifetime management. Pass by value.
class BlockingCounter {
 public:
  BlockingCounter(unsigned start_count);

  EmbeddedBlockingCounter* operator->() {
    return counter_.get();
  }

 private:
  std::shared_ptr<EmbeddedBlockingCounter> counter_;
};

class ABSL_LOCKABLE SharedMutex {
 public:
  bool try_lock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    uint32_t expect = 0;
    return state_.compare_exchange_strong(expect, WRITER, std::memory_order_acq_rel);
  }

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    ec_.await([this] { return try_lock(); });
  }

  bool try_lock_shared() ABSL_SHARED_TRYLOCK_FUNCTION(true) {
    uint32_t value = state_.fetch_add(READER, std::memory_order_acquire);
    if (value & WRITER) {
      state_.fetch_add(-READER, std::memory_order_release);
      return false;
    }
    return true;
  }

  void lock_shared() ABSL_SHARED_LOCK_FUNCTION() {
    ec_.await([this] { return try_lock_shared(); });
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    state_.fetch_and(~(WRITER), std::memory_order_relaxed);
    ec_.notifyAll();
  }

  void unlock_shared() ABSL_UNLOCK_FUNCTION() {
    state_.fetch_add(-READER, std::memory_order_relaxed);
    ec_.notifyAll();
  }

 private:
  enum : int32_t { READER = 4, WRITER = 1 };
  EventCount ec_;
  std::atomic_uint32_t state_{0};
};

inline bool EventCount::NotifyInternal(
    bool (detail::WaitQueue::*f)(detail::FiberInterface*)) noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);
  if (prev & kWaiterMask) {
    detail::FiberInterface* active = detail::FiberActive();
    std::unique_lock lk(lock_);
    return (wait_queue_.*f)(active);
  }
  return false;
}

// Atomically checks for epoch and waits on cond_var.
inline bool EventCount::wait(uint32_t epoch) noexcept {
  detail::FiberInterface* active = detail::FiberActive();

  std::unique_lock lk(lock_);
  if ((val_.load(std::memory_order_relaxed) >> kEpochShift) == epoch) {
    detail::Waiter waiter(active->CreateWaiter());
    wait_queue_.Link(&waiter);
    lk.unlock();
    active->Suspend();

    finishWait();
    return true;
  }
  return false;
}

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

// Returns true if had to preempt, false if no preemption happenned.
template <typename Condition> bool EventCount::await(Condition condition) {
  if (condition()) {
    std::atomic_thread_fence(std::memory_order_acquire);
    return false;  // fast path
  }

  // condition() is the only thing that may throw, everything else is
  // noexcept, Key destructor makes sure to cancelWait state when exiting the function.
  bool preempt = false;
  while (true) {
    Key key = prepareWait();  // Key destructor restores back the sequence counter.
    if (condition()) {
      std::atomic_thread_fence(std::memory_order_acquire);
      break;
    }
    preempt |= wait(key.epoch());
  }

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
// Can be used together with `CondVarAny` to implement local-thread `CondVar` which is more
// efficient than EventCount.
struct NoOpLock {
  void lock() {
  }
  void unlock() {
  }

  bool try_lock() {
    return true;
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

template <typename LockType> void CondVarAny::wait(LockType& lt) {
  detail::FiberInterface* active = detail::FiberActive();

  detail::Waiter waiter(active->CreateWaiter());

  wait_queue_.Link(&waiter);
  lt.unlock();

  active->Suspend();

  // relock external again before returning
  try {
    lt.lock();
  } catch (...) {
    std::terminate();
  }
}

template <typename LockType>
std::cv_status CondVarAny::wait_until(LockType& lt, std::chrono::steady_clock::time_point tp) {
  detail::FiberInterface* active = detail::FiberActive();

  detail::Waiter waiter(active->CreateWaiter());

  // store this fiber in waiting-queue, we can do it without spinlocks because
  // lt is already locked.
  wait_queue_.Link(&waiter);

  // release the lock suspend this fiber until tp.
  lt.unlock();
  bool timed_out = active->WaitUntil(tp);

  // lock back.
  lt.lock();
  std::cv_status status = PostWaitTimeout(std::move(waiter), timed_out, active);
  assert(!waiter.IsLinked());
  return status;
}

inline bool EmbeddedBlockingCounter::Wait() {
  uint64_t cnt;
  ec_.await(WaitCondition(&cnt));
  return (cnt & kCancelFlag) == 0;
}

}  // namespace fb2

using fb2::BlockingCounter;

template <typename Pred> void Await(fb2::CondVarAny& cv, Pred&& pred) {
  fb2::NoOpLock lock;
  cv.wait(lock, std::forward<Pred>(pred));
}

}  // namespace util

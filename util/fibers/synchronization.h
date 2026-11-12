// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cassert>
#include <condition_variable>  // for cv_status
#include <memory>
#include <optional>
#include <utility>

#include "base/spinlock.h"
#include "util/fibers/detail/fiber_interface.h"
#include "util/fibers/event_count.h"

namespace util {
namespace fb2 {

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

 public:
  CondVarAny() = default;

  ~CondVarAny() {
    assert(wait_queue_.empty());
  }

  CondVarAny(CondVarAny const&) = delete;
  CondVarAny& operator=(CondVarAny const&) = delete;

  // Unlike std::condition_variable::notify_one(), is not thread-safe: hold the mutex if the waiters
  // may run on other threads.
  void notify_one() noexcept {
    if (!wait_queue_.empty())
      wait_queue_.NotifyOne(detail::FiberActive());
  }

  // Unlike std::condition_variable::notify_all(), is not thread-safe: hold the mutex if the waiters
  // may run on other threads.
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

  // Unlike std::condition_variable::notify_one(), is not thread-safe: hold the mutex if the waiters
  // may run on other threads.
  void notify_one() noexcept {
    cnd_.notify_one();
  }

  // Unlike std::condition_variable::notify_all(), is not thread-safe: hold the mutex if the waiters
  // may run on other threads.
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

// A barrier similar to Go's WaitGroup for tracking remote tasks.
// Internal smart pointer for easier lifetime management. Pass by value.
// Safe to use as a long-lived member (e.g. across separate Run()/Wait() calls) since each
// instance heap-owns its own counter. For a tight, single-call, scoped rendezvous
// (construct -> dispatch -> Wait(), never overlapping with another blocking-counter round on
// the same fiber) prefer FiberBlockingCounter below to avoid the heap allocation.
class BlockingCounter {
 public:
  explicit BlockingCounter(unsigned start_count);

  EmbeddedBlockingCounter* operator->() {
    return counter_.get();
  }

 private:
  std::shared_ptr<EmbeddedBlockingCounter> counter_;
};

// Same semantics as BlockingCounter, but backed by the constructing fiber's embedded
// EmbeddedBlockingCounter (see FiberInterface::blocking_counter()) instead of a heap
// allocation. Pass by value: copies share the same underlying counter and keep the owning
// fiber's FiberInterface (and thus the counter) alive via intrusive_ptr refcounting until each
// callback consumes its copy with Release().
//
// ONLY use this for a tight, single-call, scoped round: construct, dispatch, Wait(), with
// nothing else on the constructing fiber touching a blocking counter in between. The fiber
// has exactly one embedded slot, so two independently-scoped rounds overlapping on the same
// fiber (e.g. a long-lived owner whose Run()/Wait() are separate calls) will corrupt each
// other - DCHECK-enforced in EmbeddedBlockingCounter::Start() but only in debug builds. If
// Run() and Wait() aren't back-to-back on the same call stack, use BlockingCounter instead.
class FiberBlockingCounter {
 public:
  explicit FiberBlockingCounter(unsigned start_count);

  bool Wait() {
    return owner_->blocking_counter().Wait();
  }

  void Release() &&;
  void Release() & = delete;

 private:
  boost::intrusive_ptr<detail::FiberInterface> owner_;
};
static_assert(sizeof(FiberBlockingCounter) == 8);

// Fiber-aware, non-recursive reader-writer mutex that prefers readers: lock_shared() blocks only
// while a writer holds the mutex, so sustained reader traffic can starve writers.
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
      // The writer may have unlocked since, so this rollback can be the last reader leaving.
      UnlockSharedAndNotify();
      return false;
    }
    return true;
  }

  void lock_shared() ABSL_SHARED_LOCK_FUNCTION() {
    ec_.await([this] { return try_lock_shared(); });
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    state_.fetch_and(~(WRITER), std::memory_order_release);
    ec_.notifyAll();
  }

  void unlock_shared() ABSL_UNLOCK_FUNCTION() {
    UnlockSharedAndNotify();
  }

 private:
  enum : int32_t { READER = 4, WRITER = 1 };

  // Readers wait only while a writer holds the mutex, so only writers can be waiting here, and
  // they can proceed only once the last reader leaves.
  void UnlockSharedAndNotify() {
    if (state_.fetch_add(-READER, std::memory_order_release) == READER)
      ec_.notifyAll();
  }

  EventCount ec_;
  std::atomic_uint32_t state_{0};
};

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

// Fiber-aware, non-recursive reader-writer mutex for fibers of a single thread. Not thread-safe.
// Prefers readers: lock_shared() blocks only while a writer holds the mutex, even if writers are
// waiting, so a reader never waits for a pending writer. Sustained reader traffic can starve
// writers.
class ABSL_LOCKABLE LocalSharedMutex {
 public:
  LocalSharedMutex() = default;

  LocalSharedMutex(const LocalSharedMutex&) = delete;
  LocalSharedMutex& operator=(const LocalSharedMutex&) = delete;

  bool try_lock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    if (writer_ || readers_)
      return false;
    writer_ = true;
    return true;
  }

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    NoOpLock lk;
    cv_.wait(lk, [this] { return try_lock(); });
  }

  bool try_lock_shared() ABSL_SHARED_TRYLOCK_FUNCTION(true) {
    if (writer_)
      return false;
    ++readers_;
    return true;
  }

  void lock_shared() ABSL_SHARED_LOCK_FUNCTION() {
    NoOpLock lk;
    cv_.wait(lk, [this] { return try_lock_shared(); });
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    assert(writer_);
    writer_ = false;
    cv_.notify_all();
  }

  void unlock_shared() ABSL_UNLOCK_FUNCTION() {
    assert(readers_ > 0);
    if (--readers_ == 0)
      cv_.notify_all();  // Only writers can be waiting here.
  }

 private:
  CondVarAny cv_;
  uint32_t readers_ = 0;
  bool writer_ = false;
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

  detail::Waiter waiter(active->CreateClaimableWaiter());

  // store this fiber in waiting-queue, we can do it without spinlocks because
  // lt is already locked.
  wait_queue_.Link(&waiter);

  // release the lock suspend this fiber until tp.
  lt.unlock();

  // Either a notifier or the timeout wakes us, whichever claims the wait first. So when we time
  // out, no activation is on its way to us that could collide with the one that hands `lt` over to
  // us if lt.lock() suspends.
  bool timed_out = active->WaitUntil(tp);

  // lock back.
  lt.lock();

  // A notifier that found us timed out skipped us, but unlinked us from the queue.
  if (waiter.IsLinked()) {
    assert(timed_out);
    wait_queue_.Unlink(&waiter);
  }
  return timed_out ? std::cv_status::timeout : std::cv_status::no_timeout;
}

}  // namespace fb2

using fb2::BlockingCounter;

template <typename Pred> void Await(fb2::CondVarAny& cv, Pred&& pred) {
  fb2::NoOpLock lock;
  cv.wait(lock, std::forward<Pred>(pred));
}

}  // namespace util

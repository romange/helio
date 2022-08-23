// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <optional>
#include <ostream>

#include "base/RWSpinLock.h"
#include "util/fibers/event_count.h"

namespace std {

ostream& operator<<(ostream& o, const ::boost::fibers::channel_op_status op);

}  // namespace std

namespace util {

enum DoneWaitDirective { AND_NOTHING = 0, AND_RESET = 1 };

namespace fibers_ext {

inline uint32_t short_id(::boost::fibers::context* ctx) {
  return reinterpret_cast<uintptr_t>(ctx) / 8;
}

inline uint32_t short_id() {
  return short_id(::boost::fibers::context::active());
}

// Wrap canonical pattern for condition_variable + bool flag
// We can not synchronize threads with a condition-like variable on a stack.
// The reason is that it's possible that the main (waiting) thread will pass "Wait()" call
// and continue by destructing "done" variable while the background thread
// is still accessing "done". It's possible to fix it only with usage of mutex but we want to
// refrain from using mutex to allow non-blocking call to Notify(). Thus Done becomes
// io_context friendly. Therefore we must use heap based,
// reference counted Done object.
class Done {
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
        // We want to synchronize on all changes to obj performed in other threads.
        // obj is not atomic but we know that whatever was being written - has been written
        // in other threads and no references to obj exist anymore.
        // Therefore acquiring fence is enough to synchronize.
        // "acquire" requires a release opearation to mark the end of the memory changes we wish
        // to acquire, and "fetch_sub(std::memory_order_release)" provides this marker.
        // To summarize: fetch_sub(release) and fence(acquire) needed to order and synchronize
        // on changes on obj in most performant way.
        // See: https://stackoverflow.com/q/27751025/
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

 public:
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
  ptr_t impl_;
};

class SharedMutex {
 public:
  bool try_lock() {
    return spinlock_.try_lock();
  }

  void lock() {
    ec_.await([this] { return spinlock_.try_lock(); });
  }

  bool try_lock_shared() {
    return spinlock_.try_lock_shared();
  }

  void lock_shared() {
    ec_.await([this] { return spinlock_.try_lock_shared(); });
  }

  void unlock() {
    spinlock_.unlock();
    ec_.notifyAll();
  }

  void unlock_shared() {
    spinlock_.unlock_shared();
    ec_.notifyAll();
  }

 private:
  EventCount ec_;
  folly::RWSpinLock spinlock_;
};

// Callbacks must capture BlockingCounter object by value if they call Dec() function.
// The reason is that if a thread preempts right after count_.fetch_sub returns 1 but before
// ec_.notify was called, the object may be destroyed before ec_.notify is called.
class BlockingCounter {
  class Impl {
   public:
    Impl(unsigned count) : count_(count) {
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
      ec_.await([this] { return 0 == count_.load(std::memory_order_acquire); });
    }

    void Dec() {
      if (1 == count_.fetch_sub(1, std::memory_order_acq_rel))
        ec_.notify();
    }

   private:
    friend class BlockingCounter;
    EventCount ec_;
    std::atomic<std::uint32_t> use_count_{0};
    std::atomic_long count_;
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

  void Wait() {
    impl_->Wait();
  }

 private:
  ptr_t impl_;
};

class Semaphore {
 public:
  Semaphore(uint32_t cnt) : count_(cnt) {
  }

  void Wait(uint32_t nr = 1) {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    Wait(lock, nr);
  }

  void Signal(uint32_t nr = 1) {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    count_ += nr;
    lock.unlock();

    cond_.notify_all();
  }

  template <typename Lock> void Wait(Lock& l, uint32_t nr = 1) {
    cond_.wait(l, [&] { return count_ >= nr; });
    count_ -= nr;
  }

 private:
  ::boost::fibers::condition_variable_any cond_;
  ::boost::fibers::mutex mutex_;
  uint32_t count_;
};

// For synchronizing fibers in single-threaded environment.
struct NoOpLock {
  void lock() {
  }
  void unlock() {
  }
};

template <typename Pred> void Await(::boost::fibers::condition_variable_any& cv, Pred&& pred) {
  NoOpLock lock;
  cv.wait(lock, std::forward<Pred>(pred));
}

// Returns true if predicate become true, false if timeout reached.
template <typename Pred, typename Duration>
bool AwaitFor(::boost::fibers::condition_variable_any& cv, Pred&& pred, const Duration& duration) {
  NoOpLock lock;
  return cv.wait_for(lock, duration, std::forward<Pred>(pred));
}

// Single threaded synchronization primitive between fibers.
// fibers::unbufferred_channel has problematic design? with respect to move semantics and
// "try_push" method because it will move the value even if it was not pushed.
// Therefore, for single producer, single consumer single threaded case we can use this
// Cell class for emulating unbufferred_channel.
template <typename T> class Cell {
  std::optional<T> val_;
  ::boost::fibers::condition_variable_any cv_;

 public:
  bool IsEmpty() const {
    return !bool(val_);
  }

  // Might block the calling fiber.
  void Emplace(T&& val) {
    fibers_ext::Await(cv_, [this] { return IsEmpty(); });
    val_.emplace(std::forward<T>(val));
    cv_.notify_one();
  }

  void WaitTillFull() {
    fibers_ext::Await(cv_, [this] { return !IsEmpty(); });
  }

  T& value() {
    return *val_;  // optional stays engaged.
  }

  void Clear() {
    val_ = std::nullopt;
    cv_.notify_one();
  }
};

namespace detail {

template <typename R> class ResultMover {
  R r_;  // todo: to set as optional to support objects without default c'tor.
 public:
  template <typename Func> void Apply(Func&& f) {
    r_ = f();
  }

  // Returning rvalue-reference means returning the same object r_ instead of creating a
  // temporary R{r_}. Please note that when we return function-local object, we do not need to
  // return rvalue because RVO eliminates redundant object creation.
  // But for returning data member r_ it's more efficient.
  // "get() &&" means you can call this function only on rvalue ResultMover&& object.
  R&& get() && {
    return std::forward<R>(r_);
  }
};

template <> class ResultMover<void> {
 public:
  template <typename Func> void Apply(Func&& f) {
    f();
  }
  void get() {
  }
};

}  // namespace detail
}  // namespace fibers_ext
}  // namespace util

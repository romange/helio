// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/future.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>

namespace util {

namespace fibers_ext {

// dispatch - starts immediately,
// post - enqueues for activation
using Launch = boost::fibers::launch;

class Fiber {
 public:
  Fiber() = default;

  template <typename Fn, typename... Arg>
  Fiber(Launch policy, Fn&& fn, Arg&&... arg)
      : fb_(policy, std::forward<Fn>(fn), std::forward<Arg>(arg)...) {
  }

  template <typename Fn, typename... Arg>
  Fiber(Fn&& fn, Arg&&... arg)
      : fb_(Launch::post, std::forward<Fn>(fn), std::forward<Arg>(arg)...) {
  }

  Fiber(::boost::fibers::fiber fb) : fb_(std::move(fb)) {
  }

  void Join() {
    fb_.join();
  }

  void Detach() {
    fb_.detach();
  }

  Fiber& operator=(::boost::fibers::fiber&& fb) {
    fb_ = std::move(fb);
    return *this;
  }

  bool IsJoinable() const {
    return fb_.joinable();
  }

 private:
  ::boost::fibers::fiber fb_;
};

template <typename Clock, typename Duration>
void SleepUntil(std::chrono::time_point<Clock, Duration> const& sleep_time) {
  boost::this_fiber::sleep_until(sleep_time);
}

template <typename Rep, typename Period>
void SleepFor(std::chrono::duration<Rep, Period> const& timeout_duration) {
  SleepUntil(std::chrono::steady_clock::now() + timeout_duration);
}

inline void Yield() noexcept {
  boost::fibers::context::active()->yield();
}

class Mutex {
 private:
  ::boost::fibers::mutex mtx_;

 public:
  Mutex() = default;

  ~Mutex() {
  }

  Mutex(Mutex const&) = delete;
  Mutex& operator=(Mutex const&) = delete;

  void lock() {
    mtx_.lock();
  }

  bool try_lock() {
    return mtx_.try_lock();
  }

  void unlock() {
    mtx_.unlock();
  }
};

template <typename R> class Future {
 private:
  ::boost::fibers::future<R> future_;

 public:
  Future() = default;

  Future(::boost::fibers::future<R> future) : future_(std::move(future)) {
  }

  Future(Future&& other) noexcept : future_(std::move(other.future_)) {
  }

  Future& operator=(Future&& other) noexcept {
    future_ = std::move(other.future_);
    return *this;
  }

  Future(Future const&) = delete;
  Future& operator=(Future const&) = delete;

  R get() {
    return future_.get();
  }

  bool valid() const {
    return future_.valid();
  }

  void wait() const {
    future_.wait();
  }
};

template <typename R> class Promise {
 private:
  ::boost::fibers::promise<R> promise_;

 public:
  void set_value(R& value) {
    promise_.set_value(value);
  }

  void set_value(const R& value) {
    promise_.set_value(value);
  }

  void swap(Promise& other) noexcept {
    promise_.swap(other.promise_);
  }

  Future<R> get_future() {
    return promise_.get_future();
  }
};

}  // namespace fibers_ext

template <typename Fn, typename... Arg> fibers_ext::Fiber MakeFiber(Fn&& fn, Arg&&... arg) {
  return fibers_ext::Fiber(std::forward<Fn>(fn), std::forward<Arg>(arg)...);
}

namespace ThisFiber {

inline void SleepUntil(std::chrono::steady_clock::time_point tp) {
  fibers_ext::SleepUntil(tp);
}

inline void Yield() {
  fibers_ext::Yield();
}

template <typename Rep, typename Period>
void SleepFor(const std::chrono::duration<Rep, Period>& timeout_duration) {
  fibers_ext::SleepFor(timeout_duration);
}

}  // namespace ThisFiber
}  // namespace util
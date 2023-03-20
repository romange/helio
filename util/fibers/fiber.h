// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>

namespace util {

namespace fibers_ext {

// dispatch - starts immediately,
// post - enqueues for activation
using launch = boost::fibers::launch;

class Fiber {
 public:
  Fiber() = default;

  template <typename Fn, typename... Arg>
  Fiber(launch policy, Fn&& fn, Arg&&... arg)
      : fb_(policy, std::forward<Fn>(fn), std::forward<Arg>(arg)...) {
  }

  template <typename Fn, typename... Arg>
  Fiber(Fn&& fn, Arg&&... arg)
      : fb_(launch::post, std::forward<Fn>(fn), std::forward<Arg>(arg)...) {
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

}  // namespace fibers_ext

template <typename Fn, typename... Arg> fibers_ext::Fiber MakeFiber(Fn&& fn, Arg&&... arg) {
  return fibers_ext::Fiber(std::forward<Fn>(fn), std::forward<Arg>(arg)...);
}

}  // namespace util
// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <string_view>

#include "util/fibers/detail/fiber_interface.h"

namespace util {
namespace fb2 {

namespace detail {
template <typename X, typename Y>
using disable_overload = boost::context::detail::disable_overload<X, Y>;
}

class Fiber {
 public:
  using ID = uint64_t;

  Fiber() = default;

  template <typename Fn> Fiber(Fn&& fn) : Fiber(std::string_view{}, std::forward<Fn>(fn)) {
  }

  template <typename Fn>
  Fiber(std::string_view name, Fn&& fn) : Fiber(Launch::post, name, std::forward<Fn>(fn)) {
  }

  template <typename Fn>
  Fiber(Launch policy, std::string_view name, Fn&& fn)
      : impl_{util::fb2::detail::MakeWorkerFiberImpl(name, boost::context::fixedsize_stack(),
                                                     std::forward<Fn>(fn))} {
    Start(policy);
  }

  template <typename Fn, typename... Arg>
  Fiber(Launch policy, std::string_view name, Fn&& fn, Arg&&... arg)
      : impl_{util::fb2::detail::MakeWorkerFiberImpl(name, boost::context::fixedsize_stack(),
                                                     std::forward<Fn>(fn),
                                                     std::forward<Arg>(arg)...)} {
    Start(policy);
  }

  template <typename Fn, typename... Arg>
  Fiber(std::string_view name, Fn&& fn, Arg&&... arg)
      : Fiber(Launch::post, name, std::forward<Fn>(fn), std::forward<Arg>(arg)...) {
  }

  ~Fiber();

  Fiber(Fiber const&) = delete;
  Fiber& operator=(Fiber const&) = delete;

  Fiber(Fiber&& other) noexcept : impl_{} {
    swap(other);
  }

  Fiber& operator=(Fiber&& other) noexcept;

  void swap(Fiber& other) noexcept {
    impl_.swap(other.impl_);
  }

  ID get_id() const noexcept {
    return reinterpret_cast<ID>(impl_.get());
  }

  bool IsJoinable() const noexcept {
    return nullptr != impl_;
  }

  void Join();

  void Detach();

 private:
  void Start(Launch launch) {
    impl_->Start(launch);
  }

  boost::intrusive_ptr<util::fb2::detail::FiberInterface> impl_;
};

}  // namespace fb2

template <typename Fn, typename... Arg> fb2::Fiber MakeFiber(Fn&& fn, Arg&&... arg) {
  return fb2::Fiber(std::string_view{}, std::forward<Fn>(fn), std::forward<Arg>(arg)...);
}

namespace ThisFiber {

inline void SleepUntil(std::chrono::steady_clock::time_point tp) {
  static_assert(sizeof(tp) == 8);
  fb2::detail::FiberActive()->WaitUntil(tp);
}

inline void Yield() {
  fb2::detail::FiberActive()->Yield();
}

template <typename Rep, typename Period>
void SleepFor(const std::chrono::duration<Rep, Period>& timeout_duration) {
  SleepUntil(std::chrono::steady_clock::now() + timeout_duration);
}

inline void SetName(std::string_view name) {
  fb2::detail::FiberActive()->SetName(name);
}

};  // namespace ThisFiber

}  // namespace util

// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <string_view>

#include "util/fibers/detail/fiber_interface.h"

namespace util {
namespace fb2 {

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
  Fiber(Launch policy, Fn&& fn) : Fiber(policy, std::string_view{}, std::forward<Fn>(fn)) {
  }

  template <typename Fn, typename... Arg>
  Fiber(std::string_view name, Fn&& fn, Arg&&... arg)
      : Fiber(Launch::post, name, std::forward<Fn>(fn), std::forward<Arg>(arg)...) {
  }

  template <typename Fn, typename... Arg>
  Fiber(Launch policy, std::string_view name, Fn&& fn, Arg&&... arg)
      : impl_{detail::FiberPool::PopOrCreate(std::forward<Fn>(fn), std::forward<Arg>(arg)...)} {
    impl_->SetName(name);
    Start(policy);
  }

  template <typename Fn, typename StackAlloc, typename... Arg>
  Fiber(Launch policy, StackAlloc&&, std::string_view name, Fn&& fn, Arg&&... arg)
      : impl_{detail::FiberPool::PopOrCreate(std::forward<Fn>(fn), std::forward<Arg>(arg)...)} {
     impl_->SetName(name);
    Start(policy);
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

  // Join fiber if it's running, else do nothing.
  void JoinIfNeeded();

  void Detach();

 private:
  void Start(Launch launch) {
    impl_->Start(launch);
  }

  boost::intrusive_ptr<util::fb2::detail::FiberInterface> impl_;
};

// Returns the context switch epoch number for this thread.
uint64_t FiberSwitchEpoch() noexcept;

// Returns the aggregated delay between activation of fibers and
// the time they were switched to in microseconds.
uint64_t FiberSwitchDelayUsec() noexcept;

// Exposes the number of times fiber were running for a "long" time (longer than 1ms).
uint64_t FiberLongRunCnt() noexcept;

// Exposes total duration of fibers running for a "long" time (longer than 1ms).
uint64_t FiberLongRunSumUsec() noexcept;

// Injects a custom memory resource for stack allocation. Can be called only once.
// It is advised to call this function when a program starts.
void SetDefaultStackResource(PMR_NS::memory_resource* mr, size_t default_size = 64 * 1024);

// Returns the total stack size (virtual memory) for worker fibers for the current thread.
// Please note that RSS memory usage is usually smaller, depending on the actuall stack usage
// of the fibers.
size_t WorkerFibersStackSize();

// Returns number of worker fibers for the current thread.
size_t WorkerFibersCount();

class StdMallocResource : public PMR_NS::memory_resource {
 private:
  void* do_allocate(std::size_t size, std::size_t align) final;

  void do_deallocate(void* ptr, std::size_t size, std::size_t align) final;

  bool do_is_equal(const PMR_NS::memory_resource& o) const noexcept final {
    return this == &o;
  }
};

extern StdMallocResource std_malloc_resource;

}  // namespace fb2

template <typename Fn, typename... Arg> fb2::Fiber MakeFiber(Fn&& fn, Arg&&... arg) {
  return fb2::Fiber(std::string_view{}, std::forward<Fn>(fn), std::forward<Arg>(arg)...);
}

template <typename Fn, typename... Arg>
fb2::Fiber MakeFiber(fb2::Launch launch, Fn&& fn, Arg&&... arg) {
  return fb2::Fiber(launch, std::string_view{}, std::forward<Fn>(fn), std::forward<Arg>(arg)...);
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

inline std::string_view GetName() {
  return fb2::detail::FiberActive()->name();
}

};  // namespace ThisFiber

class FiberAtomicGuard {
  FiberAtomicGuard(const FiberAtomicGuard&) = delete;

 public:
  FiberAtomicGuard() {
    fb2::detail::EnterFiberAtomicSection();
  }

  ~FiberAtomicGuard() {
    fb2::detail::LeaveFiberAtomicSection();
  }
};

}  // namespace util

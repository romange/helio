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

  struct Opts {
    Launch launch = Launch::post;
    FiberPriority priority = FiberPriority::NORMAL;
    std::string name;
    uint32_t stack_size = 64 * 1024;
  };

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
      : Fiber(Opts{.launch = policy, .name = std::string{name}}, std::forward<Fn>(fn),
              std::forward<Arg>(arg)...) {
  }

  template <typename Fn, typename StackAlloc, typename... Arg>
  Fiber(Launch policy, StackAlloc&& stack_alloc, std::string_view name, Fn&& fn, Arg&&... arg)
      : impl_{util::fb2::detail::MakeWorkerFiberImpl(
            name, FiberPriority::NORMAL, std::forward<StackAlloc>(stack_alloc),
            std::forward<Fn>(fn), std::forward<Arg>(arg)...)} {
    Start(policy);
  }

  template <typename Fn, typename... Arg>
  Fiber(const Opts& opts, Fn&& fn, Arg&&... arg)
      : impl_{detail::default_stack_resource
                  ? detail::MakeWorkerFiberImpl(
                        opts.name, opts.priority,
                        FixedStackAllocator(detail::default_stack_resource, opts.stack_size),
                        std::forward<Fn>(fn), std::forward<Arg>(arg)...)
                  : detail::MakeWorkerFiberImpl(opts.name, opts.priority,
                                                // explicitly pass the stack size.
                                                boost::context::fixedsize_stack(opts.stack_size),
                                                std::forward<Fn>(fn), std::forward<Arg>(arg)...)} {
    Start(opts.launch);
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

  // returns true if the fiber is running in the calling thread
  bool IsLocal() const {
    return impl_->scheduler() == detail::FiberActive()->scheduler();
  }

  void Join();

  // Join fiber if it's running, else do nothing.
  void JoinIfNeeded();

  void Detach();

  // Returns true if this is the active (calling) fiber.
  bool IsActive() const {
    return impl_.get() == detail::FiberActive();
  }

 private:
  void Start(Launch launch) {
    impl_->Start(launch);
  }

  boost::intrusive_ptr<util::fb2::detail::FiberInterface> impl_;
};

// Returns the context switch epoch number for this thread.
inline uint64_t FiberSwitchEpoch() noexcept {
  return detail::FiberInterface::TL_Epoch();
}

// Returns the aggregated delay between activation of fibers and
// the time they were switched to in microseconds.
uint64_t FiberSwitchDelayUsec() noexcept;

// Exposes the number of times fiber were running for a "long" time (longer than 1ms).
uint64_t FiberLongRunCnt() noexcept;

// Exposes total duration of fibers running for a "long" time (longer than 1ms).
uint64_t FiberLongRunSumUsec() noexcept;
uint32_t GetFiberRunSeq() noexcept;

// Returns last K names of fibers that were running in this thread since it became active.
std::vector<std::string> GetPastFiberNames() noexcept;

void SetFiberLongRunWarningThreshold(uint32_t warn_ms);

// Injects a custom memory resource for stack allocation. Can be called only once.
// It is advised to call this function when a program starts.
void SetDefaultStackResource(PMR_NS::memory_resource* mr, size_t default_size = 64 * 1024);

// Returns the total stack size (virtual memory) for worker fibers for the current thread.
// Please note that RSS memory usage is usually smaller, depending on the actuall stack usage
// of the fibers.
size_t WorkerFibersStackSize();

// Returns number of worker fibers for the current thread.
size_t WorkerFibersCount();
void PrintFiberStackTracesInThread();

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

inline fb2::FiberPriority Priority() {
  return fb2::detail::FiberActive()->Priority();
}

inline uint64_t GetRunningTimeCycles() {
  return fb2::detail::FiberActive()->GetRunningTimeCycles();
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

/* Returns the margin between the provided stack address
   and the bottom of the fiber's stack. */
inline uint32_t GetStackMargin(const void* stack_address) {
  return fb2::detail::FiberActive()->GetStackMargin(stack_address);
}

inline void CheckSafetyMargin() {
  fb2::detail::FiberActive()->CheckStackMargin();
}

inline uint64_t GetPreemptCount() {
  return fb2::detail::FiberActive()->preempt_cnt();
}

// Returns false inside NoStallGuard, see below.
inline bool CanStall() {
  return fb2::detail::FiberActive()->no_stall_depth() == 0;
}

// Installs a hook on the active fiber.
inline fb2::FiberSwitchHook SetSwitchHook(fb2::FiberSwitchHook hook) noexcept {
  return fb2::detail::FiberActive()->SetSwitchHook(hook);
}

class PrintLocalsCallback {
 public:
  template <typename Fn> PrintLocalsCallback(Fn&& fn) {
    fb2::detail::FiberActive()->SetPrintStacktraceCb(std::forward<Fn>(fn));
  }

  ~PrintLocalsCallback() {
    fb2::detail::FiberActive()->SetPrintStacktraceCb(nullptr);
  }
};

};  // namespace ThisFiber

// Two guards restrict how the calling fiber may wait while they are alive:
//
// FiberAtomicGuard forbids any suspension. It protects code that must not yield at all, for
// example state under spinlocks. It is tracked per thread, and suspending inside it fails.
//
// NoStallGuard forbids stalls: waits whose length is controlled by something outside the
// process, e.g. a socket peer. Use it while the fiber holds something that other fibers wait for,
// e.g. a token that a global operation drains or an object borrowed from a small per-thread pool:
// a client that stops reading would otherwise stall all of them. Waits inside the process
// (mutexes, EventCount, timers, Yield(), file I/O) stay allowed. It is tracked per fiber, so it
// stays in effect across suspensions and migrations. The guard only marks the section: code that
// may stall checks it, e.g. DCHECK(ThisFiber::CanStall()) before a blocking socket write.
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

class NoStallGuard {
  NoStallGuard(const NoStallGuard&) = delete;

 public:
  NoStallGuard() : fiber_(fb2::detail::FiberActive()) {
    fiber_->EnterNoStall();
  }

  ~NoStallGuard() {
    fiber_->LeaveNoStall();
  }

 private:
  fb2::detail::FiberInterface* fiber_;
};

}  // namespace util

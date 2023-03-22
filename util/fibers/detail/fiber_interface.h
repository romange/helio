// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <boost/context/fiber.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/slist.hpp>
#include <chrono>

#include "base/mpsc_intrusive_queue.h"

namespace util {
namespace fb2 {

enum class Launch {
  dispatch,  // switch to the fiber immediately
  post       // enqueue the fiber for activation but continue with the current fiber.
};

namespace detail {

using FI_ListHook =
    boost::intrusive::slist_member_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>;

using FI_SleepHook =
    boost::intrusive::set_member_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>;

class Scheduler;

class FiberInterface {
  friend class Scheduler;

 protected:
  // holds its own fiber_context when it's not active.
  // the difference between fiber_context and continuation is that continuation is launched
  // straight away via callcc and fiber is created without switching to it.

  // TODO: I still do not know how continuation_fcontext and fiber_fcontext achieve this
  // difference because their code looks very similar except some renaming.
  //
  // Important: this should be the first data member in the class,
  // because it should be destroyed the last:
  // It indirectly deallocates the memory that backs up this instance so other objects will become
  // garbage. We also solve this problem by moving the entry before calling the d'tor inside
  // intrusive_ptr_release but just in case we keep the ordering here as well.
  ::boost::context::fiber_context entry_;  // 8 bytes

 public:
  enum Type : uint8_t { MAIN, DISPATCH, WORKER };

  // init_count is the initial use_count of the fiber.
  FiberInterface(Type type, uint32_t init_count, std::string_view nm = std::string_view{});

  virtual ~FiberInterface();

  FI_ListHook list_hook;
  FI_SleepHook sleep_hook;

  ::boost::context::fiber_context SwitchTo();

  void Start(Launch launch);

  void Join();

  // inline
  void Yield();

  // inline
  void WaitUntil(std::chrono::steady_clock::time_point tp);

  // Schedules another fiber without switching to it.
  // other can belong to another thread.
  void ActivateOther(FiberInterface* other);

  void Suspend();

  bool IsDefined() const {
    return bool(entry_);
  }

#if 0
  void StartParking() {
    flags_.fetch_or(kParkingInProgress, std::memory_order_relaxed);
  }

  // Notifies a fiber that intends to park to resume itself.
  void NotifyParked(FiberInterface* other);
  FiberInterface* NotifyParked(uint64_t token);
  void NotifyAllParked(uint64_t token);
  void SuspendUntilWakeup();

  // Returns true if the fiber was suspended, false otherwise.
  bool SuspendConditionally(uint64_t token, absl::FunctionRef<bool()> validate);

  void set_park_token(uint64_t token) {
    park_token_ = token;
  }

  uint64_t park_token() const {
    return park_token_;
  }
#endif

  // We need refcounting for referencing handles via .
  friend void intrusive_ptr_add_ref(FiberInterface* ctx) noexcept {
    ctx->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend void intrusive_ptr_release(FiberInterface* ctx) noexcept {
    if (1 == ctx->use_count_.fetch_sub(1, std::memory_order_release)) {
      std::atomic_thread_fence(std::memory_order_acquire);

      // At this time, entry_ contains the jump point to a stack inside Terminate() right before
      // it returns (where it preempted).
      //
      // The order here is important:
      // When we get here
      // 1. we first move the entry out of the object, so that ~FiberInterface won't destroy it.
      // 2. Then we call the destructor
      // 3. Then we switch to entry and after the switch we release the stack as well.
      boost::context::fiber c = std::move(ctx->entry_);

      // destruct object
      ctx->~FiberInterface();

      // jumps back to end of the Terminate function,
      // exits the fiber and deallocates the stack.
      std::move(c).resume();
    }
  }

  Scheduler* scheduler() {
    return scheduler_;
  }

  Type type() const {
    return type_;
  }

  // For MPSCIntrusiveQueue queue.
  friend void MPSC_intrusive_store_next(FiberInterface* dest, FiberInterface* next_node) {
    dest->next_.store(next_node, std::memory_order_relaxed);
  }

  friend FiberInterface* MPSC_intrusive_load_next(const FiberInterface& src) {
    return src.next_.load(std::memory_order_acquire);
  }

  void SetName(std::string_view nm);

  const char* name() const {
    return name_;
  }

 protected:
  static constexpr uint16_t kTerminatedBit = 0x1;
  static constexpr uint16_t kBusyBit = 0x2;
  static constexpr uint16_t kParkingInProgress = 0x4;

  using WaitQueueType = boost::intrusive::slist<
      FiberInterface,
      boost::intrusive::member_hook<FiberInterface, FI_ListHook, &FiberInterface::list_hook>>;

  ::boost::context::fiber_context Terminate();

  std::atomic<uint32_t> use_count_;
  std::atomic<uint16_t> flags_;

  Type type_;

  // FiberInterfaces that join on this fiber to terminate are added here.
  WaitQueueType wait_queue_;

  Scheduler* scheduler_ = nullptr;
  uint64_t park_token_ = 0;

  std::atomic<FiberInterface*> next_{nullptr};
  std::chrono::steady_clock::time_point tp_;

  char name_[24];
};

template <typename Fn, typename... Arg> class WorkerFiberImpl : public FiberInterface {
  using FbCntx = ::boost::context::fiber_context;

 public:
  template <typename StackAlloc>
  WorkerFiberImpl(std::string_view name, const boost::context::preallocated& palloc,
                  StackAlloc&& salloc, Fn&& fn, Arg&&... arg)
      : FiberInterface(WORKER, 1, name), fn_(std::forward<Fn>(fn)),
        arg_(std::forward<Arg>(arg)...) {
    entry_ = FbCntx(std::allocator_arg, palloc, std::forward<StackAlloc>(salloc),
                    [this](FbCntx&& caller) { return run_(std::move(caller)); });
  }

 private:
  FbCntx run_(FbCntx&& c) {
    // assert(!c)  <- we never pass the caller,
    // because with update c_ with it before switching.
    {
      // fn and tpl must be destroyed before calling terminate()
      auto fn = std::move(fn_);
      auto arg = std::move(arg_);
      std::apply(std::move(fn), std::move(arg));
    }

    return Terminate();
  }

  // Without decay - fn_ can be a reference, depending how a function is passed to the constructor.
  typename std::decay<Fn>::type fn_;
  std::tuple<std::decay_t<Arg>...> arg_;
};

template <typename FbImpl>
boost::context::preallocated MakePreallocated(const boost::context::stack_context& sctx) {
  // reserve space for control structure
  uintptr_t storage =
      (reinterpret_cast<uintptr_t>(sctx.sp) - sizeof(FbImpl)) & ~static_cast<uintptr_t>(0xff);
  uintptr_t stack_bottom = reinterpret_cast<uintptr_t>(sctx.sp) - static_cast<uintptr_t>(sctx.size);
  const std::size_t size = storage - stack_bottom;
  void* sp_ptr = reinterpret_cast<void*>(storage);

  return boost::context::preallocated{sp_ptr, size, sctx};
}

template <typename StackAlloc, typename Fn, typename... Arg>
static WorkerFiberImpl<Fn, Arg...>* MakeWorkerFiberImpl(std::string_view name, StackAlloc&& salloc,
                                                        Fn&& fn, Arg&&... arg) {
  boost::context::stack_context sctx = salloc.allocate();
  using WorkerImpl = WorkerFiberImpl<Fn, Arg...>;
  boost::context::preallocated palloc = MakePreallocated<WorkerImpl>(sctx);

  void* sp_ptr = palloc.sp;

  // placement new of context on top of fiber's stack
  WorkerImpl* fctx =
      new (sp_ptr) WorkerImpl{name, std::move(palloc), std::forward<StackAlloc>(salloc),
                              std::forward<Fn>(fn), std::forward<Arg>(arg)...};
  return fctx;
}

FiberInterface* FiberActive() noexcept;

}  // namespace detail
}  // namespace fb2
}  // namespace util

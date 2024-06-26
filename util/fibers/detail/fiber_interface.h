// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <boost/context/fiber.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <chrono>

#include "base/mpsc_intrusive_queue.h"
#include "base/pmr/memory_resource.h"
#include "util/fibers/detail/wait_queue.h"
#include "base/mpmc_bounded_queue.h"

namespace util {
namespace fb2 {

enum class Launch {
  dispatch,  // switch to the fiber immediately
  post       // enqueue the fiber for activation but continue with the current fiber.
};

// based on boost::context::fixedsize_stack but uses pmr::memory_resource for allocation.
class FixedStackAllocator {
 public:
  using stack_context = boost::context::stack_context;

  FixedStackAllocator(PMR_NS::memory_resource* mr, std::size_t size = 64 * 1024)
      : mr_(mr), size_(size) {
  }

  stack_context allocate() {
    void* vp = mr_->allocate(size_);
    stack_context sctx;
    sctx.size = size_;
    sctx.sp = static_cast<char*>(vp) + sctx.size;
    return sctx;
  }

  void deallocate(stack_context& sctx) BOOST_NOEXCEPT_OR_NOTHROW {
    void* vp = static_cast<char*>(sctx.sp) - sctx.size;
    mr_->deallocate(vp, sctx.size);
  }

 private:
  PMR_NS::memory_resource* mr_;
  std::size_t size_;
};

namespace detail {

using FI_ListHook =
    boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>;

using FI_SleepHook =
    boost::intrusive::set_member_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>;

class Scheduler;

class FiberInterface {
  friend class Scheduler;

  
 protected:
 static constexpr uint64_t kRemoteFree = 1;


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

  // public hooks for intrusive data structures.
  FI_ListHook list_hook;  // used to add to ready/terminate queues.
  FI_SleepHook sleep_hook;
  FI_ListHook fibers_hook;  // For a list of all fibers in the thread

  // init_count is the initial use_count of the fiber.
  FiberInterface(Type type, uint32_t init_count, std::string_view nm = std::string_view{});

  virtual ~FiberInterface();

  // Switch functions
  ::boost::context::fiber_context SwitchTo();
  void SwitchToAndExecute(std::function<void()> fn);

  using PrintFn = std::function<void(FiberInterface*)>;

  // ExecuteInFiberStack function must be called from the thread where this fiber is running.
  // 'fn' should not preempt (i.e. should not use any fiber-blocking primitives),
  void ExecuteOnFiberStack(PrintFn fn);

  // Deprecated. Use detail::PrintAllFiberStackTraces() below.
  static void PrintAllFiberStackTraces();

  void Start(Launch launch);

  void Join();

  void Yield();

  // inline
  bool WaitUntil(std::chrono::steady_clock::time_point tp);

  // Schedules another fiber without switching to it.
  // other can belong to another thread.
  void ActivateOther(FiberInterface* other);

  void Suspend();

  // Detaches itself from the thread scheduler.
  void DetachScheduler();

  // Attaches itself to a thread scheduler and makes it ready to run.
  // Must be detached first.
  void AttachScheduler();

  bool IsDefined() const {
    return bool(entry_);
  }

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
#if defined(__clang__)
  // clang ubsan checks that dest is a proper object but it breaks with MPSCIntrusiveQueue
  // setting a stub.next field since the stub is not properly initialized.
  __attribute__((no_sanitize("undefined")))
#endif
  friend void
  MPSC_intrusive_store_next(FiberInterface* dest, FiberInterface* next_node) {
    dest->remote_next_.store(next_node, std::memory_order_relaxed);
  }

  friend FiberInterface* MPSC_intrusive_load_next(const FiberInterface& src) {
    return src.remote_next_.load(std::memory_order_acquire);
  }

  void SetName(std::string_view nm);

  const char* name() const {
    return name_;
  }

  uint32_t DEBUG_use_count() const {
    return use_count_.load(std::memory_order_relaxed);
  }

  Waiter CreateWaiter() {
    return Waiter{this};
  }

  void PullMyselfFromRemoteReadyQueue();

  bool IsScheduledRemotely() const {
    return uint64_t(remote_next_.load(std::memory_order_relaxed)) != kRemoteFree;
  }

  uint32_t stack_size() const {
    return stack_size_;
  }

  uint64_t DEBUG_remote_epoch = 0;

 protected:
  static constexpr uint16_t kTerminatedBit = 0x1;
  static constexpr uint16_t kBusyBit = 0x2;

  // used to set up a critical section when scheduling a fiber from another thread.
  static constexpr uint16_t kScheduleRemote = 0x4;

  ::boost::context::fiber_context Terminate();

  std::atomic<uint32_t> use_count_;  // used for intrusive_ptr refcounting.

  // trace_ variable - used only for debugging purposes.
  enum TraceState : uint8_t {
    TRACE_NONE,
    TRACE_SLEEP_WAKE,
    TRACE_TERMINATE,
    TRACE_READY
  } trace_ = TRACE_NONE;
  Type type_;

  std::atomic<uint16_t> flags_{0};

  // FiberInterfaces that join on this fiber to terminate are added here.
  WaitQueue join_q_;

  Scheduler* scheduler_ = nullptr;

  std::atomic<FiberInterface*> remote_next_{nullptr};

  // used for sleeping with a timeout. Specifies the time when this fiber should be woken up.
  std::chrono::steady_clock::time_point tp_;

  // A tsc of when this fiber becames ready or becomes active (in cycles).
  uint64_t cpu_tsc_ = 0;
  char name_[24];
  uint32_t stack_size_ = 0;
 private:
  // Handles all the stats and also updates the involved data structure before actually switching
  // the fiber context. Returns the active fiber before the context switch.
  FiberInterface* SwitchSetup();
};

template <typename Fn, typename... Arg> class WorkerFiberImpl : public FiberInterface {
  using FbCntx = ::boost::context::fiber_context;

 public:
  template <typename StackAlloc>
  WorkerFiberImpl(std::string_view name, const boost::context::preallocated& palloc,
                  StackAlloc&& salloc, Fn&& fn, Arg&&... arg)
      : FiberInterface(WORKER, 1, name), fn_(std::forward<Fn>(fn)),
        arg_(std::forward<Arg>(arg)...) {
    stack_size_ = palloc.sctx.size;
    entry_ = FbCntx(std::allocator_arg, palloc, std::forward<StackAlloc>(salloc),
                    [this](FbCntx&& caller) { return run_(std::move(caller)); });
#if defined(BOOST_USE_UCONTEXT)
    entry_ = std::move(entry_).resume();
#endif
  }

 private:
  FbCntx run_(FbCntx&& c) {
    // assert(!c)  <- we never pass the caller,
    // because with update c_ with it before switching.
    {
      // fn and tpl must be destroyed before calling terminate()
      auto fn = std::move(fn_);
      auto arg = std::move(arg_);

#if defined(BOOST_USE_UCONTEXT)
      std::move(c).resume();
#endif

      std::apply(std::move(fn), std::move(arg));
    }

    return Terminate();
  }

  // Without decay - fn_ can be a reference, depending how a function is passed to the constructor.
  typename std::decay<Fn>::type fn_;
  std::tuple<std::decay_t<Arg>...> arg_;
};

class ReusableFiberImpl : public FiberInterface {
  using FbCntx = ::boost::context::fiber_context;

 public:
  template <typename StackAlloc>
  ReusableFiberImpl(std::string_view name, const boost::context::preallocated& palloc,
                  StackAlloc&& salloc) : FiberInterface(WORKER, 1, name) {
    stack_size_ = palloc.sctx.size;
    entry_ = FbCntx(std::allocator_arg, palloc, std::forward<StackAlloc>(salloc),
                    [this](FbCntx&& caller) { return run_(std::move(caller)); });
#if defined(BOOST_USE_UCONTEXT)
    entry_ = std::move(entry_).resume();
#endif
  }
  
  template <class Fn, class ...Arg>
  void SetTask(Fn&& fn, Arg&&... arg) {
    flags_.fetch_and(~kTerminatedBit, std::memory_order_release);
    remote_next_.store((FiberInterface*)kRemoteFree, std::memory_order_relaxed);
    auto fn_ptr = std::make_shared<std::decay_t<Fn>>(std::forward<Fn>(fn));
    auto arg_ptr = std::make_shared<std::tuple<std::decay_t<Arg>...>>(std::forward<Arg>(arg)...);
    fn_ = [fn_ptr, arg_ptr]() mutable {
        std::apply(std::move(*fn_ptr), std::move(*arg_ptr));
      };
  }

 private:
  FbCntx run_(FbCntx&& c) {
    // assert(!c)  <- we never pass the caller,
    // because with update c_ with it before switching.
    while (!stop_flag) {
      if (fn_) {
        // fn and tpl must be destroyed before calling terminate()
        auto fn = std::move(fn_);
        fn_ = nullptr;

#if defined(BOOST_USE_UCONTEXT)
        std::move(c).resume();
#endif

        fn();
      } else {
        MoveToPool();
      }
    }

    return Terminate();
  }

  void MoveToPool();

  volatile bool stop_flag = false;
  // Without decay - fn_ can be a reference, depending how a function is passed to the constructor.
  std::function<void()> fn_;
};

template <typename FbImpl>
boost::context::preallocated MakePreallocated(const boost::context::stack_context& sctx) {
  // reserve space for FbImpl control structure. fb_impl_ptr points to the address where FbImpl
  // will be placed.
  uintptr_t fb_impl_ptr =
      (reinterpret_cast<uintptr_t>(sctx.sp) - sizeof(FbImpl)) & ~static_cast<uintptr_t>(0xff);

  // stack_bottom is the real pointer allocated inside salloc.allocate()
  // sctx.sp, on the other hand, points to the top (right boundary) of the allocated region.
  // The deeper the stack grows, the closer it gets to the stack_bottom.
  uintptr_t stack_bottom = reinterpret_cast<uintptr_t>(sctx.sp) - static_cast<uintptr_t>(sctx.size);
  const std::size_t size = fb_impl_ptr - stack_bottom;  // effective stack size.

  // we place FbImpl object at the top of the stack.
  void* sp_ptr = reinterpret_cast<void*>(fb_impl_ptr);

  return boost::context::preallocated{sp_ptr, size, sctx};
}

// temporary static
class FiberPool {
 public:
  static void AddAvailableFiber(ReusableFiberImpl* cntx) {
    auto res = available_queue_.try_enqueue(cntx);
    assert(res);
  }

  template <typename Fn, typename... Arg>
  static ReusableFiberImpl* PopOrCreate( Fn&& fn, Arg&&... arg) {
    FiberInterface* popped_fiber = nullptr;
    bool is_popped = available_queue_.try_dequeue(popped_fiber);
    auto* res = is_popped ? static_cast<ReusableFiberImpl*>(popped_fiber) : MakeReusableFiberImpl();
    res->SetTask(std::forward<Fn>(fn), std::forward<Arg>(arg)...);
    return res;
  }

 private:
  static ReusableFiberImpl* MakeReusableFiberImpl() {
    boost::context::fixedsize_stack salloc(64 * 1024);
    boost::context::stack_context sctx = salloc.allocate();
    boost::context::preallocated palloc = MakePreallocated<ReusableFiberImpl>(sctx);

    void* obj_ptr = palloc.sp;  // copy because we move palloc.

    // placement new of context on top of fiber's stack
    return new (obj_ptr) ReusableFiberImpl("", std::move(palloc), std::move(salloc));
  }

 private:
  static base::mpmc_bounded_queue<FiberInterface*> available_queue_;
};

template <typename StackAlloc, typename Fn, typename... Arg>
static WorkerFiberImpl<Fn, Arg...>* MakeWorkerFiberImpl(std::string_view name, StackAlloc&& salloc,
                                                        Fn&& fn, Arg&&... arg) {
  boost::context::stack_context sctx = salloc.allocate();
  using WorkerImpl = WorkerFiberImpl<Fn, Arg...>;
  boost::context::preallocated palloc = MakePreallocated<WorkerImpl>(sctx);

  void* obj_ptr = palloc.sp;  // copy because we move palloc.

  // placement new of context on top of fiber's stack
  WorkerImpl* fctx =
      new (obj_ptr) WorkerImpl{name, std::move(palloc), std::forward<StackAlloc>(salloc),
                               std::forward<Fn>(fn), std::forward<Arg>(arg)...};
  return fctx;
}

FiberInterface* FiberActive() noexcept;
void EnterFiberAtomicSection() noexcept;
void LeaveFiberAtomicSection() noexcept;
bool IsFiberAtomicSection() noexcept;

void PrintAllFiberStackTraces();

// Runs fn on all fibers in the thread. See FiberInterface::ExecuteOnFiberStack for details.
void ExecuteOnAllFiberStacks(FiberInterface::PrintFn fn);

// A convenience function to improve the readability of the code.
inline void ActivateSameThread(FiberInterface* active, FiberInterface* other) {
  assert(active->scheduler() == other->scheduler());
  active->ActivateOther(other);
}

extern PMR_NS::memory_resource* default_stack_resource;
extern size_t default_stack_size;

}  // namespace detail
}  // namespace fb2
}  // namespace util

#ifndef __FIBERS_SCHEDULER_H__
#include "util/fibers/detail/fiber_interface_impl.h"
#endif

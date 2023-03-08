// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/context/fiber.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/slist.hpp>

#include <atomic>
#include <chrono>

namespace util {
namespace fb2 {
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
  // Important: this must be the first data member in the class,
  // because it should be destroyed the last:
  // It indirectly deallocates the memory that backs up this instance so other objects will become
  // garbage. We also solve this problem by moving the entry before calling the d'tor inside
  // intrusive_ptr_release but just in case we keep the ordering here as well.
  ::boost::context::fiber_context entry_;  // 8 bytes

 public:
  enum Type : uint8_t { MAIN, DISPATCH, WORKER };

  FiberInterface(Type type, uint32_t init_count, std::string_view nm = std::string_view{});

  virtual ~FiberInterface();

  FI_ListHook list_hook;
  FI_SleepHook sleep_hook;

  ::boost::context::fiber_context SwitchTo();

  void Start();
  void Join();
  void Yield();
  void WaitUntil(std::chrono::steady_clock::time_point tp);

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

 protected:
  // TODO: should be mpsc lock-free intrusive queue.
  using WaitQueue = boost::intrusive::slist<
      FiberInterface,
      boost::intrusive::member_hook<FiberInterface, FI_ListHook, &FiberInterface::list_hook>>;


  ::boost::context::fiber_context Terminate();

  std::atomic<uint32_t> use_count_;

  union {
    uint16_t flagval_;
    struct {
      uint16_t terminated : 1;
    } flags;
  };

  Type type_;

  // FiberInterfaces that join on this fiber to terminate are added here.
  WaitQueue wait_queue_;

  Scheduler* scheduler_ = nullptr;
  std::chrono::steady_clock::time_point tp_;

  char name_[24];
};


// The class that is responsible for fiber management and scheduling.
// It's main loop is in DefaultDispatch() and it runs in the context of Dispatch fiber.
// Optionally, main fiber can override the main scheduling loop with custom one.
// The override call is done via SetCustomDispatcher() and it's called from the main fiber.
class Scheduler {
 public:
  Scheduler(FiberInterface* main);
  ~Scheduler();

  void MarkReady(FiberInterface* cntx) {
    ready_queue_.push_back(*cntx);
  }

  void Attach(FiberInterface* cntx);

  void ScheduleTermination(FiberInterface* cntx);

  bool HasReady() const {
    return !ready_queue_.empty();
  }

  void DefaultDispatch();

  ::boost::context::fiber_context Preempt();

  void WaitUntil(std::chrono::steady_clock::time_point tp, FiberInterface* me);

  // Assumes HasReady() is true.
  FiberInterface* PopReady() {
    FiberInterface* res = &ready_queue_.front();
    ready_queue_.pop_front();
    return res;
  }

  FiberInterface* main_context() {
    return main_cntx_;
  }

  bool IsShutdown() const {
    return shutdown_;
  }

  void DestroyTerminated();
  void ProcessSleep();

 private:
  // I use cache_last<true> so that slist will have push_back support.
  using FI_Queue = boost::intrusive::slist<
      FiberInterface,
      boost::intrusive::member_hook<FiberInterface, FI_ListHook, &FiberInterface::list_hook>,
      boost::intrusive::constant_time_size<false>, boost::intrusive::cache_last<true>>;

  struct TpLess {
    bool operator()(const FiberInterface& l, const FiberInterface& r) const noexcept {
      return l.tp_ < r.tp_;
    }
  };

  using SleepQueue = boost::intrusive::multiset<
      FiberInterface,
      boost::intrusive::member_hook<FiberInterface, FI_SleepHook, &FiberInterface::sleep_hook>,
      boost::intrusive::constant_time_size<false>, boost::intrusive::compare<TpLess>>;

  static constexpr size_t kQSize = sizeof(FI_Queue);

  FiberInterface* main_cntx_;
  boost::intrusive_ptr<FiberInterface> dispatch_cntx_;
  FI_Queue ready_queue_, terminate_queue_;
  SleepQueue sleep_queue_;

  bool shutdown_ = false;
  uint32_t num_worker_fibers_ = 0;
};


template <typename Fn> class WorkerFiberImpl : public FiberInterface {
  using FbCntx = ::boost::context::fiber_context;

 public:
  template <typename StackAlloc>
  WorkerFiberImpl(std::string_view name, const boost::context::preallocated& palloc,
                  StackAlloc&& salloc, Fn&& fn)
      : FiberInterface(WORKER, 1, name), fn_(std::forward<Fn>(fn)) {
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
      fn();
    }

    return Terminate();
  }

  Fn fn_;
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

template <typename StackAlloc, typename Fn>
static WorkerFiberImpl<Fn>* MakeWorkerFiberImpl(std::string_view name, StackAlloc&& salloc,
                                                Fn&& fn) {
  boost::context::stack_context sctx = salloc.allocate();
  boost::context::preallocated palloc = MakePreallocated<WorkerFiberImpl<Fn>>(sctx);

  void* sp_ptr = palloc.sp;

  // placement new of context on top of fiber's stack
  WorkerFiberImpl<Fn>* fctx = new (sp_ptr) WorkerFiberImpl<Fn>{
      name, std::move(palloc), std::forward<StackAlloc>(salloc), std::forward<Fn>(fn)};
  return fctx;
}

FiberInterface* FiberActive() noexcept;

inline void FiberInterface::Yield() {
  scheduler_->MarkReady(this);
  scheduler_->Preempt();
}

inline void FiberInterface::WaitUntil(std::chrono::steady_clock::time_point tp) {
  scheduler_->WaitUntil(tp, this);
}

}  // namespace detail

using DispatcherAlgo = std::function<void(detail::Scheduler* sched)>;
void SetCustomDispatcher(DispatcherAlgo algo);

}  // namespace fb2
}  // namespace util

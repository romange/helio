// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#define __FIBERS_SCHEDULER_H__
#include "util/fibers/detail/fiber_interface.h"
#undef __FIBERS_SCHEDULER_H__

namespace util {
namespace fb2 {

class DispatchPolicy;

namespace detail {

// The class that is responsible for fiber management and scheduling.
// It's main loop is in DefaultDispatch() and it runs in the context of Dispatch fiber.
// Optionally, main fiber can override the main scheduling loop with custom one.
// The override call is done via SetCustomDispatcher() and it's called from the main fiber.
class Scheduler {
 public:
  Scheduler(FiberInterface* main);
  ~Scheduler();

  void AddReady(FiberInterface* fibi);

  // ScheduleFromRemote is called from a different thread than the one that runs the scheduler.
  // fibi must exist during the run of this function.
  void ScheduleFromRemote(FiberInterface* fibi);

  void Attach(FiberInterface* fibi);
  void DetachWorker(FiberInterface* cntx);

  void ScheduleTermination(FiberInterface* fibi);

  bool HasReady() const {
    return !ready_queue_.empty();
  }

  ::boost::context::fiber_context Preempt();

  // Returns true if the fiber timed out by reaching tp.
  bool WaitUntil(std::chrono::steady_clock::time_point tp, FiberInterface* me);

  // Assumes HasReady() is true.
  FiberInterface* PopReady() {
    assert(!ready_queue_.empty());
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

  uint32_t num_worker_fibers() const {
    return num_worker_fibers_;
  }

  bool HasSleepingFibers() const {
    return !sleep_queue_.empty();
  }

  std::chrono::steady_clock::time_point NextSleepPoint() const {
    return sleep_queue_.begin()->tp_;
  }

  void DestroyTerminated();

  // Returns whether the active fiber was pulled from the queue and the queue state.
  bool ProcessRemoteReady(FiberInterface* active);

  // Returns number of sleeping fibers being activated from sleep.
  unsigned ProcessSleep();

  void AttachCustomPolicy(DispatchPolicy* policy);

  DispatchPolicy* policy() {
    return custom_policy_;
  }

  void PrintAllFiberStackTraces();
  void ExecuteOnAllFiberStacks(FiberInterface::PrintFn fn);
  void SuspendAndExecuteOnDispatcher(std::function<void()> fn);

  bool RemoteEmpty() const {
    return remote_ready_queue_.Empty();
  }

  size_t worker_stack_size() const {
    return worker_stack_size_;
  }
 private:
  // We use intrusive::list and not slist because slist has O(N) complexity for some operations
  // which may be time consuming for long lists.
  using FI_Queue = boost::intrusive::list<
      FiberInterface,
      boost::intrusive::member_hook<FiberInterface, FI_ListHook, &FiberInterface::list_hook>,
      boost::intrusive::constant_time_size<false>>;

  using FI_List = boost::intrusive::list<
      FiberInterface,
      boost::intrusive::member_hook<FiberInterface, FI_ListHook, &FiberInterface::fibers_hook>,
      boost::intrusive::constant_time_size<false>>;

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
  DispatchPolicy* custom_policy_ = nullptr;

  boost::intrusive_ptr<FiberInterface> dispatch_cntx_;
  FI_Queue ready_queue_, terminate_queue_;
  SleepQueue sleep_queue_;
  base::MPSCIntrusiveQueue<FiberInterface> remote_ready_queue_;
  std::atomic_uint64_t remote_epoch_{0};

  // A list of all fibers in the thread.
  FI_List fibers_;

  bool shutdown_ = false;
  uint32_t num_worker_fibers_ = 0;
  size_t worker_stack_size_ = 0;
};

}  // namespace detail

class DispatchPolicy {
 public:
  virtual ~DispatchPolicy();

  virtual void Run(detail::Scheduler* sched) = 0;
  virtual void Notify() = 0;
};

void SetCustomDispatcher(DispatchPolicy* policy);

}  // namespace fb2
}  // namespace util

#include "util/fibers/detail/fiber_interface_impl.h"

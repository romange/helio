// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/fibers/detail/fiber_interface.h"

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

  void AddReady(FiberInterface* cntx) {
    ready_queue_.push_back(*cntx);
  }

  void ScheduleFromRemote(FiberInterface* cntx);

  void Attach(FiberInterface* cntx);

  void ScheduleTermination(FiberInterface* cntx);

  bool HasReady() const {
    return !ready_queue_.empty();
  }

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

  uint32_t num_worker_fibers() const {
    return num_worker_fibers_;
  }

  void DestroyTerminated();
  void ProcessRemoteReady();
  void ProcessSleep();

  void AttachCustomPolicy(DispatchPolicy* policy);

  DispatchPolicy* policy() {
    return custom_policy_;
  }

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
  DispatchPolicy* custom_policy_ = nullptr;

  boost::intrusive_ptr<FiberInterface> dispatch_cntx_;
  FI_Queue ready_queue_, terminate_queue_;
  SleepQueue sleep_queue_;
  base::MPSCIntrusiveQueue<FiberInterface> remote_ready_queue_;

  bool shutdown_ = false;
  uint32_t num_worker_fibers_ = 0;
};


inline void FiberInterface::Yield() {
  scheduler_->AddReady(this);
  scheduler_->Preempt();
}

inline void FiberInterface::WaitUntil(std::chrono::steady_clock::time_point tp) {
  scheduler_->WaitUntil(tp, this);
}

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

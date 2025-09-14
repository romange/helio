// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <vector>
#define __FIBERS_SCHEDULER_H__
#include "util/fibers/detail/fiber_interface.h"
#undef __FIBERS_SCHEDULER_H__

namespace util {
namespace fb2 {

class DispatchPolicy;

namespace detail {

enum class RunFiberResult {
  EXHAUSTED,   // No more fibers to run.
  HAS_ACTIVE,  // There are still fibers to run.
};

// The class that is responsible for fiber management and scheduling.
// It's main loop is in DefaultDispatch() and it runs in the context of Dispatch fiber.
// Optionally, main fiber can override the main scheduling loop with custom one.
// The override call is done via SetCustomDispatcher() and it's called from the main fiber.
class Scheduler {
 public:
  struct Config {
    uint64_t budget_normal_fib = 1'000'000;   // ns, round_robin_run budget for normal fibers
    uint64_t budget_background_fib = 50'000;  // ns, round_robin_run budget for background fibers
    uint64_t background_sleep_prob = 10;      // [0-100]% probability of sleeps when reaching budget
    uint64_t background_warrant_pct = 10;     // % of minimum cpu time background fibers get
  };

  Scheduler(FiberInterface* main);
  ~Scheduler();

  void AddReady(FiberInterface* fibi, bool to_front = false);

  // ScheduleFromRemote is called from a different thread than the one that runs the scheduler.
  // fibi must exist during the run of this function.
  void ScheduleFromRemote(FiberInterface* fibi);

  void Attach(FiberInterface* fibi);
  void DetachWorker(FiberInterface* cntx);

  void ScheduleTermination(FiberInterface* fibi);

  bool HasReady(FiberPriority p) const {
    return !ready_queue_[GetQueueIndex(p)].empty();
  }

  bool HasReadyAtLeast(FiberPriority p = FiberPriority::BACKGROUND) const {
    // TODO: Move high prioity to 0 index and use enum value based comparison
    return HasReady(FiberPriority::NORMAL) ||
           (p == FiberPriority::BACKGROUND && HasReady(FiberPriority::BACKGROUND));
  }

  bool HasAnyReady() const {
    return HasReadyAtLeast(FiberPriority::BACKGROUND);
  }

  ::boost::context::fiber_context Preempt(bool yield);

  // Returns true if the fiber timed out by reaching tp.
  bool WaitUntil(std::chrono::steady_clock::time_point tp, FiberInterface* me);

  // Assumes HasReady() is true.
  FiberInterface* PopReady(FiberPriority p = FiberPriority::NORMAL) {
    auto idx = GetQueueIndex(p);
    assert(!ready_queue_[idx].empty());
    FiberInterface* res = &ready_queue_[idx].front();
    ready_queue_[idx].pop_front();
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

  // Run fibers from ready queue with given priority baseline
  RunFiberResult Run(FiberPriority baseline);

  void PrintAllFiberStackTraces();
  void ExecuteOnAllFiberStacks(FiberInterface::PrintFn fn);
  void SuspendAndExecuteOnDispatcher(std::function<void()> fn);

  bool RemoteEmpty() const {
    return remote_ready_queue_.Empty();
  }

  size_t worker_stack_size() const {
    return worker_stack_size_;
  }

  // Update config field with provided value
  void UpdateConfig(uint64_t Config::*field, uint64_t value);

 private:
  // Run fibers from ready queue with given priority.
  RunFiberResult RunReadyFibersInternal(FiberPriority priority);

  // For HIGH priority fibers, we still use NORMAL(0) queue.
  static constexpr unsigned GetQueueIndex(FiberPriority prio) {
    return prio == FiberPriority::HIGH ? 0 : unsigned(prio);
  }

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

  struct RuntimeCounter : public std::array<uint64_t /* runtime ns */, 2 /* FiberPrioriry */> {
    using std::array<uint64_t, 2>::operator[];

    uint64_t& operator[](FiberPriority p) {
      return (*this)[static_cast<uint8_t>(p)];
    }

    uint64_t operator[](FiberPriority p) const {
      return (*this)[static_cast<uint8_t>(p)];
    }

    uint64_t total() const {
      return operator[](FiberPriority::NORMAL) + operator[](FiberPriority::BACKGROUND);
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

  Config config_;
  struct {
    uint64_t last_normal_ts = 0;  // last timepoint (ns) when NORMAL priority fiber ran

    uint64_t start_ts = 0;   // timestamp (ns) start of round robin run (set by scheduler)
    uint64_t last_ts = 0;    // timestamp (ns) of last fiber yield
    uint64_t took_ns = 0;    // ns, how much round robin took so far
    uint64_t budget_ns = 0;  // ns, budget for running whole round robin

    uint64_t yields = 0;  // number of yields
  } round_robin_run_;

  RuntimeCounter runtime_ns_;  // total running times of fibers in ns
  FI_Queue ready_queue_[2 /* FiberPriority */], terminate_queue_;
  SleepQueue sleep_queue_;
  base::MPSCIntrusiveQueue<FiberInterface> remote_ready_queue_;

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

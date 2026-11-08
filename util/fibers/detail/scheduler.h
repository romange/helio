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

    // The dispatcher polls for remote wakes, tasks and I/O events between rounds, and a round
    // lasts until every fiber that was ready when it started switches out. A fiber that switches
    // out at least poll_min_cycles after the last poll switches to the dispatcher ahead of its
    // turn (see Preempt()), which ends the round and polls, if:
    //   1. remotely woken fibers or tasks wait, or
    //   2. at least poll_min_events I/O events wait, or
    //   3. any I/O event waits and poll_max_cycles passed since the last poll.
    // Queued submissions are flushed from the current fiber at most once per poll_min_cycles,
    // independently of the I/O event threshold. Completion callbacks still run on the dispatcher.
    // I/O events are counted by the dispatch policy, see DispatchPolicy::GetPendingWork().
    // poll_min_cycles == 0 disables early polling and flushing.
    // Cycles are CycleClock cycles. Default to the --scheduler_poll_* flags.
    uint64_t poll_min_cycles = 0;
    uint64_t poll_max_cycles = 0;
    uint64_t poll_min_events = 0;
  };

  Scheduler(FiberInterface* main);
  ~Scheduler();

  void AddReady(FiberInterface* fibi, bool to_front, uint64_t tsc);

  // ScheduleFromRemote is called from a different thread than the one that runs the scheduler.
  // fibi must exist during the run of this function. With `to_front`, this thread queues fibi
  // ahead of the ready fibers of its priority when it pulls fibi from the remote queue.
  void ScheduleFromRemote(FiberInterface* fibi, bool to_front = false);

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

  // Moves the fibers activated by remote threads to the ready queue. Must be called by the
  // dispatcher.
  void ProcessRemoteReady();

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

  const Config& config() const {
    return config_;
  }

  // Update config field with provided value
  void UpdateConfig(uint64_t Config::* field, uint64_t value);

 private:
  // Run fibers from ready queue with given priority.
  RunFiberResult RunReadyFibersInternal(FiberPriority priority);

  // Flushes submissions if needed and returns whether the dispatcher should run.
  // Refreshes now and runtime accounting after a flush.
  bool ServicePendingWork(uint64_t& now);

  // Called by Preempt(): takes the dispatcher out of the ready queue and switches to it, so it
  // ends the round. See Config::poll_min_cycles.
  ::boost::context::fiber_context EndRoundEarly(uint64_t tsc);

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

  struct RuntimeCounter : public std::array<uint64_t /* runtime cycles */, 2 /* FiberPrioriry */> {
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
    uint64_t last_normal_cycles = 0;  // CycleClock value when NORMAL priority fiber last ran

    uint64_t start_cycles = 0;   // CycleClock at start of round robin run (set by scheduler)
    uint64_t last_cycles = 0;    // CycleClock at last fiber yield
    uint64_t took_cycles = 0;    // cycles, how much round robin took so far
    uint64_t budget_cycles = 0;  // cycles, budget for running whole round robin

    uint32_t yields = 0;     // number of yields
    bool end_round = false;  // set by Preempt() to end the round early, see poll_min_cycles

    // The dispatcher waits for its turn in RunReadyFibersInternal(NORMAL).
    bool in_normal_round = false;
  } round_robin_run_;

  uint64_t last_poll_cycles_ = 0;  // CycleClock of the last ProcessRemoteReady().
  uint64_t last_submit_cycles_ = 0;

  RuntimeCounter runtime_cycles_;  // total running times of fibers in cycles
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

  struct PendingWork {
    bool tasks = false;        // work queued by other threads, such as brief tasks.
    uint32_t submissions = 0;  // queued I/O requests not yet submitted to the kernel.
    uint32_t io_events = 0;    // a lower bound on the number of I/O events that wait.
  };

  // Returns pending submissions and work waiting for the dispatcher. Called by the scheduler
  // while fibers run, see Scheduler::Config::poll_min_cycles, so it must be cheap.
  virtual PendingWork GetPendingWork() {
    return {};
  }

  // Flushes submissions without suspending or running completion callbacks.
  // Returns false if the dispatcher must take over, e.g. to make room in a full completion queue.
  ABSL_MUST_USE_RESULT virtual bool FlushSubmissions() {
    return false;
  }
};

void SetCustomDispatcher(DispatchPolicy* policy);

}  // namespace fb2
}  // namespace util

#include "util/fibers/detail/fiber_interface_impl.h"

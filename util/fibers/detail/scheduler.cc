// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/detail/scheduler.h"

#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "base/cycle_clock.h"
#include "base/logging.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/stacktrace.h"

namespace util {
namespace fb2 {
namespace detail {

namespace ctx = boost::context;

using namespace std;
using base::CycleClock;
using chrono::duration;
using chrono::steady_clock;

namespace {

class DispatcherImpl final : public FiberInterface {
 public:
  DispatcherImpl(ctx::preallocated const& palloc, ctx::fixedsize_stack&& salloc,
                 Scheduler* sched) noexcept;
  ~DispatcherImpl();

  bool is_terminating() const {
    return is_terminating_;
  }

  void Notify() {
    unique_lock<mutex> lk(mu_);
    wake_suspend_ = true;
    cnd_.notify_one();
  }

 private:
  void DefaultDispatch(Scheduler* sched);

  ctx::fiber Run(ctx::fiber&& c);

  bool is_terminating_ = false;

  // This is used to wake up the scheduler from sleep.
  bool wake_suspend_ = false;

  mutex mu_;  // protects wake_suspend_.
  condition_variable cnd_;
};

DispatcherImpl* MakeDispatcher(Scheduler* sched) {
  ctx::fixedsize_stack salloc(1024 * 32);  // 32K stack.
  ctx::stack_context sctx = salloc.allocate();
  ctx::preallocated palloc = MakePreallocated<DispatcherImpl>(sctx);

  void* sp_ptr = palloc.sp;

  // placement new of context on top of fiber's stack
  return new (sp_ptr) DispatcherImpl{std::move(palloc), std::move(salloc), sched};
}

// DispatcherImpl implementation.
DispatcherImpl::DispatcherImpl(const ctx::preallocated& palloc, ctx::fixedsize_stack&& salloc,
                               detail::Scheduler* sched) noexcept
    : FiberInterface{DISPATCH, FiberPriority::NORMAL, 0, "_dispatch"} {
  stack_size_ = palloc.sctx.size;
  entry_ = ctx::fiber(std::allocator_arg, palloc, std::move(salloc),
                      [this](ctx::fiber&& caller) { return Run(std::move(caller)); });
  scheduler_ = sched;
#if defined(BOOST_USE_UCONTEXT)
  entry_ = std::move(entry_).resume();
#endif
}

DispatcherImpl::~DispatcherImpl() {
  DVLOG(1) << "~DispatcherImpl";

  DCHECK(!entry_);
}

ctx::fiber DispatcherImpl::Run(ctx::fiber&& c) {
#if defined(BOOST_USE_UCONTEXT)
  std::move(c).resume();
#else
  DCHECK(!c);
#endif

  if (scheduler_->policy()) {
    scheduler_->policy()->Run(scheduler_);
  } else {
    DefaultDispatch(scheduler_);
  }

  DVLOG(1) << "Dispatcher exiting, switching to main_cntx";
  is_terminating_ = true;

  // Like with worker fibers, we switch to another fiber, but in this case to the main fiber.
  // We will come back here during the deallocation of DispatcherImpl from intrusive_ptr_release
  // in order to return from Run() and come back to main context.
  auto fc = scheduler_->main_context()->SwitchTo();

  DCHECK(fc);  // Should bring us back to main, into intrusive_ptr_release.
  return fc;
}

void DispatcherImpl::DefaultDispatch(Scheduler* sched) {
  DCHECK(FiberActive() == this);

  while (true) {
    if (sched->IsShutdown()) {
      if (sched->num_worker_fibers() == 0)
        break;
    }

    sched->ProcessRemoteReady(nullptr);
    if (sched->HasSleepingFibers()) {
      sched->ProcessSleep();
    }

    if (sched->Run(FiberPriority::NORMAL) == RunFiberResult::HAS_ACTIVE)
      continue;

    if (sched->Run(FiberPriority::BACKGROUND) == RunFiberResult::HAS_ACTIVE)
      continue;

    sched->DestroyTerminated();

    bool has_sleeping = sched->HasSleepingFibers();
    auto cb = [this]() { return wake_suspend_; };

    unique_lock<mutex> lk{mu_};
    DCHECK(!sched->HasReady(FiberPriority::NORMAL) && !sched->HasReady(FiberPriority::BACKGROUND));

    if (has_sleeping) {
      auto next_tp = sched->NextSleepPoint();
      cnd_.wait_until(lk, next_tp, std::move(cb));
    } else {
      if (!sched->IsShutdown())
        cnd_.wait(lk, std::move(cb));
    }
    wake_suspend_ = false;
    detail::ResetFiberRunSeq();
  }
  sched->DestroyTerminated();
}

}  // namespace

Scheduler::Scheduler(FiberInterface* main_cntx) : main_cntx_(main_cntx) {
  DCHECK(!main_cntx->scheduler_);
  main_cntx->scheduler_ = this;
  dispatch_cntx_.reset(MakeDispatcher(this));

  fibers_.push_back(*main_cntx);
  fibers_.push_back(*dispatch_cntx_);
  ready_queue_[GetQueueIndex(FiberPriority::NORMAL)].push_back(*dispatch_cntx_);
}

Scheduler::~Scheduler() {
  shutdown_ = true;
  DCHECK(main_cntx_ == FiberActive());

  for (auto prio : {FiberPriority::NORMAL, FiberPriority::BACKGROUND}) {
    while (HasReady(prio)) {
      FiberInterface* fi = PopReady(prio);
      DCHECK(!fi->sleep_hook.is_linked());
      fi->SwitchTo();
    }
  }

  DispatcherImpl* dimpl = static_cast<DispatcherImpl*>(dispatch_cntx_.get());
  if (!dimpl->is_terminating()) {
    DVLOG(1) << "~Scheduler switching to dispatch " << dispatch_cntx_->IsDefined();
    auto fc = dispatch_cntx_->SwitchTo();
    CHECK(!fc);
    CHECK(dimpl->is_terminating());
  }
  delete custom_policy_;
  custom_policy_ = nullptr;

  if (num_worker_fibers_ != 0) {
    PrintAllFiberStackTraces();
    LOG(FATAL) << "Scheduler is destroyed with " << num_worker_fibers_ << " worker fibers";
  }

  fibers_.erase(fibers_.iterator_to(*dispatch_cntx_));
  fibers_.erase(fibers_.iterator_to(*main_cntx_));

  // destroys the stack and the object via intrusive_ptr_release.
  dispatch_cntx_.reset();
  DestroyTerminated();
}

ctx::fiber_context Scheduler::Preempt(bool yield) {
  auto* active_fiber = FiberActive();
  if (active_fiber == dispatch_cntx_.get()) {
    LOG(DFATAL) << "Should not preempt dispatcher: " << GetStacktrace();
  }

  if (IsFiberAtomicSection()) {
    static int64_t last_ts = 0;
    int64_t now = time(nullptr);
    if (now != last_ts) {  //   once a second at most.
      last_ts = now;
      LOG(DFATAL) << "Preempting inside of atomic section, fiber: " << FiberActive()->name()
                  << ", stacktrace:\n"
                  << GetStacktrace();
    }
  }

  ProactorBase::UpdateMonotonicTime();
  uint64_t last = exchange(round_robin_run_.last_ts, ProactorBase::GetMonotonicTimeNs());
  round_robin_run_.yields += yield ? 1 : 0;
  round_robin_run_.took_ns = round_robin_run_.last_ts - round_robin_run_.start_ts;

  if (active_fiber->Priority() == FiberPriority::BACKGROUND) {
    // Fast path: keep running the active fiber to avoid costly context switch.
    // Other tasks will be run on the next iteration or when this one is done.
    if (yield && round_robin_run_.took_ns < round_robin_run_.budget_ns)
      return {};

    // Fiber ate a big chunk of cpu time
    bool hungry = round_robin_run_.last_ts - last > round_robin_run_.budget_ns;
    // No other normal fibers were present in last 5ms, server is likely (almost) idle
    bool likely_idle = round_robin_run_.last_ts - round_robin_run_.last_normal_ts > 5'000'000;
    // Yield with specified probability (frequency)
    bool should_yield = round_robin_run_.last_ts % 100 < config_.background_sleep_prob;

    // If the fiber was hungry and we potentially disrupted other fibers, put it to sleep.
    // Alternatively sleep every `frequency` time to yield to the OS for long cpu tasks
    if (yield) {
      if (likely_idle && (hungry || should_yield)) {
        uint64_t took = round_robin_run_.last_ts - last;
        uint64_t sleep_ns = std::min<uint64_t>(1'500'000, std::max<uint64_t>(took, 10'000));
        active_fiber->tp_ = steady_clock::now() + duration<uint64_t, std::nano>(sleep_ns);
        sleep_queue_.insert(*active_fiber);
      } else {
        AddReady(active_fiber);
      }
    }

    // Background fibers always return to the dispatcher
    return dispatch_cntx_->SwitchTo();
  }

  if (yield)
    AddReady(active_fiber);

  DCHECK(HasReady(FiberPriority::NORMAL));  // dispatcher fiber is always in the ready queue.
  auto& rq = ready_queue_[GetQueueIndex(FiberPriority::NORMAL)];
  FiberInterface* fi = &rq.front();
  rq.pop_front();
  return fi->SwitchTo();
}

void Scheduler::AddReady(FiberInterface* fibi, bool to_front) {
  DCHECK(!fibi->list_hook.is_linked());
  DVLOG(2) << "Adding " << fibi->name() << " to ready_queue_";

  // We measure runqueue time for fibers that are not active. Yielding fibers are
  // excluded, otherwise they won't be accounted correctly by SwitchSetup when
  // calculating run_cycles.
  if (FiberActive() != fibi) {
    fibi->SetRunQueueStart();
  }

  unsigned q_idx = GetQueueIndex(fibi->prio_);

  if (to_front) {
    ready_queue_[q_idx].push_front(*fibi);
  } else {
    ready_queue_[q_idx].push_back(*fibi);
  }

  FIBER_TRACE(fibi, TRACE_READY);

  // Case of notifications coming to a sleeping fiber.
  if (fibi->sleep_hook.is_linked()) {
    sleep_queue_.erase(sleep_queue_.iterator_to(*fibi));
  }
}

// Is called only from ActivateOther.
void Scheduler::ScheduleFromRemote(FiberInterface* cntx) {
  // This function is called from FiberInterface::ActivateOther from a remote scheduler.
  // But the fiber belongs to this scheduler.
  DCHECK(cntx->scheduler_ == this);

  // If someone else holds the bit - give up on scheduling by this call.
  // This should not happen as ScheduleFromRemote should be called under a WaitQueue lock.
  if ((cntx->flags_.fetch_or(FiberInterface::kScheduleRemote, memory_order_acquire) &
       FiberInterface::kScheduleRemote) != 0) {
    LOG(DFATAL) << "Already scheduled remotely " << cntx->name() << " " << cntx->DEBUG_use_count();
    return;
  }

  if (cntx->IsScheduledRemotely()) {
    // We schedule a fiber remotely only once.
    // This should not happen in general, because we usually schedule a fiber under
    // a spinlock when pulling it from the WaitQueue. However, there are ActivateOther calls
    // that happen due to I/O events that might break this assumption. To see if this happens,
    // I log the case and will investigate if it happens.
    LOG(DFATAL) << "Fiber " << cntx->name() << " is already scheduled remotely";

    // revert the flags.
    cntx->flags_.fetch_and(~FiberInterface::kScheduleRemote, memory_order_release);
  } else {
    remote_ready_queue_.Push(cntx);

    DVLOG(2) << "ScheduleFromRemote " << cntx->name() << " " << cntx->use_count_.load();

    if (custom_policy_) {
      custom_policy_->Notify();
    } else {
      DispatcherImpl* dimpl = static_cast<DispatcherImpl*>(dispatch_cntx_.get());
      dimpl->Notify();
    }
  }
}

void Scheduler::Attach(FiberInterface* cntx) {
  cntx->scheduler_ = this;

  fibers_.push_back(*cntx);

  if (cntx->type() == FiberInterface::WORKER) {
    worker_stack_size_ += cntx->stack_size();
    ++num_worker_fibers_;
  }
}

void Scheduler::DetachWorker(FiberInterface* cntx) {
  fibers_.erase(fibers_.iterator_to(*cntx));
  --num_worker_fibers_;
  worker_stack_size_ -= cntx->stack_size();
}

void Scheduler::ScheduleTermination(FiberInterface* cntx) {
  terminate_queue_.push_back(*cntx);
  if (cntx->type() == FiberInterface::WORKER) {
    --num_worker_fibers_;
    worker_stack_size_ -= cntx->stack_size();
  }
}

void Scheduler::DestroyTerminated() {
  unsigned i = 0;
  while (!terminate_queue_.empty()) {
    FiberInterface* tfi = &terminate_queue_.front();
    terminate_queue_.pop_front();
    DVLOG(2) << "Releasing terminated " << tfi->name_;

    fibers_.erase(fibers_.iterator_to(*tfi));

    // maybe someone holds a Fiber handle and waits for the fiber to join.
    intrusive_ptr_release(tfi);
    ++i;
  }
  if (i > 10) {
    DVLOG(1) << "Destroyed " << i << " fibers";
  }
}

bool Scheduler::WaitUntil(chrono::steady_clock::time_point tp, FiberInterface* me) {
  DCHECK(!me->sleep_hook.is_linked());
  DCHECK(!me->list_hook.is_linked());

  me->tp_ = tp;
  sleep_queue_.insert(*me);
  auto fc = Preempt(false);
  DCHECK(!fc);
  DCHECK(!me->sleep_hook.is_linked());
  bool has_timed_out = (me->tp_ == chrono::steady_clock::time_point::max());

  return has_timed_out;
}

bool Scheduler::ProcessRemoteReady(FiberInterface* active) {
  bool res = false;
  unsigned iteration = 0;

  while (true) {
    auto [fi, qempty] = remote_ready_queue_.PopWeak();
    if (!fi) {
      if (qempty) {
        if (active && !res) {
          // UB state. Try to recover.
          FiberInterface* next = active->remote_next_.load(std::memory_order_acquire);
          bool qempty = remote_ready_queue_.Empty();
          LOG(ERROR) << "Failed to pull active fiber from remote_ready_queue, iteration "
                     << iteration << " remote_empty: " << qempty << ", next:" << (uint64_t)next;
          LOG(ERROR) << "Stacktrace: " << GetStacktrace();
          if (next != (FiberInterface*)FiberInterface::kRemoteFree) {
            if (iteration < 100) {
              // Work around the inconsistency by retrying.
              ++iteration;
              continue;
            }
          }
        }
        break;
      } else {  // qempty == false, retry.
        ++iteration;
        VLOG(1) << "Retrying " << iteration;
        continue;
      }
    }

    // Marks as free.
    fi->remote_next_.store((FiberInterface*)FiberInterface::kRemoteFree, memory_order_relaxed);

    // clear the bit after we pulled from the queue.
    fi->flags_.fetch_and(~FiberInterface::kScheduleRemote, memory_order_release);

    DVLOG(2) << "Pulled " << fi->name() << " " << fi->DEBUG_use_count();

    DCHECK(fi->scheduler_ == this);

    if (fi == active)
      res = true;
    // Generally, fi should not be in the ready queue if it's still in the remote queue,
    // because being in the remote queue means fi is still registered in the wait_queue of
    // some event. However, in case fi is waiting with timeout, ProcessSleep below can not
    // remove fi from the wait_queue and from the remote queue. In that case fi will be put
    // into special transitional state by adding it to ready_queue even though it's still
    // blocked on the wait queue. When fi runs, it first unregisters itself from the
    // wait queue atomically and pulls itself from the remote queue.
    // There is a race condition between ProcessSleep and ProcessRemoteReady,
    // where we can process remote notification before fi run,
    // but after it was added to ready_queue. It's fine though, all we need to check is that
    // fi->list_hook is not already linked.
    //
    // Another corner-case is that ProcessRemoteReady can be called
    // by a FiberInterface::PullMyselfFromRemoteReadyQueue
    // i.e. when fi is already active. In that case we should not add it to the ready queue.
    if (fi != active && !fi->list_hook.is_linked()) {
      DVLOG(2) << "set ready " << fi->name();
      fi->SetRunQueueStart();
      AddReady(fi, fi->prio_ == FiberPriority::HIGH /* to_front */);
    }
  }

  return res;
}

unsigned Scheduler::ProcessSleep() {
  DCHECK(!sleep_queue_.empty());
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  DVLOG(3) << "now " << now.time_since_epoch().count();

  unsigned result = 0;
  do {
    auto it = sleep_queue_.begin();
    if (it->tp_ > now)
      break;

    FiberInterface& fi = *it;
    sleep_queue_.erase(it);

    DCHECK(!fi.list_hook.is_linked());
    fi.tp_ = chrono::steady_clock::time_point::max();  // meaning it has timed out.
    fi.SetRunQueueStart();
    ready_queue_[GetQueueIndex(fi.prio_)].push_back(fi);
    FIBER_TRACE(&fi, TRACE_SLEEP_WAKE);
    ++result;
  } while (!sleep_queue_.empty());

  return result;
}

void Scheduler::AttachCustomPolicy(DispatchPolicy* policy) {
  CHECK(custom_policy_ == nullptr);
  custom_policy_ = policy;
}

void Scheduler::ExecuteOnAllFiberStacks(FiberInterface::PrintFn fn) {
  for (auto& fiber : fibers_) {
    DCHECK(fiber.scheduler() == this);
    fiber.ExecuteOnFiberStack(fn);
  }
}

void Scheduler::SuspendAndExecuteOnDispatcher(std::function<void()> fn) {
  CHECK(FiberActive() != dispatch_cntx_.get());

  // All our dispatch policies add dispatcher to ready queue, hence it must be there.
  CHECK(dispatch_cntx_->list_hook.is_linked());

  constexpr unsigned prio = GetQueueIndex(FiberPriority::NORMAL);
  // We must erase it from the ready queue because we switch to dispatcher "abnormally",
  // not through Preempt().
  ready_queue_[prio].erase(FI_Queue::s_iterator_to(*dispatch_cntx_));

  dispatch_cntx_->SwitchToAndExecute([fn = std::move(fn)] { fn(); });
}

void Scheduler::UpdateConfig(uint64_t Scheduler::Config::* field, uint64_t value) {
  config_.*field = value;
}

void Scheduler::PrintAllFiberStackTraces() {
  auto* active = FiberActive();
  if (!sleep_queue_.empty()) {
    LOG(INFO) << "Sleep queue size " << sleep_queue_.size();
  }
  auto print_fn = [active](FiberInterface* fb) {
    string state = "suspended";
    bool add_time = true;
    if (fb->list_hook.is_linked()) {
      state = "ready";
    } else if (active == fb) {
      state = "active";
    } else if (fb->sleep_hook.is_linked()) {
      state = absl::StrCat("sleeping until ", fb->tp_.time_since_epoch().count(), " now is ",
                           chrono::steady_clock::now().time_since_epoch().count());
      add_time = false;
    }

    string print_cb_str;
#ifndef NDEBUG
    print_cb_str = fb->stacktrace_print_cb_ ? fb->stacktrace_print_cb_() : string{};
#endif

    if (add_time) {
      uint64_t tsc = CycleClock::Now();
      uint64_t delta = tsc - fb->cpu_tsc_;
      uint64_t freq_ms = CycleClock::Frequency() / 1000;
      absl::StrAppend(&state, ":", delta / freq_ms, "ms");
    }

    LOG(INFO) << "------------ Fiber " << fb->name_ << " (" << state << ") ------------\n"
              << print_cb_str << GetStacktrace();
  };

  ExecuteOnAllFiberStacks(print_fn);
}

RunFiberResult Scheduler::Run(FiberPriority priority) {
  // TODO: use c++ 20 using enum
  const uint64_t kBaseSlice = 10'000'000 /* 10 ms */;
  DCHECK(FiberActive() == dispatch_cntx_.get());

  ProactorBase::UpdateMonotonicTime();

  // Reset total runtime every while to restore normal balance.
  // If background fibers took most of the time, punish them by resetting later.
  if (runtime_ns_.total() >= kBaseSlice) {
    if (runtime_ns_[FiberPriority::BACKGROUND] <= runtime_ns_[FiberPriority::NORMAL] ||
        runtime_ns_.total() >= 5 * kBaseSlice) {
      runtime_ns_.fill(0);
    }
  }

  // Proactor should progress on normal priority fibers first
  DCHECK(priority == FiberPriority::NORMAL || !HasReady(FiberPriority::NORMAL));

  if (HasReady(priority))
    RunReadyFibersInternal(priority);

  // If background fibers are below their warrant, let them run
  if (HasReady(FiberPriority::BACKGROUND) && priority == FiberPriority::NORMAL) {
    uint64_t warrant = runtime_ns_.total() * config_.background_warrant_pct / 100;
    if (runtime_ns_[FiberPriority::NORMAL] > 0 && runtime_ns_[FiberPriority::BACKGROUND] <= warrant)
      RunReadyFibersInternal(FiberPriority::BACKGROUND);
  }

  // Fibers can wake each other, so we have to check all fibers of equal or higher priority
  return HasReadyAtLeast(priority) ? RunFiberResult::HAS_ACTIVE : RunFiberResult::EXHAUSTED;
}

RunFiberResult Scheduler::RunReadyFibersInternal(FiberPriority priority) {
  DCHECK(HasReady(priority));
  const uint64_t Config::* kBudgets[2] = {&Config::budget_normal_fib,
                                          &Config::budget_background_fib};

  ProactorBase::UpdateMonotonicTime();
  round_robin_run_.took_ns = 0;
  round_robin_run_.start_ts = round_robin_run_.last_ts = ProactorBase::GetMonotonicTimeNs();
  round_robin_run_.budget_ns = config_.*kBudgets[static_cast<unsigned>(priority)];
  round_robin_run_.yields = 0;

  // Normal fibers are run in round robin fashion with the dispatch fiber being among the round.
  // Yields cause it to stop running even with leftover budget to return to processing io events.
  // Background fibers are run until the budget allows it. Yield instructions don't even cause
  // fibers to switch to avoid context switch costs.
  do {
    FiberInterface* fi = PopReady(priority);
    DCHECK(!fi->list_hook.is_linked());
    DCHECK(!fi->sleep_hook.is_linked());

    // Run round robin only with normal priority, returing to the dispatcher after every round.
    // Background fibers switch back directly to dispatcher
    if (priority == FiberPriority::NORMAL)
      AddReady(dispatch_cntx_.get());

    DVLOG(2) << "Switching to " << fi->name();

    // Switch to fiber, next logic is determined by Preempt()
    fi->SwitchTo();

    // It could be that we iterate over multiple fibers and meanwhile we postpone the execution of
    // brief tasks or do not handle I/O events. Therefore we limit the time spent in the loop
    // to kMaxTimeSpentNs.
    if (round_robin_run_.took_ns > round_robin_run_.budget_ns) {
      break;
    }

    // Background fibers yield as often as possible to be cooperative, so break only for normal ones
    if (priority == FiberPriority::NORMAL && round_robin_run_.yields > 0)
      break;

    // We may need to break here even if ready_queue_ is not empty if one of the fibers yielded,
    // because otherwise we may end up in a busy loop with a fiber keeps yielding and we never
    // return to the dispatcher.
  } while (HasReady(priority));

  runtime_ns_[static_cast<size_t>(priority)] += round_robin_run_.took_ns;
  if (priority == FiberPriority::NORMAL)
    round_robin_run_.last_normal_ts = round_robin_run_.last_ts;

  return HasReady(priority) ? RunFiberResult::HAS_ACTIVE : RunFiberResult::EXHAUSTED;
}

}  // namespace detail

DispatchPolicy::~DispatchPolicy() {
}

}  // namespace fb2
}  // namespace util

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
#include "base/flags.h"
#include "base/logging.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/stacktrace.h"

ABSL_FLAG(uint32_t, scheduler_poll_min_usec, 0,
          "If greater than 0, a fiber that switches out at least this many usec after the "
          "dispatcher last polled switches to the dispatcher, which ends the round and polls, if "
          "remote wakes or tasks wait, or if enough I/O events wait (see "
          "scheduler_poll_min_events and scheduler_poll_max_usec). 0 disables it.");
ABSL_FLAG(uint32_t, scheduler_poll_max_usec, 200,
          "With scheduler_poll_min_usec, a single I/O event ends the round once this many usec "
          "passed since the dispatcher last polled.");
ABSL_FLAG(uint32_t, scheduler_poll_min_events, 16,
          "With scheduler_poll_min_usec, this many I/O events end the round once "
          "scheduler_poll_min_usec passed since the dispatcher last polled.");

namespace util {
namespace fb2 {
namespace detail {

namespace ctx = boost::context;

using namespace std;
using base::CycleClock;
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

  // Runs `publish` and wakes the dispatcher under `mu_`. Once `publish` makes a fiber runnable,
  // this thread may run it and exit; ~DispatcherImpl() takes `mu_` before destroying it, so the
  // thread cannot release the dispatcher until the remote caller leaves Notify().
  template <typename F> void Notify(F&& publish) {
    unique_lock<mutex> lk(mu_);
    publish();
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

std::string DispatcherName() {
  ProactorBase* proactor = ProactorBase::me();
  if (proactor == nullptr)
    return "_dispatch";

  return absl::StrCat("_dispatch_p", proactor->GetPoolIndex());
}

// DispatcherImpl implementation.
DispatcherImpl::DispatcherImpl(const ctx::preallocated& palloc, ctx::fixedsize_stack&& salloc,
                               detail::Scheduler* sched) noexcept
    : FiberInterface{DISPATCH, FiberPriority::NORMAL, 0, DispatcherName()} {
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

  // Wait for a remote Notify() that may still hold `mu_`.
  lock_guard<mutex> lk(mu_);
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

    sched->ProcessRemoteReady();
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

  // Before CycleClock::InitOnce(), this converts to 0, which disables early polls.
  config_.poll_min_cycles = CycleClock::FromUsec(absl::GetFlag(FLAGS_scheduler_poll_min_usec));
  config_.poll_max_cycles = CycleClock::FromUsec(absl::GetFlag(FLAGS_scheduler_poll_max_usec));
  config_.poll_min_events = absl::GetFlag(FLAGS_scheduler_poll_min_events);
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

  uint64_t last = exchange(round_robin_run_.last_cycles, base::CycleClock::Now());
  round_robin_run_.yields += yield ? 1 : 0;
  round_robin_run_.took_cycles = round_robin_run_.last_cycles - round_robin_run_.start_cycles;

  if (active_fiber->Priority() == FiberPriority::BACKGROUND) {
    // Fast path: keep running the active fiber to avoid costly context switch.
    // Other tasks will be run on the next iteration or when this one is done.
    if (yield && round_robin_run_.took_cycles < round_robin_run_.budget_cycles)
      return {};

    // Fiber ate a big chunk of cpu time
    bool hungry = round_robin_run_.last_cycles - last > round_robin_run_.budget_cycles;
    // No other normal fibers were present in last 5ms, server is likely (almost) idle
    bool likely_idle = round_robin_run_.last_cycles - round_robin_run_.last_normal_cycles >
                       base::CycleClock::FromUsec(5'000);
    // Yield with specified probability (frequency)
    bool should_yield = round_robin_run_.last_cycles % 100 < config_.background_sleep_prob;

    // If the fiber was hungry and we potentially disrupted other fibers, put it to sleep.
    // Alternatively sleep every `frequency` time to yield to the OS for long cpu tasks
    if (yield) {
      uint64_t sleep_us = 0;
      if (likely_idle && (hungry || should_yield)) {
        uint64_t took_us = CycleClock::ToUsec(round_robin_run_.last_cycles - last);
        sleep_us = std::min<uint64_t>(1'500, took_us);
      }
      if (sleep_us > 0) {
        active_fiber->tp_ = steady_clock::now() + chrono::microseconds(sleep_us);
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

  // The dispatcher waits for its turn in RunReadyFibersInternal(), so the round is ours to end.
  if (round_robin_run_.in_normal_round) {
    // Switch to the dispatcher ahead of its turn, so it ends the round: its loop pulls remotely
    // woken fibers and processes I/O before it starts the next round.
    if (ShouldPollEarly(round_robin_run_.last_cycles))
      return EndRoundEarly();
    SkipDispatcherTurn();
  }

  auto& rq = ready_queue_[GetQueueIndex(FiberPriority::NORMAL)];
  FiberInterface* fi = &rq.front();
  rq.pop_front();
  return fi->SwitchTo();
}

// EndRoundEarly() and SkipDispatcherTurn() are separate functions, because Preempt() runs on the
// stack of every fiber that switches out, and in unoptimized sanitizer builds their code adds
// hundreds of bytes to its frame.
ctx::fiber_context Scheduler::EndRoundEarly() {
  FiberInterface* dispatcher = dispatch_cntx_.get();
  DCHECK(dispatcher->list_hook.is_linked());
  auto& rq = ready_queue_[GetQueueIndex(FiberPriority::NORMAL)];
  rq.erase(rq.iterator_to(*dispatcher));
  round_robin_run_.end_round = true;
  return dispatcher->SwitchTo();
}

// On its turn, the dispatcher would only start the next round with the fibers that became ready
// during this one, see RunReadyFibersInternal(). Moving it behind them starts that round here,
// which saves switching to the dispatcher and back.
void Scheduler::SkipDispatcherTurn() {
  auto& rq = ready_queue_[GetQueueIndex(FiberPriority::NORMAL)];
  FiberInterface* dispatcher = dispatch_cntx_.get();
  if (&rq.front() == dispatcher && &rq.back() != dispatcher && round_robin_run_.yields == 0 &&
      round_robin_run_.took_cycles <= round_robin_run_.budget_cycles) {
    rq.pop_front();
    rq.push_back(*dispatcher);
  }
}

void Scheduler::AddReady(FiberInterface* fibi, bool to_front) {
  DCHECK(!fibi->list_hook.is_linked());
  DVLOG(2) << "Adding " << fibi->name() << " to ready_queue_";

  unsigned q_idx = GetQueueIndex(fibi->prio_);

  fibi->SetReadyStart();
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
void Scheduler::ScheduleFromRemote(FiberInterface* cntx, bool to_front) {
  // This function is called from FiberInterface::ActivateOther from a remote scheduler.
  // But the fiber belongs to this scheduler.
  DCHECK(cntx->scheduler_ == this);

  // kScheduleRemote is held from here until ProcessRemoteReady() pulls `cntx` from the queue, so
  // `cntx` is never in the queue while the bit is clear. A waiter is detached and activated
  // exactly once, and a fiber that resumes on its own first waits for the activation it is
  // owed (see FiberInterface::AwaitRemoteWake), so the bit can not be set here. We still check
  // it because pushing `cntx` twice would corrupt the intrusive queue.
  // kWakeToFront is set together with kScheduleRemote, before the push, because `cntx` must not
  // be accessed after the push. ProcessRemoteReady() clears both.
  const uint16_t bits =
      FiberInterface::kScheduleRemote | (to_front ? FiberInterface::kWakeToFront : 0);
  if ((cntx->flags_.fetch_or(bits, memory_order_acquire) & FiberInterface::kScheduleRemote) != 0) {
    LOG(DFATAL) << "Already scheduled remotely " << cntx->name() << " " << cntx->DEBUG_use_count();
    return;
  }

  // Once pushed, `cntx` may run to completion on its own thread before Notify() returns, so
  // it must not be accessed after Push().
  DVLOG(2) << "ScheduleFromRemote " << cntx->name() << " " << cntx->use_count_.load();

  if (DispatchPolicy* policy = custom_policy_) {
    remote_ready_queue_.Push(cntx);

    // Unlike the default dispatcher, nothing here keeps `policy` alive if this thread runs
    // `cntx` and exits. Proactor threads must outlive the remote wakers that target them.
    policy->Notify();
  } else {
    // A thread with the default dispatcher may exit as soon as `cntx` runs. Push under the
    // dispatcher mutex, which the exiting thread takes before destroying the dispatcher.
    static_cast<DispatcherImpl*>(dispatch_cntx_.get())->Notify([&] {
      remote_ready_queue_.Push(cntx);
    });
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

  // End a claimable wait. If it timed out, a notifier may still find our waiter until the caller
  // unlinks it, and skips it since the wait is no longer claimable.
  me->wait_claim_.store(FiberInterface::kNoClaim, memory_order_relaxed);
  bool has_timed_out = (me->tp_ == chrono::steady_clock::time_point::max());

  return has_timed_out;
}

bool Scheduler::ShouldPollEarly(uint64_t now) {
  if (config_.poll_min_cycles == 0)
    return false;

  uint64_t since_poll = now - last_poll_cycles_;
  if (since_poll < config_.poll_min_cycles)
    return false;

  if (!remote_ready_queue_.Empty())
    return true;

  if (custom_policy_ == nullptr)
    return false;

  // A poll may enter the kernel and splits batches of I/O events, so a few pending I/O events
  // wait until poll_max_cycles passed.
  DispatchPolicy::PendingWork pending = custom_policy_->GetPendingWork();
  if (pending.tasks)
    return true;

  return pending.io_events > 0 &&
         (pending.io_events >= config_.poll_min_events || since_poll >= config_.poll_max_cycles);
}

void Scheduler::ProcessRemoteReady() {
  if (config_.poll_min_cycles)
    last_poll_cycles_ = CycleClock::Now();
  unsigned iteration = 0;

  while (true) {
    auto [fi, qempty] = remote_ready_queue_.PopWeak();
    if (!fi) {
      if (qempty)
        break;

      // A producer is in the middle of Push(), retry.
      ++iteration;
      VLOG(1) << "Retrying " << iteration;
      continue;
    }

    // Marks as free.
    fi->remote_next_.store((FiberInterface*)FiberInterface::kRemoteFree, memory_order_relaxed);

    // clear the bits after we pulled from the queue.
    uint16_t prev_flags = fi->flags_.fetch_and(
        ~(FiberInterface::kScheduleRemote | FiberInterface::kWakeToFront), memory_order_release);
    bool to_front = (prev_flags & FiberInterface::kWakeToFront) || fi->prio_ == FiberPriority::HIGH;

    DVLOG(2) << "Pulled " << fi->name() << " " << fi->DEBUG_use_count();

    DCHECK(fi->scheduler_ == this);

    // We run on the dispatcher, so `fi` is not running. If `fi` waited with a timeout,
    // ProcessSleep() could have added it to the ready queue already, and this activation is
    // absorbed. Otherwise `fi` is suspended in its wait or in FiberInterface::AwaitRemoteWake().
    if (!fi->list_hook.is_linked()) {
      DVLOG(2) << "set ready " << fi->name();
      AddReady(fi, to_front);
    }
  }
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

    if (!fi.ClaimWaitForTimeout())
      continue;

    fi.tp_ = chrono::steady_clock::time_point::max();  // meaning it has timed out.
    fi.SetReadyStart();
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
      if (fb->flags_.load(std::memory_order_relaxed) & FiberInterface::kTerminatedBit) {
        state = "terminating";
      } else {
        state = "ready";
      }
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
      // Last time of context switch
      uint64_t delta = tsc - fb->cpu_tsc_;
      uint64_t freq_ms = CycleClock::Frequency() / 1000;
      // Last time the fiber got into the ready queue
      uint64_t last_ready = tsc - fb->ready_tsc_;
      absl::StrAppend(&state, ":last_context_switch=", delta / freq_ms,
                      "ms:last_ready=", last_ready / freq_ms, "ms");
    }

    LOG(INFO) << "------------ Fiber " << fb->name_ << " (" << state << ") ------------\n"
              << print_cb_str << GetStacktrace();
  };

  ExecuteOnAllFiberStacks(print_fn);
}

RunFiberResult Scheduler::Run(FiberPriority priority) {
  // TODO: use c++ 20 using enum
  const uint64_t kBaseSliceCycles = base::CycleClock::FromUsec(10'000) /* 10 ms */;

  // Reset total runtime every while to restore normal balance.
  // If background fibers took most of the time, punish them by resetting later.
  if (runtime_cycles_.total() >= kBaseSliceCycles) {
    if (runtime_cycles_[FiberPriority::BACKGROUND] <= runtime_cycles_[FiberPriority::NORMAL] ||
        runtime_cycles_.total() >= 5 * kBaseSliceCycles) {
      runtime_cycles_.fill(0);
    }
  }

  // Proactor should progress on normal priority fibers first
  DCHECK(priority == FiberPriority::NORMAL || !HasReady(FiberPriority::NORMAL));

  if (HasReady(priority))
    RunReadyFibersInternal(priority);

  // If background fibers are below their warrant, let them run
  if (HasReady(FiberPriority::BACKGROUND) && priority == FiberPriority::NORMAL) {
    uint64_t warrant = runtime_cycles_.total() * config_.background_warrant_pct / 100;
    if (runtime_cycles_[FiberPriority::NORMAL] > 0 &&
        runtime_cycles_[FiberPriority::BACKGROUND] <= warrant)
      RunReadyFibersInternal(FiberPriority::BACKGROUND);
  }

  // Fibers can wake each other, so we have to check all fibers of equal or higher priority
  return HasReadyAtLeast(priority) ? RunFiberResult::HAS_ACTIVE : RunFiberResult::EXHAUSTED;
}

RunFiberResult Scheduler::RunReadyFibersInternal(FiberPriority priority) {
  DCHECK(HasReady(priority));
  const uint64_t Config::* kBudgets[2] = {&Config::budget_normal_fib,
                                          &Config::budget_background_fib};

  round_robin_run_.took_cycles = 0;
  round_robin_run_.start_cycles = round_robin_run_.last_cycles = base::CycleClock::Now();
  // Config budgets are stored in nanoseconds.
  round_robin_run_.budget_cycles =
      base::CycleClock::FromUsec((config_.*kBudgets[static_cast<unsigned>(priority)]) / 1000);
  round_robin_run_.yields = 0;
  round_robin_run_.end_round = false;
  round_robin_run_.in_normal_round = priority == FiberPriority::NORMAL;

  // Normal fibers are run in round robin fashion with the dispatch fiber being among the round.
  // Yields cause it to stop running even with leftover budget to return to processing io events.
  // Preempt() starts the next round itself when the loop below would just continue.
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

    // Preempt() switched here ahead of our turn because work waits, see ShouldPollEarly().
    if (round_robin_run_.end_round)
      break;

    // It could be that we iterate over multiple fibers and meanwhile we postpone the execution of
    // brief tasks or do not handle I/O events. Therefore we limit the time spent in the loop
    // to kMaxTimeSpentNs.
    if (round_robin_run_.took_cycles > round_robin_run_.budget_cycles) {
      break;
    }

    // Background fibers yield as often as possible to be cooperative, so break only for normal ones
    if (priority == FiberPriority::NORMAL && round_robin_run_.yields > 0)
      break;

    // We may need to break here even if ready_queue_ is not empty if one of the fibers yielded,
    // because otherwise we may end up in a busy loop with a fiber keeps yielding and we never
    // return to the dispatcher.
  } while (HasReady(priority));
  round_robin_run_.in_normal_round = false;

  runtime_cycles_[static_cast<size_t>(priority)] += round_robin_run_.took_cycles;
  if (priority == FiberPriority::NORMAL)
    round_robin_run_.last_normal_cycles = round_robin_run_.last_cycles;

  return HasReady(priority) ? RunFiberResult::HAS_ACTIVE : RunFiberResult::EXHAUSTED;
}

}  // namespace detail

DispatchPolicy::~DispatchPolicy() {
}

}  // namespace fb2
}  // namespace util

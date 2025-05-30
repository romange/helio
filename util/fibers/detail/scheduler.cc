// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/detail/scheduler.h"

#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>

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

    if (sched->HasReady()) {
      FiberInterface* fi = sched->PopReady();
      DCHECK(!fi->list_hook.is_linked());
      DCHECK(!fi->sleep_hook.is_linked());
      sched->AddReady(this);

      DVLOG(2) << "Switching to " << fi->name();

      fi->SwitchTo();
      DCHECK(!list_hook.is_linked());
      DCHECK(FiberActive() == this);
    } else {
      sched->DestroyTerminated();

      bool has_sleeping = sched->HasSleepingFibers();
      auto cb = [this]() { return wake_suspend_; };

      unique_lock<mutex> lk{mu_};
      if (has_sleeping) {
        auto next_tp = sched->NextSleepPoint();
        cnd_.wait_until(lk, next_tp, std::move(cb));
      } else {
        cnd_.wait(lk, std::move(cb));
      }
      wake_suspend_ = false;
    }
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
  AddReady(dispatch_cntx_.get());
}

Scheduler::~Scheduler() {
  shutdown_ = true;
  DCHECK(main_cntx_ == FiberActive());

  while (HasReady()) {
    FiberInterface* fi = PopReady();
    DCHECK(!fi->sleep_hook.is_linked());
    fi->SwitchTo();
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

void Scheduler::Yield(FiberInterface* me) {
  AddReady(me);
  was_yield_ = true;
  Preempt();
}

ctx::fiber_context Scheduler::Preempt() {
  if (FiberActive() == dispatch_cntx_.get()) {
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

  DCHECK(!ready_queue_.empty());  // dispatcher fiber is always in the ready queue.

  FiberInterface* fi = &ready_queue_.front();
  ready_queue_.pop_front();

  return fi->SwitchTo();
}

void Scheduler::AddReady(FiberInterface* fibi) {
  DCHECK(!fibi->list_hook.is_linked());
  DVLOG(2) << "Adding " << fibi->name() << " to ready_queue_";

  fibi->cpu_tsc_ = CycleClock::Now();
  ready_queue_.push_back(*fibi);
  fibi->trace_ = FiberInterface::TRACE_READY;

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
  auto fc = Preempt();
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
      AddReady(fi);
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
    fi.cpu_tsc_ = CycleClock::Now();
    ready_queue_.push_back(fi);
    fi.trace_ = FiberInterface::TRACE_SLEEP_WAKE;
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

  // We must erase it from the ready queue because we switch to dispatcher "abnormally",
  // not through Preempt().
  ready_queue_.erase(FI_Queue::s_iterator_to(*dispatch_cntx_));

  dispatch_cntx_->SwitchToAndExecute([fn = std::move(fn)] { fn(); });
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
      absl::StrAppend(&state, ":", delta / freq_ms , "ms");
    }

    LOG(INFO) << "------------ Fiber " << fb->name_ << " (" << state << ") ------------\n"
              << print_cb_str << GetStacktrace();
  };

  ExecuteOnAllFiberStacks(print_fn);
}

bool Scheduler::RunWorkerFibersStepImpl() {
  DCHECK(!ready_queue_.empty());

  FiberInterface* fi = PopReady();
  DCHECK(!fi->list_hook.is_linked());
  DCHECK(!fi->sleep_hook.is_linked());
  AddReady(dispatch_cntx_.get());

  DVLOG(2) << "Switching to " << fi->name();
  ProactorBase::UpdateMonotonicTime();
  fi->SwitchTo();

  was_yield_ = false;

  // We are back to the dispatcher, return true if there are no ready fibers.
  return !HasReady();
}

}  // namespace detail

DispatchPolicy::~DispatchPolicy() {
}

}  // namespace fb2
}  // namespace util

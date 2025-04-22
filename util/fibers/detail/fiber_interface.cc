// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/detail/fiber_interface.h"

#include <absl/time/clock.h>

#include <mutex>  // for g_scheduler_lock

#include "base/cycle_clock.h"
#include "base/flags.h"
#include "base/logging.h"
#include "util/fibers/detail/scheduler.h"

ABSL_FLAG(uint32_t, fiber_safety_margin, 0,
          "If > 0, ensures the stack each fiber has at least this margin. "
          "The check is done at fiber destruction time.");

namespace util {
namespace fb2 {
using namespace std;
using base::CycleClock;

namespace detail {
namespace ctx = boost::context;

struct TL_FiberInitializer;

namespace {

[[maybe_unused]] size_t kSzFiberInterface = sizeof(FiberInterface);

inline void CpuPause() {
#if defined(__i386__) || defined(__amd64__)
  __asm__ __volatile__("pause");
#elif defined(__aarch64__)
  /* Use an isb here as we've found it's much closer in duration to
   * the x86 pause instruction vs. yield which is a nop and thus the
   * loop count is lower and the interconnect gets a lot more traffic
   * from loading the ticket above. */
  __asm__ __volatile__("isb");
#endif
}

// Serves as a stub Fiber since it does not allocate any stack.
// It's used as a main fiber of the thread.
class MainFiberImpl final : public FiberInterface {
 public:
  MainFiberImpl() noexcept : FiberInterface{MAIN, FiberPriority::NORMAL, 1, "main"} {
  }

  ~MainFiberImpl() {
    use_count_.fetch_sub(1, memory_order_relaxed);
  }

 protected:
  void Terminate() {
  }
};

mutex g_scheduler_lock;

TL_FiberInitializer* g_fiber_thread_list = nullptr;
uint64_t g_tsc_cycles_per_ms = 0;

}  // namespace

PMR_NS::memory_resource* default_stack_resource = nullptr;
size_t default_stack_size = 64 * 1024;


__thread FiberInterface::TL FiberInterface::tl;

// Per thread initialization structure.
struct TL_FiberInitializer {
  TL_FiberInitializer* next = nullptr;

  // Currently active fiber.
  FiberInterface* active;

  // Per-thread scheduler instance.
  // Allows overriding the main dispatch loop
  Scheduler* sched;
  uint64_t switch_delay_cycles = 0;  // switch delay in cycles.

  // Tracks fiber runtimes that took longer than 1ms.
  uint64_t long_runtime_cnt = 0;
  uint64_t long_runtime_usec = 0;

  uint32_t atomic_section = 0;

  TL_FiberInitializer(const TL_FiberInitializer&) = delete;

  TL_FiberInitializer() noexcept;

  ~TL_FiberInitializer();
};

TL_FiberInitializer::TL_FiberInitializer() noexcept : sched(nullptr) {
  DVLOG(1) << "Initializing FiberLib";

  // main fiber context of this thread.
  // We use it as a stub
  FiberInterface* main_ctx = new MainFiberImpl{};
  active = main_ctx;
  sched = new Scheduler(main_ctx);

  unique_lock lk(g_scheduler_lock);

  next = g_fiber_thread_list;
  g_fiber_thread_list = this;
  if (g_tsc_cycles_per_ms == 0) {
    g_tsc_cycles_per_ms = CycleClock::Frequency() / 1000;
    VLOG(1) << "TSC Frequency : " << g_tsc_cycles_per_ms << "/ms";
  }
}

TL_FiberInitializer::~TL_FiberInitializer() {
  FiberInterface* main_cntx = sched->main_context();

  // If main_cntx != active it means we are exiting via unexpected route (exit, abort, etc).
  // Do not bother with orderly clean-up of the fibers since they can just block on events
  // that will never happen.
  if (main_cntx == active) {
    delete sched;
    delete main_cntx;
  }

  unique_lock lk(g_scheduler_lock);
  TL_FiberInitializer** p = &g_fiber_thread_list;
  while (*p != this) {
    p = &(*p)->next;
  }
  *p = next;
}

TL_FiberInitializer& FbInitializer() noexcept {
  // initialized the first time control passes; per thread
  thread_local static TL_FiberInitializer fb_initializer;
  return fb_initializer;
}

FiberInterface* FiberActive() noexcept {
  return FbInitializer().active;
}

FiberInterface::FiberInterface(Type type, FiberPriority prio, uint32_t cnt, string_view nm)
    : use_count_(cnt), type_(type), prio_(prio) {
  remote_next_.store((FiberInterface*)kRemoteFree, memory_order_relaxed);
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  name_[len] = 0;
  if (len) {
    memcpy(name_, nm.data(), len);
  }
  cpu_tsc_ = CycleClock::Now();
}

FiberInterface::~FiberInterface() {
  DVLOG(2) << "Destroying " << name_;
  DCHECK_EQ(use_count_.load(), 0u);
  DCHECK(join_q_.empty());
  DCHECK(!list_hook.is_linked());
}

void FiberInterface::SetName(std::string_view nm) {
  if (nm.empty())
    return;
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  memcpy(name_, nm.data(), len);
  name_[len] = 0;
}

// We can not destroy this instance within the context of the fiber it's been running in.
// The reason: the instance is hosted within the stack region of the fiber itself, and it
// implicitly destroys the stack when destroying its 'entry_' member variable.
// Therefore, to destroy a FiberInterface (WORKER) object, we must call intrusive_ptr_release
// from another fiber. intrusive_ptr_release is smart about how it releases resources too.
ctx::fiber_context FiberInterface::Terminate() {
  DCHECK(this == FiberActive());
  DCHECK(!list_hook.is_linked());

  CheckStackMargin();
  scheduler_->ScheduleTermination(this);
  DVLOG(2) << "Terminating " << name_;

  while (true) {
    // We signal that the fiber is being terminated by setting the kTerminatedBit flag.
    // We also set the kBusyBit flag to try to acquire the lock.
    uint16_t fprev = flags_.fetch_or(kTerminatedBit | kBusyBit, memory_order_acquire);
    if ((fprev & kBusyBit) == 0) {  // has been acquired
      break;
    }
    CpuPause();
  }
  trace_ = TRACE_TERMINATE;
  join_q_.NotifyAll(this);

  flags_.fetch_and(~kBusyBit, memory_order_release);

  // usually Preempt returns empty fc but here we return the value of where
  // to switch to when this fiber completes. See intrusive_ptr_release for more info.
  return scheduler_->Preempt();
}

size_t FiberInterface::GetStackMargin(const void* stack_address) const {
  return static_cast<const uint8_t*>(stack_address) - stack_bottom_;
}

void FiberInterface::CheckStackMargin() {
  uint32_t check_margin = absl::GetFlag(FLAGS_fiber_safety_margin);
  if (check_margin == 0)
    return;

  const uint8_t* ptr = stack_bottom_;
  while (*ptr == 0xAB) {
    ++ptr;
  }
  uint32_t margin = ptr - stack_bottom_;

  CHECK_GE(margin, check_margin) << "Low stack margin for " << name_;

  // Log if margins are within the the orange zone.
  LOG_IF(INFO, margin < check_margin * 1.5)
      << "Stack margin for " << name_ << ": " << margin << " bytes";
}

uint64_t FiberInterface::GetRunningTimeCycles() const {
  uint64_t now = CycleClock::Now();
  return now > cpu_tsc_ ? now - cpu_tsc_ : 0;
}

void FiberInterface::InitStackBottom(uint8_t* stack_bottom, uint32_t stack_size) {
  stack_bottom_ = stack_bottom;

  if (absl::GetFlag(FLAGS_fiber_safety_margin) > 0) {
    memset(stack_bottom_, 0xAB, stack_size);
  }
}

void FiberInterface::Start(Launch launch) {
  auto& fb_init = detail::FbInitializer();
  fb_init.sched->Attach(this);

  switch (launch) {
    case Launch::post:
      // Activate but do not switch to it.
      fb_init.sched->AddReady(this);
      break;
    case Launch::dispatch:
      // Add the active fiber to the ready queue and switch to the new fiber.
      fb_init.sched->AddReady(fb_init.active);
      {
        auto fc = SwitchTo();
        DCHECK(!fc);
      }
      break;
  }
}

void FiberInterface::Join() {
  FiberInterface* active = FiberActive();

  // We suspend the current fiber and add it to the wait queue of the fiber we are joining on.
  CHECK(active != this);

  while (true) {
    uint16_t fprev = flags_.fetch_or(kBusyBit, memory_order_acquire);
    if (fprev & kTerminatedBit) {     // The fiber is in process of being terminated.
      if ((fprev & kBusyBit) == 0) {  // Caller became the owner.
        flags_.fetch_and(~kBusyBit, memory_order_relaxed);  // release the lock
      }
      return;
    }

    if ((fprev & kBusyBit) == 0) {  // Caller became the owner.
      break;
    }
    CpuPause();
  }

  Waiter waiter{active->CreateWaiter()};
  join_q_.Link(&waiter);
  flags_.fetch_and(~kBusyBit, memory_order_release);  // release the lock
  DVLOG(2) << "Joining on " << name_;

  active->Suspend();
  DCHECK(!waiter.IsLinked());
}

void FiberInterface::ActivateOther(FiberInterface* other) {
  DCHECK(other->scheduler_);

  // Check first if we the fiber belongs to the active thread.
  if (other->scheduler_ == scheduler_) {
    DVLOG(1) << "Activating " << other->name() << " from " << this->name();
    DCHECK_EQ(other->flags_.load(std::memory_order_relaxed) & kTerminatedBit, 0);

    // In case `other` times out on wait, it could be added to the ready queue already by
    // ProcessSleep.
    if (!other->list_hook.is_linked())
      scheduler_->AddReady(other);
  } else {
    // The fiber belongs to another thread. We need to schedule it on that thread.
    // Note, that in this case it is assumed that ActivateOther was called by WaitQueue
    // that is under a lock, and it's guaranteed that `other` is alive during the
    // ScheduleFromRemote() call.
    other->scheduler_->ScheduleFromRemote(other);
  }
}

void FiberInterface::DetachScheduler() {
  scheduler_->DetachWorker(this);
  scheduler_ = nullptr;
}

void FiberInterface::AttachScheduler() {
  scheduler_ = detail::FbInitializer().sched;
  scheduler_->Attach(this);
  scheduler_->AddReady(this);
}

ctx::fiber_context FiberInterface::SwitchTo() {
  DCHECK(entry_) << name();
  FiberInterface* prev = SwitchSetup();

  // pass pointer to the context that resumes `this`
  return std::move(entry_).resume_with([prev](ctx::fiber_context&& c) {
    DCHECK(!prev->entry_);

    prev->entry_ = std::move(c);  // update the return address in the context we just switch from.
    return ctx::fiber_context{};
  });
}

void FiberInterface::SwitchToAndExecute(std::function<void()> fn) {
  FiberInterface* prev = SwitchSetup();

  // pass pointer to the context that resumes `this`
  std::move(entry_).resume_with([prev, fn = std::move(fn)](ctx::fiber_context&& c) {
    DCHECK(!prev->entry_ && c);
    prev->entry_ = std::move(c);  // update the return address in the context we just switch from.
    fn();

    return ctx::fiber_context{};
  });

  // We resumed.
  DCHECK(FiberActive() == prev);
  DCHECK(!prev->entry_);
}

void FiberInterface::PullMyselfFromRemoteReadyQueue() {
  if (!IsScheduledRemotely())
    return;
  // We can not just remove ourselves from the middle of the queue.
  // Therefore we process all the items and since this function is called after the fiber
  // was pulled from the wait queue, it is guaranteed that other threads won't add this object
  // back to the remote_ready_queue.
  bool res = scheduler_->ProcessRemoteReady(this);
  if (IsScheduledRemotely()) {
    LOG(FATAL) << "Failed to pull " << name_ << " from remote_ready_queue. res=" << res
               << " remotenext: " << uint64_t(remote_next_.load(std::memory_order_relaxed))
               << " remotempty: " << scheduler_->RemoteEmpty();
  }
}

void FiberInterface::ExecuteOnFiberStack(PrintFn fn) {
  if (FiberActive() == this) {
    return fn(this);
  }

  // We're in a random fiber but we want to execute in the context of `this`. We call
  // `this->entry_.resume_with(L)`. Calling this method suspends the current fiber and executes `L`
  // on top of the stack of `this->entry_`.
  // It is similar to running an interrupt handler in the same stack as a suspended program.
  // Inside `L`, we execute `fn` and then resume the original fiber that was suspended which is
  // passed to us as the argument. When the other fiber will be resumed normally, it will execute
  // the final `return` statement and go back to its original suspension state.
  entry_ = std::move(entry_).resume_with([fn = std::move(fn), this](ctx::fiber_context&& c) {
    fn(this);

    c = std::move(c).resume();
    return std::move(c);
  });
}

FiberInterface* FiberInterface::SwitchSetup() {
  auto& fb_initializer = FbInitializer();

  // switch the active fiber and set to_suspend to the previously active fiber.
  FiberInterface* to_suspend = fb_initializer.active;
  fb_initializer.active = this;

  uint64_t tsc = CycleClock::Now();

  // When a kernel suspends we may get a negative delta because TSC is reset.
  // We ignore such cases (and they are very rare).
  if (tsc > cpu_tsc_) {
    ++tl.epoch;
    DCHECK_GE(tsc, to_suspend->cpu_tsc_);
    fb_initializer.switch_delay_cycles += (tsc - cpu_tsc_);

    // to_suspend points to the fiber that is active and is going to be suspended.
    uint64_t delta_cycles = tsc - to_suspend->cpu_tsc_;
    if (delta_cycles > g_tsc_cycles_per_ms) {
      fb_initializer.long_runtime_cnt++;

      // improves precision, instead of "delta_cycles / (g_tsc_cycles_per_ms / 1000)"
      fb_initializer.long_runtime_usec += (delta_cycles * 1000) / g_tsc_cycles_per_ms;
    }
  }

  to_suspend->cpu_tsc_ = tsc;  // mark when the fiber was suspended.
  to_suspend->preempt_cnt_++;

  cpu_tsc_ = tsc;
  return to_suspend;
}

void FiberInterface::PrintAllFiberStackTraces() {
  FbInitializer().sched->PrintAllFiberStackTraces();
}

void EnterFiberAtomicSection() noexcept {
  ++FbInitializer().atomic_section;
}

void LeaveFiberAtomicSection() noexcept {
  --FbInitializer().atomic_section;
}

bool IsFiberAtomicSection() noexcept {
  return FbInitializer().atomic_section > 0;
}

void PrintAllFiberStackTraces() {
  FbInitializer().sched->PrintAllFiberStackTraces();
}

void ExecuteOnAllFiberStacks(FiberInterface::PrintFn fn) {
  FbInitializer().sched->ExecuteOnAllFiberStacks(std::move(fn));
}

void ActivateSameThread(FiberInterface* active, FiberInterface* other) {
  if (active->scheduler() != other->scheduler()) {
    LOG(DFATAL) << "Fibers belong to different schedulers";
  }
  active->ActivateOther(other);
}

}  // namespace detail

void SetCustomDispatcher(DispatchPolicy* policy) {
  detail::TL_FiberInitializer& fb_init = detail::FbInitializer();
  fb_init.sched->AttachCustomPolicy(policy);
}

// Total accumulated time in microseconds for ready fibers to become active.
// Together with FiberSwitchEpoch we can compute the average delay per fiber.
uint64_t FiberSwitchDelayUsec() noexcept {
  // in nanoseconds, so lets convert from cycles
  return detail::FbInitializer().switch_delay_cycles * 1000 / detail::g_tsc_cycles_per_ms;
}

// Total number of events of fibers running too long.
uint64_t FiberLongRunCnt() noexcept {
  return detail::FbInitializer().long_runtime_cnt;
}

// Total accumulated time in microseconds for active fibers running too long.
// Together with FiberLongRunCnt we can compute the average time per long running event.
uint64_t FiberLongRunSumUsec() noexcept {
  return detail::FbInitializer().long_runtime_usec;
}

size_t WorkerFibersStackSize() {
  return detail::FbInitializer().sched->worker_stack_size();
}

size_t WorkerFibersCount() {
  return detail::FbInitializer().sched->num_worker_fibers();
}

void PrintFiberStackTracesInThread() {
  detail::PrintAllFiberStackTraces();
}

}  // namespace fb2
}  // namespace util

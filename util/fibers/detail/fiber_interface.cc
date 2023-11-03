// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/detail/fiber_interface.h"

#include <absl/time/clock.h>
#include <condition_variable>
#include <mutex>

#include "base/logging.h"
#include "util/fibers/detail/scheduler.h"

namespace util {
namespace fb2 {
using namespace std;

namespace detail {
namespace ctx = boost::context;

namespace {

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
  MainFiberImpl() noexcept : FiberInterface{MAIN, 1, "main"} {
  }

  ~MainFiberImpl() {
    use_count_.fetch_sub(1, memory_order_relaxed);
  }

 protected:
  void Terminate() {
  }
};

mutex g_scheduler_lock;

}  // namespace

struct TL_FiberInitializer;
TL_FiberInitializer* g_fiber_thread_list = nullptr;

// Per thread initialization structure.
struct TL_FiberInitializer {
  TL_FiberInitializer* next = nullptr;

  // Currently active fiber.
  FiberInterface* active;

  // Per-thread scheduler instance.
  // Allows overriding the main dispatch loop
  Scheduler* sched;
  uint64_t epoch = 0;
  uint64_t switch_delay = 0;  // switch delay in nanoseconds.
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
  /*if (g_parking_ht == nullptr) {
    g_parking_ht = new ParkingHT{};
  }*/
  next = g_fiber_thread_list;
  g_fiber_thread_list = this;
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

uint64_t FiberEpoch() noexcept {
  return FbInitializer().epoch;
}

uint64_t FiberSwitchDelay() noexcept {
  return FbInitializer().switch_delay;
}

FiberInterface::FiberInterface(Type type, uint32_t cnt, string_view nm)
    : use_count_(cnt), flags_(0), type_(type) {
  remote_next_.store((FiberInterface*)kRemoteFree, memory_order_relaxed);
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  name_[len] = 0;
  if (len) {
    memcpy(name_, nm.data(), len);
  }
  ready_ts_ = absl::GetCurrentTimeNanos();
}

FiberInterface::~FiberInterface() {
  DVLOG(2) << "Destroying " << name_;
  DCHECK_EQ(use_count_.load(), 0u);
  DCHECK(wait_queue_.empty());
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

  scheduler_->ScheduleTermination(this);
  DVLOG(2) << "Terminating " << name_;

  while (true) {
    uint16_t fprev = flags_.fetch_or(kTerminatedBit | kBusyBit, memory_order_acquire);
    if ((fprev & kBusyBit) == 0) {
      break;
    }
    CpuPause();
  }

  wait_queue_.NotifyAll(this);

  flags_.fetch_and(~kBusyBit, memory_order_release);

  // usually Preempt returns empty fc but here we return the value of where
  // to switch to when this fiber completes. See intrusive_ptr_release for more info.
  return scheduler_->Preempt();
}

void FiberInterface::Start(Launch launch) {
  auto& fb_init = detail::FbInitializer();
  fb_init.sched->Attach(this);
  uint64_t now = absl::GetCurrentTimeNanos();

  switch (launch) {
    case Launch::post:
      // Activate but do not switch to it.
      fb_init.sched->AddReady(now, this);
      break;
    case Launch::dispatch:
      // Add the active fiber to the ready queue and switch to the new fiber.
      fb_init.sched->AddReady(now, fb_init.active);
      {
        auto fc = SwitchTo(now);
        DCHECK(!fc);
      }
      break;
  }
}

void FiberInterface::Join() {
  FiberInterface* active = FiberActive();

  CHECK(active != this);

  while (true) {
    uint16_t fprev = flags_.fetch_or(kBusyBit, memory_order_acquire);
    if (fprev & kTerminatedBit) {
      if ((fprev & kBusyBit) == 0) {                        // Caller became the owner.
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
  wait_queue_.Link(&waiter);
  flags_.fetch_and(~kBusyBit, memory_order_release);  // release the lock
  DVLOG(2) << "Joining on " << name_;

  active->Suspend();
}

void FiberInterface::Yield() {
  scheduler_->AddReady(absl::GetCurrentTimeNanos(), this);
  scheduler_->Preempt();
}

void FiberInterface::ActivateOther(FiberInterface* other) {
  DVLOG(1) << "Activating " << other->name() << " from " << this->name();
  DCHECK(other->scheduler_);

  // Check first if we the fiber belongs to the active thread.
  if (other->scheduler_ == scheduler_) {
    // In case `other` times out on wait, it could be added to the ready queue already by
    // ProcessSleep.
    if (!other->list_hook.is_linked())
      scheduler_->AddReady(absl::GetCurrentTimeNanos(), other);
  } else {
    other->scheduler_->ScheduleFromRemote(other);
  }
}

void FiberInterface::DetachThread() {
  scheduler_->DetachWorker(this);
  scheduler_ = nullptr;
}

void FiberInterface::AttachThread() {
  scheduler_ = detail::FbInitializer().sched;
  scheduler_->Attach(this);
}

ctx::fiber_context FiberInterface::SwitchTo(uint64_t now) {
  // We can not assert !wait_hook.is_linked() here, because for timeout operations,
  // the fiber can be activated with the wait_hook still linked.
  FiberInterface* prev = this;

  auto& fb_initializer = FbInitializer();
  std::swap(fb_initializer.active, prev);
  ++fb_initializer.epoch;
  DCHECK_GE(now, ready_ts_);
  fb_initializer.switch_delay += (now - ready_ts_);

  // pass pointer to the context that resumes `this`
  return std::move(entry_).resume_with([prev](ctx::fiber_context&& c) {
    DCHECK(!prev->entry_);

    prev->entry_ = std::move(c);  // update the return address in the context we just switch from.
    return ctx::fiber_context{};
  });
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

}  // namespace detail

void SetCustomDispatcher(DispatchPolicy* policy) {
  detail::TL_FiberInitializer& fb_init = detail::FbInitializer();
  fb_init.sched->AttachCustomPolicy(policy);
}

}  // namespace fb2
}  // namespace util

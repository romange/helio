// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/detail/scheduler.h"

#include <condition_variable>
#include <mutex>

#include "base/logging.h"

namespace util {
namespace fb2 {
namespace detail {

namespace ctx = boost::context;
using namespace std;

namespace {

constexpr size_t kSizeOfCtx = sizeof(FiberInterface);  // because of the virtual +8 bytes.
constexpr size_t kSizeOfSH = sizeof(FI_SleepHook);
constexpr size_t kSizeOfLH = sizeof(FI_ListHook);

constexpr uint16_t kTerminatedBit = 0x1;
constexpr uint16_t kBusyBit = 0x2;

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

  mutex mu_;
  condition_variable cnd_;
};

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

DispatcherImpl* MakeDispatcher(Scheduler* sched) {
  ctx::fixedsize_stack salloc;
  ctx::stack_context sctx = salloc.allocate();
  ctx::preallocated palloc = MakePreallocated<DispatcherImpl>(sctx);

  void* sp_ptr = palloc.sp;

  // placement new of context on top of fiber's stack
  return new (sp_ptr) DispatcherImpl{std::move(palloc), std::move(salloc), sched};
}

// Per thread initialization structure.
struct FiberInitializer {
  // Currently active fiber.
  FiberInterface* active;

  // Per-thread scheduler instance.
  // Allows overriding the main dispatch loop
  Scheduler* sched;

  FiberInitializer(const FiberInitializer&) = delete;

  FiberInitializer() : sched(nullptr) {
    DVLOG(1) << "Initializing FiberLib";

    // main fiber context of this thread.
    // We use it as a stub
    FiberInterface* main_ctx = new MainFiberImpl{};
    active = main_ctx;
    sched = new Scheduler(main_ctx);
  }

  ~FiberInitializer() {
    FiberInterface* main_cntx = sched->main_context();
    delete sched;
    delete main_cntx;
  }
};

FiberInitializer& FbInitializer() noexcept {
  // initialized the first time control passes; per thread
  thread_local static FiberInitializer fb_initializer;
  return fb_initializer;
}

// DispatcherImpl implementation.
DispatcherImpl::DispatcherImpl(ctx::preallocated const& palloc, ctx::fixedsize_stack&& salloc,
                               detail::Scheduler* sched) noexcept
    : FiberInterface{DISPATCH, 0, "_dispatch"} {
  entry_ = ctx::fiber(std::allocator_arg, palloc, salloc,
                      [this](ctx::fiber&& caller) { return Run(std::move(caller)); });
  scheduler_ = sched;
}

DispatcherImpl::~DispatcherImpl() {
  DVLOG(1) << "~DispatcherImpl";

  DCHECK(!entry_);
}

ctx::fiber DispatcherImpl::Run(ctx::fiber&& c) {
  if (c) {
    // We context switched from intrusive_ptr_release and this object is destroyed.
    return std::move(c);
  }

  // Normal SwitchTo operation.

  auto& fb_init = detail::FbInitializer();
  if (fb_init.sched->policy()) {
    fb_init.sched->policy()->Run(fb_init.sched);
  } else {
    DefaultDispatch(fb_init.sched);
  }

  DVLOG(1) << "Dispatcher exiting, switching to main_cntx";
  is_terminating_ = true;

  // Like with worker fibers, we switch to another fiber, but in this case to the main fiber.
  // We will come back here during the deallocation of DispatcherImpl from intrusive_ptr_release
  // in order to return from Run() and come back to main context.
  auto fc = fb_init.sched->main_context()->SwitchTo();

  DCHECK(fc);  // Should bring us back to main, into intrusive_ptr_release.
  return fc;
}

void DispatcherImpl::DefaultDispatch(Scheduler* sched) {
  DCHECK(FiberActive() == this);
  DCHECK(!list_hook.is_linked());

  while (true) {
    if (sched->IsShutdown()) {
      if (sched->num_worker_fibers() == 0)
        break;
    }
    sched->DestroyTerminated();
    sched->ProcessRemoteReady();

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
      unique_lock<mutex> lk{mu_};

      cnd_.wait(lk, [&]() { return wake_suspend_; });
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
}

Scheduler::~Scheduler() {
  shutdown_ = true;
  DCHECK(main_cntx_ == FiberActive());

  while (HasReady()) {
    FiberInterface* fi = PopReady();
    DCHECK(!fi->list_hook.is_linked());
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

  CHECK_EQ(0u, num_worker_fibers_);

  // destroys the stack and the object via intrusive_ptr_release.
  dispatch_cntx_.reset();
  DestroyTerminated();
}

ctx::fiber_context Scheduler::Preempt() {
  if (ready_queue_.empty()) {
    return dispatch_cntx_->SwitchTo();
  }

  DCHECK(!ready_queue_.empty());
  FiberInterface* fi = &ready_queue_.front();
  ready_queue_.pop_front();

  __builtin_prefetch(fi);
  return fi->SwitchTo();
}

void Scheduler::ScheduleFromRemote(FiberInterface* cntx) {
  remote_ready_queue_.Push(cntx);

  if (custom_policy_) {
    custom_policy_->Notify();
  } else {
    DispatcherImpl* dimpl = static_cast<DispatcherImpl*>(dispatch_cntx_.get());
    dimpl->Notify();
  }
}

void Scheduler::Attach(FiberInterface* cntx) {
  cntx->scheduler_ = this;
  if (cntx->type() == FiberInterface::WORKER) {
    ++num_worker_fibers_;
  }
}

void Scheduler::ScheduleTermination(FiberInterface* cntx) {
  terminate_queue_.push_back(*cntx);
  if (cntx->type() == FiberInterface::WORKER) {
    --num_worker_fibers_;
  }
}

void Scheduler::DestroyTerminated() {
  while (!terminate_queue_.empty()) {
    FiberInterface* tfi = &terminate_queue_.front();
    terminate_queue_.pop_front();
    DVLOG(1) << "Releasing terminated " << tfi->name_;

    // maybe someone holds a Fiber handle and waits for the fiber to join.
    intrusive_ptr_release(tfi);
  }
}

void Scheduler::WaitUntil(chrono::steady_clock::time_point tp, FiberInterface* me) {
  DCHECK(!me->sleep_hook.is_linked());
  me->tp_ = tp;
  sleep_queue_.insert(*me);
  auto fc = Preempt();
  DCHECK(!fc);
}

void Scheduler::ProcessRemoteReady() {
  while (true) {
    FiberInterface* fi = remote_ready_queue_.Pop();
    if (!fi)
      break;
    DVLOG(1) << "set ready " << fi->name();

    ready_queue_.push_back(*fi);
  }
}

void Scheduler::ProcessSleep() {
  if (sleep_queue_.empty())
    return;

  chrono::steady_clock::time_point now = chrono::steady_clock::now();
  do {
    auto it = sleep_queue_.begin();
    if (it->tp_ > now)
      break;

    FiberInterface& fi = *it;
    sleep_queue_.erase(it);
    ready_queue_.push_back(fi);
  } while (!sleep_queue_.empty());
}

void Scheduler::AttachCustomPolicy(DispatchPolicy* policy) {
  CHECK(custom_policy_ == nullptr);
  custom_policy_ = policy;
}

FiberInterface* FiberActive() noexcept {
  return FbInitializer().active;
}

FiberInterface::FiberInterface(Type type, uint32_t cnt, string_view nm)
    : use_count_(cnt), flags_(0), type_(type) {
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  name_[len] = 0;
  if (len) {
    memcpy(name_, nm.data(), len);
  }
}

FiberInterface::~FiberInterface() {
  DVLOG(2) << "Destroying " << name_;
  DCHECK_EQ(use_count_.load(), 0u);
  DCHECK(wait_queue_.empty());
  DCHECK(!list_hook.is_linked());
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
  DVLOG(1) << "Terminating " << name_;

  while (true) {
    uint16_t fprev = flags_.fetch_or(kTerminatedBit | kBusyBit, memory_order_acquire);
    if ((fprev & kBusyBit) == 0) {
      break;
    }
    CpuPause();
  }

  while (!wait_queue_.empty()) {
    FiberInterface* wait_fib = &wait_queue_.front();
    wait_queue_.pop_front();
    DVLOG(2) << "Scheduling " << wait_fib;

    ActivateOther(wait_fib);
  }

  flags_.fetch_and(~kBusyBit, memory_order_release);

  // usually Preempt returns empty fc but here we return the value of where
  // to switch to when this fiber completes. See intrusive_ptr_release for more info.
  return scheduler_->Preempt();
}

void FiberInterface::Start(Launch launch) {
  auto& fb_init = detail::FbInitializer();
  fb_init.sched->Attach(this);

  switch (launch) {
    case Launch::post:
      fb_init.sched->AddReady(this);
      break;
    case Launch::dispatch:
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

  wait_queue_.push_front(*active);
  flags_.fetch_and(~kBusyBit, memory_order_release);  // release the lock
  active->scheduler_->Preempt();
}

void FiberInterface::ActivateOther(FiberInterface* other) {
  DCHECK(other->scheduler_);

  // Check first if we the fiber belongs to the active thread.
  if (other->scheduler_ == scheduler_) {
    scheduler_->AddReady(other);
  } else {
    other->scheduler_->ScheduleFromRemote(other);
  }
}

void FiberInterface::Suspend() {
  scheduler_->Preempt();
}

ctx::fiber_context FiberInterface::SwitchTo() {
  FiberInterface* prev = this;

  std::swap(FbInitializer().active, prev);

  // pass pointer to the context that resumes `this`
  return std::move(entry_).resume_with([prev](ctx::fiber_context&& c) {
    DCHECK(!prev->entry_);

    prev->entry_ = std::move(c);  // update the return address in the context we just switch from.
    return ctx::fiber_context{};
  });
}

}  // namespace detail

void SetCustomDispatcher(DispatchPolicy* policy) {
  detail::FiberInitializer& fb_init = detail::FbInitializer();
  fb_init.sched->AttachCustomPolicy(policy);
}

DispatchPolicy::~DispatchPolicy() {
}

}  // namespace fb2
}  // namespace util

// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "examples/fiber.h"

#include "base/logging.h"

namespace example {

namespace ctx = boost::context;

namespace detail {

namespace {

constexpr size_t kSizeOfCtx = sizeof(FiberInterface);  // because of the virtual +8 bytes.

class DispatcherImpl final : public FiberInterface {
 public:
  DispatcherImpl(ctx::preallocated const& palloc, ctx::fixedsize_stack&& salloc,
                 Scheduler* sched) noexcept;
  ~DispatcherImpl();

 private:
  ctx::fiber Run(ctx::fiber&& c);
};

class MainFiberImpl final : public FiberInterface {
 public:
  MainFiberImpl() noexcept : FiberInterface{MAIN, 1, "main"} {
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

struct FiberInitializer {
  FiberInterface* active;
  Scheduler* sched;
  DispatcherAlgo custom_algo;

  FiberInitializer(const FiberInitializer&) = delete;

  FiberInitializer() : sched(nullptr) {
    DVLOG(1) << "Initializing FiberLib";

    // main fiber context of this thread
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

}  // namespace

FiberInterface* FiberActive() noexcept {
  return FbInitializer().active;
}

FiberInterface::FiberInterface(Type type, uint32_t cnt, std::string_view nm)
    : use_count_(cnt), flagval_(0), type_(type) {
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  name_[len] = 0;
  if (len) {
    memcpy(name_, nm.data(), len);
  }
}

FiberInterface::~FiberInterface() {
  DVLOG(2) << "Destroying " << name_;
  DCHECK(wait_queue_.empty());
  DCHECK(!list_hook.is_linked());
}

// We can not destroy this instance within the context of the fiber it's been running in.
// The reason: the instance is hosted within the stack region of the fiber itself, and it
// implicitly destroys the stack when destroying its 'entry_' member variable.
// Therefore, to destroy a FiberInterface (WORKER) object, we must call intrusive_ptr_release
// from another fiber. intrusive_ptr_release is smart how it releases resources too
ctx::fiber_context FiberInterface::Terminate() {
  DCHECK(this == FiberActive());
  DCHECK(!list_hook.is_linked());
  DCHECK(!flags.terminated);

  flags.terminated = 1;
  scheduler_->ScheduleTermination(this);

  while (!wait_queue_.empty()) {
    FiberInterface* blocked = &wait_queue_.front();
    wait_queue_.pop_front();

    // should be the scheduler of the blocked fiber.
    blocked->scheduler_->MarkReady(blocked);
  }

  // usually Preempt returns empty fc but here we return the value of where
  // to switch to when this fiber completes. See intrusive_ptr_release for more info.
  return scheduler_->Preempt();
}

void FiberInterface::Join() {
  FiberInterface* active = FiberActive();

  CHECK(active != this);

  // currently single threaded.
  // TODO: to use Vyukov's intrusive mpsc queue:
  // https://www.boost.org/doc/libs/1_63_0/boost/fiber/detail/context_mpsc_queue.hpp
  CHECK(active->scheduler_ == scheduler_);

  if (!flags.terminated) {
    wait_queue_.push_front(*active);
    scheduler_->Preempt();
  }
}

ctx::fiber_context FiberInterface::Resume() {
  FiberInterface* prev = this;

  std::swap(FbInitializer().active, prev);

  // pass pointer to the context that resumes `this`
  return std::move(entry_).resume_with([prev](ctx::fiber_context&& c) {
    DCHECK(!prev->entry_);

    prev->entry_ = std::move(c);  // update the return address in the context we just switch from.
    return ctx::fiber_context{};
  });
}

Scheduler::Scheduler(FiberInterface* main_cntx) : main_cntx_(main_cntx) {
  DCHECK(!main_cntx->scheduler_);
  main_cntx->scheduler_ = this;
  dispatch_cntx_.reset(MakeDispatcher(this));
}

Scheduler::~Scheduler() {
  shutdown_ = true;
  DCHECK(main_cntx_ == FiberActive());
  DCHECK(ready_queue_.empty());

  auto fc = dispatch_cntx_->Resume();
  CHECK(!fc);
  DCHECK_EQ(0u, num_worker_fibers_);

  DestroyTerminated();
}

ctx::fiber_context Scheduler::Preempt() {
  if (ready_queue_.empty()) {
    return dispatch_cntx_->Resume();
  }

  DCHECK(!ready_queue_.empty());
  FiberInterface* fi = &ready_queue_.front();
  ready_queue_.pop_front();

  __builtin_prefetch(fi);
  return fi->Resume();
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

void Scheduler::DefaultDispatch() {
  DCHECK(ready_queue_.empty());

  /*while (true) {
    if (shutdown_) {
      if (num_worker_fibers_ == 0)
        break;
    }
    DestroyTerminated();

  }*/
  DestroyTerminated();
  LOG(WARNING) << "No thread suspension is supported";
}

void Scheduler::DestroyTerminated() {
  while (!terminate_queue_.empty()) {
    FiberInterface* tfi = &terminate_queue_.front();
    terminate_queue_.pop_front();
    DVLOG(1) << "Destructing " << tfi->name_;

    // maybe someone holds a Fiber handle and waits for the fiber to join.
    intrusive_ptr_release(tfi);
  }
}

DispatcherImpl::DispatcherImpl(ctx::preallocated const& palloc, ctx::fixedsize_stack&& salloc,
                               detail::Scheduler* sched) noexcept
    : FiberInterface{DISPATCH, 0, "_dispatch"} {
  entry_ = ctx::fiber(std::allocator_arg, palloc, salloc,
                      [this](ctx::fiber&& caller) { return Run(std::move(caller)); });
  scheduler_ = sched;
}

DispatcherImpl::~DispatcherImpl() {
  DCHECK(!entry_);
}

ctx::fiber DispatcherImpl::Run(ctx::fiber&& c) {
  if (c) {
    // We context switched from intrusive_ptr_release and this object is destroyed.
    return std::move(c);
  }

  // Normal Resume operation.

  auto& fb_init = detail::FbInitializer();
  if (fb_init.custom_algo) {
    fb_init.custom_algo(fb_init.sched);
  } else {
    fb_init.sched->DefaultDispatch();
  }

  return fb_init.sched->main_context()->Resume();
}

}  // namespace detail

Fiber::~Fiber() {
  CHECK(!joinable());
}

Fiber& Fiber::operator=(Fiber&& other) noexcept {
  CHECK(!joinable());

  if (this == &other) {
    return *this;
  }

  impl_.swap(other.impl_);
  return *this;
}

void Fiber::Start() {
  auto& fb_init = detail::FbInitializer();
  fb_init.sched->Attach(impl_.get());
  fb_init.sched->MarkReady(impl_.get());
}

void Fiber::Detach() {
  impl_.reset();
}

void Fiber::Join() {
  CHECK(joinable());
  impl_->Join();
  impl_.reset();
}

void SetCustomScheduler(DispatcherAlgo algo) {
  detail::FiberInitializer& fb_init = detail::FbInitializer();
  fb_init.custom_algo = std::move(algo);
}

}  // namespace example
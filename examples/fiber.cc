// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "examples/fiber.h"

#include "base/logging.h"

namespace example {

namespace ctx = boost::context;

namespace detail {

class MainFiberImpl final : public FiberInterface {
 public:
  MainFiberImpl() noexcept : FiberInterface{1, "main"} {
  }

 protected:
  void Terminate() {
  }
};

struct FiberInitializer {
  FiberInterface* active_;
  FiberInterface* sched_;
  FiberInterface* main_ctx_;

  FiberInitializer() : sched_(nullptr) {
    DVLOG(1) << "Initializing FiberLib";

    // main fiber context of this thread
    main_ctx_ = new MainFiberImpl{};
    active_ = main_ctx_;
  }

  ~FiberInitializer() {
    FiberInterface* main_ctx = active_;
    delete main_ctx;
  }
};

FiberInitializer& FbInitializer() noexcept {
  // initialized the first time control passes; per thread
  thread_local static FiberInitializer fb_initializer;
  return fb_initializer;
}

FiberInterface* FiberActive() noexcept {
  return FbInitializer().active_;
}

void SetSched(FiberInterface* fi) {
  auto& fb_init = FbInitializer();
  CHECK(fb_init.sched_ == NULL);
  fb_init.sched_ = fi;
}

void SchedNext() {
  auto& fb_init = FbInitializer();
  DCHECK(fb_init.sched_);
  fb_init.sched_->Resume();
}

constexpr size_t kSizeOfCtx = sizeof(FiberInterface);  // because of the virtual +8 bytes.

typedef boost::intrusive::list<
    FiberInterface,
    boost::intrusive::member_hook<FiberInterface, detail::FiberContextHook,
                                  &FiberInterface::list_hook>,
    boost::intrusive::constant_time_size<false> >
    FiberWrapperQueue;

thread_local FiberWrapperQueue ready_queue;
thread_local FiberWrapperQueue terminated_queue;

FiberInterface::FiberInterface(uint32_t cnt, std::string_view nm) : use_count_(cnt), flags_(0) {
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  name_[len] = 0;
  if (len) {
    memcpy(name_, nm.data(), len);
  }
}

FiberInterface::~FiberInterface() {
}

void FiberInterface::Terminate() {
  DCHECK(this == FiberActive());
  DCHECK(!list_hook.is_linked());

  terminated_queue.push_back(*this);

  auto& fb_init = FbInitializer();

  if (this == fb_init.sched_) {
    fb_init.sched_ = nullptr;
    fb_init.main_ctx_->Resume();
  } else {
    fb_init.sched_->Resume();
  }
}

void FiberInterface::Join() {
  // currently single threaded.
  // TODO: to use Vyukov's intrusive mpsc queue:
  // https://www.boost.org/doc/libs/1_63_0/boost/fiber/detail/context_mpsc_queue.hpp
  if (!bits.terminated) {
    SchedNext();
  }
}

void FiberInterface::Resume() {
  FiberInterface* prev = this;

  std::swap(FbInitializer().active_, prev);

  // pass pointer to the context that resumes `this`
  std::move(c_).resume_with([prev](ctx::fiber_context&& c) {
    DCHECK(!prev->c_);

    prev->c_ = std::move(c);  // update the return address in the context we just switch from.
    return ctx::fiber_context{};
  });
}

void FiberInterface::SetReady() {
  DCHECK(!list_hook.is_linked());
  ready_queue.push_back(*this);
}

void RunReadyFbs() {
  while (!ready_queue.empty()) {
    detail::FiberInterface* cntx = &ready_queue.front();
    ready_queue.pop_front();

    cntx->Resume();
  }
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
  impl_->SetReady();
}

void Fiber::Detach() {
  impl_.reset();
}

void Fiber::Join() {
  CHECK(joinable());
  CHECK(detail::FiberActive() != impl_.get());
  impl_->Join();
  impl_.reset();
}

void Fiber::SchedNext() {
  detail::SchedNext();
}

}  // namespace example
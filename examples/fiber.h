// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <boost/context/fiber.hpp>
#include <boost/intrusive/list.hpp>
#include <string_view>

namespace example {

namespace detail {

typedef boost::intrusive::list_member_hook<
    boost::intrusive::link_mode<boost::intrusive::auto_unlink> >
    FiberContextHook;

}  // namespace detail

class BaseFiberWrapper {
 public:
  BaseFiberWrapper(uint32_t init_count, std::string_view nm = std::string_view{});

  virtual ~BaseFiberWrapper();

  detail::FiberContextHook list_hook{};  // 16 bytes

  void StartMain();

  void Resume();
  void Suspend();
  void SetReady();

  bool IsDefined() const {
    return bool(c_);
  }

  // We need refcounting for referencing handles via .
  friend void intrusive_ptr_add_ref(BaseFiberWrapper* ctx) noexcept {
    ctx->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend void intrusive_ptr_release(BaseFiberWrapper* ctx) noexcept {
    if (1 == ctx->use_count_.fetch_sub(1, std::memory_order_release)) {
      std::atomic_thread_fence(std::memory_order_acquire);
      boost::context::fiber c = std::move(ctx->c_);

      // destruct context
      ctx->~BaseFiberWrapper();

      // deallocated stack
      std::move(c).resume();
    }
  }

 protected:
  void Terminate();

  std::atomic<uint32_t> use_count_;

  // holds its own fiber_context when it's not active.
  // the difference between fiber_context and continuation is that continuation is launched
  // straight away via callcc and fiber is created without switching to it.
  // TODO: I still do not know how continuation_fcontext and fiber_fcontext achieve this
  // difference because their code looks very similar except some renaming.
  ::boost::context::fiber_context c_;  // 8 bytes

  char name_[24];
};

namespace detail {

template <typename Fn> class CustomFiberWrapper : public BaseFiberWrapper {
  using FbCntx = ::boost::context::fiber_context;

 public:
  template <typename StackAlloc>
  CustomFiberWrapper(std::string_view name, const boost::context::preallocated& palloc,
                     StackAlloc&& salloc, Fn&& fn)
      : BaseFiberWrapper(1, name), fn_(std::forward<Fn>(fn)) {
    c_ = FbCntx(std::allocator_arg, palloc, std::forward<StackAlloc>(salloc),
                [this](FbCntx&& caller) { return run_(std::move(caller)); });
  }

 private:
  FbCntx run_(FbCntx&& c) {
    {
      // fn and tpl must be destroyed before calling terminate()
      auto fn = std::move(fn_);
      fn();
    }

    Terminate();

    // Only main_cntx reaches this point because all the rest should switch to main_wrapper and
    // never switch back.
    return std::move(c);
  }

  Fn fn_;
};

template <typename StackAlloc, typename Fn>
static CustomFiberWrapper<Fn>* MakeCustomFiberWrapper(std::string_view name, StackAlloc&& salloc,
                                                      Fn&& fn) {
  boost::context::stack_context sctx = salloc.allocate();

  // reserve space for control structure
  uintptr_t storage = (reinterpret_cast<uintptr_t>(sctx.sp) - sizeof(CustomFiberWrapper<Fn>)) &
                      ~static_cast<uintptr_t>(0xff);
  uintptr_t stack_bottom = reinterpret_cast<uintptr_t>(sctx.sp) - static_cast<uintptr_t>(sctx.size);
  const std::size_t size = storage - stack_bottom;
  void* st_ptr = reinterpret_cast<void*>(storage);

  // placement new of context on top of fiber's stack
  CustomFiberWrapper<Fn>* fctx =
      new (st_ptr) CustomFiberWrapper<Fn>{name, boost::context::preallocated{st_ptr, size, sctx},
                                          std::forward<StackAlloc>(salloc), std::forward<Fn>(fn)};
  return fctx;
}

}  // namespace detail

class Fiber {
 public:
  using ID = uint64_t;

  Fiber() = default;

  template <typename Fn> Fiber(Fn&& fn) : Fiber(std::string_view{}, std::forward<Fn>(fn)) {
  }

  template <typename Fn>
  Fiber(std::string_view name, Fn&& fn)
      : impl_{detail::MakeCustomFiberWrapper(name, boost::context::fixedsize_stack(),
                                             std::forward<Fn>(fn))} {
    Start();
  }

  ~Fiber();

  Fiber(Fiber const&) = delete;
  Fiber& operator=(Fiber const&) = delete;

  Fiber(Fiber&& other) noexcept : impl_{} {
    swap(other);
  }

  Fiber& operator=(Fiber&& other) noexcept;

  void swap(Fiber& other) noexcept {
    impl_.swap(other.impl_);
  }

  ID get_id() const noexcept {
    return reinterpret_cast<ID>(impl_.get());
  }

  bool joinable() const noexcept {
    return nullptr != impl_;
  }

  void Join();

  void Detach();

 private:
  void Start();

  boost::intrusive_ptr<BaseFiberWrapper> impl_;
};

}  // namespace example

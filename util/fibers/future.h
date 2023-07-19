// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// This code is a modified version of Boost.Fibers future code.
//
#include <boost/intrusive_ptr.hpp>
#include <memory>

#include "util/fibers/synchronization.h"

namespace util {
namespace fb2 {

enum class future_status { ready = 1, timeout, deferred };
template <typename R> class Future;

namespace detail {

class shared_state_base {
 private:
  std::atomic<std::size_t> use_count_{0};
  mutable CondVar waiters_{};

 protected:
  mutable Mutex mtx_{};
  bool ready_{false};

  void mark_ready_and_notify_(std::unique_lock<Mutex>& lk) noexcept {
    ready_ = true;
    lk.unlock();
    waiters_.notify_all();
  }

  void wait_(std::unique_lock<Mutex>& lk) const {
    waiters_.wait(lk, [this]() { return ready_; });
  }

  template <typename Rep, typename Period>
  future_status wait_for_(std::unique_lock<Mutex>& lk,
                          std::chrono::duration<Rep, Period> const& timeout_duration) const {
    return waiters_.wait_for(lk, timeout_duration, [this]() { return ready_; })
               ? future_status::ready
               : future_status::timeout;
  }

  template <typename Clock, typename Duration>
  future_status wait_until_(std::unique_lock<Mutex>& lk,
                            std::chrono::time_point<Clock, Duration> const& timeout_time) const {
    return waiters_.wait_until(lk, timeout_time, [this]() { return ready_; })
               ? future_status::ready
               : future_status::timeout;
  }

  virtual void deallocate_future() noexcept = 0;

 public:
  shared_state_base() = default;

  virtual ~shared_state_base() = default;

  shared_state_base(shared_state_base const&) = delete;
  shared_state_base& operator=(shared_state_base const&) = delete;

  void wait() const {
    std::unique_lock<Mutex> lk{mtx_};
    wait_(lk);
  }

  template <typename Rep, typename Period>
  future_status wait_for(std::chrono::duration<Rep, Period> const& timeout_duration) const {
    std::unique_lock<Mutex> lk{mtx_};
    return wait_for_(lk, timeout_duration);
  }

  template <typename Clock, typename Duration>
  future_status wait_until(std::chrono::time_point<Clock, Duration> const& timeout_time) const {
    std::unique_lock<Mutex> lk{mtx_};
    return wait_until_(lk, timeout_time);
  }

  friend inline void intrusive_ptr_add_ref(shared_state_base* p) noexcept {
    p->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend inline void intrusive_ptr_release(shared_state_base* p) noexcept {
    if (1 == p->use_count_.fetch_sub(1, std::memory_order_release)) {
      std::atomic_thread_fence(std::memory_order_acquire);
      p->deallocate_future();
    }
  }
};

template <typename R> class shared_state : public shared_state_base {
 private:
  typename std::aligned_storage<sizeof(R), alignof(R)>::type storage_{};

  void set_value_(R const& value, std::unique_lock<Mutex>& lk) {
    assert(!ready_);
    ::new (static_cast<void*>(std::addressof(storage_))) R(value);
    mark_ready_and_notify_(lk);
  }

  void set_value_(R&& value, std::unique_lock<Mutex>& lk) {
    assert(!ready_);
    ::new (static_cast<void*>(std::addressof(storage_))) R(std::move(value));
    mark_ready_and_notify_(lk);
  }

  R& get_(std::unique_lock<Mutex>& lk) {
    wait_(lk);
    return *reinterpret_cast<R*>(std::addressof(storage_));
  }

 public:
  typedef boost::intrusive_ptr<shared_state> ptr_type;

  shared_state() = default;

  virtual ~shared_state() {
    if (ready_) {
      reinterpret_cast<R*>(std::addressof(storage_))->~R();
    }
  }

  shared_state(shared_state const&) = delete;
  shared_state& operator=(shared_state const&) = delete;

  void set_value(R const& value) {
    std::unique_lock<Mutex> lk{mtx_};
    set_value_(value, lk);
  }

  void set_value(R&& value) {
    std::unique_lock<Mutex> lk{mtx_};
    set_value_(std::move(value), lk);
  }

  R& get() {
    std::unique_lock<Mutex> lk{mtx_};
    return get_(lk);
  }
};

template <typename R, typename Allocator> class shared_state_object : public shared_state<R> {
 public:
  typedef typename std::allocator_traits<Allocator>::template rebind_alloc<shared_state_object>
      allocator_type;

  shared_state_object(allocator_type const& alloc) : shared_state<R>{}, alloc_{alloc} {
  }

 protected:
  void deallocate_future() noexcept override final {
    destroy_(alloc_, this);
  }

 private:
  allocator_type alloc_;

  static void destroy_(allocator_type const& alloc, shared_state_object* p) noexcept {
    allocator_type a{alloc};
    typedef std::allocator_traits<allocator_type> traity_type;
    traity_type::destroy(a, p);
    traity_type::deallocate(a, p, 1);
  }
};

template <typename R> struct future_base {
  typedef typename shared_state<R>::ptr_type ptr_type;

  ptr_type state_{};

  future_base() = default;

  explicit future_base(ptr_type const& p) noexcept : state_{p} {
  }

  ~future_base() = default;

  future_base(future_base const& other) : state_{other.state_} {
  }

  future_base(future_base&& other) noexcept : state_{other.state_} {
    other.state_.reset();
  }

  future_base& operator=(future_base const& other) noexcept {
    if ((this != &other)) {
      state_ = other.state_;
    }
    return *this;
  }

  future_base& operator=(future_base&& other) noexcept {
    if ((this != &other)) {
      state_ = other.state_;
      other.state_.reset();
    }
    return *this;
  }

  bool valid() const noexcept {
    return nullptr != state_.get();
  }

  void wait() const {
    state_->wait();
  }

  template <typename Rep, typename Period>
  future_status wait_for(std::chrono::duration<Rep, Period> const& timeout_duration) const {
    return state_->wait_for(timeout_duration);
  }

  template <typename Clock, typename Duration>
  future_status wait_until(std::chrono::time_point<Clock, Duration> const& timeout_time) const {
    return state_->wait_until(timeout_time);
  }
};

template <typename R> struct promise_base {
  typedef typename shared_state<R>::ptr_type ptr_type;

  bool obtained_{false};
  ptr_type future_{};

  promise_base() : promise_base{std::allocator_arg, std::allocator<promise_base>{}} {
  }

  template <typename Allocator> promise_base(std::allocator_arg_t, Allocator alloc) {
    typedef detail::shared_state_object<R, Allocator> object_type;
    typedef std::allocator_traits<typename object_type::allocator_type> traits_type;
    typename object_type::allocator_type a{alloc};
    typename traits_type::pointer ptr{traits_type::allocate(a, 1)};
    auto* p = ptr;

    try {
      traits_type::construct(a, p, a);
    } catch (...) {
      traits_type::deallocate(a, ptr, 1);
      throw;
    }
    future_.reset(p);
  }

  ~promise_base() {
    if (future_ && obtained_) {
      // future_->owner_destroyed();
    }
  }

  promise_base(promise_base const&) = delete;
  promise_base& operator=(promise_base const&) = delete;

  promise_base(promise_base&& other) noexcept
      : obtained_{other.obtained_}, future_{std::move(other.future_)} {
    other.obtained_ = false;
  }

  promise_base& operator=(promise_base&& other) noexcept {
    if ((this != &other)) {
      promise_base tmp{std::move(other)};
      swap(tmp);
    }
    return *this;
  }

  Future<R> get_future() {
    assert(!obtained_);
    assert(future_);
    obtained_ = true;
    return Future<R>{future_};
  }

  void swap(promise_base& other) noexcept {
    std::swap(obtained_, other.obtained_);
    future_.swap(other.future_);
  }
};

}  // namespace detail

template <typename R> class Future : private detail::future_base<R> {
 private:
  typedef detail::future_base<R> base_type;

  friend struct detail::promise_base<R>;

  explicit Future(typename base_type::ptr_type const& p) noexcept : base_type{p} {
  }

 public:
  Future() = default;

  Future(Future const&) = delete;
  Future& operator=(Future const&) = delete;

  Future(Future&& other) noexcept : base_type{std::move(other)} {
  }

  Future& operator=(Future&& other) noexcept {
    if ((this != &other)) {
      base_type::operator=(std::move(other));
    }
    return *this;
  }

  R get() {
    typename base_type::ptr_type tmp{};
    tmp.swap(base_type::state_);
    return std::move(tmp->get());
  }

  using base_type::valid;
  using base_type::wait;
  using base_type::wait_for;
  using base_type::wait_until;
};

template <typename R> class Promise : private detail::promise_base<R> {
  typedef detail::promise_base<R> base_type;

 public:
  void set_value(R& value) {
    base_type::future_->set_value(value);
  }

  void set_value(const R& value) {
    base_type::future_->set_value(value);
  }

  void swap(Promise& other) noexcept {
    base_type::swap(other);
  }

  using base_type::get_future;
};

}  // namespace fb2
}  // namespace util

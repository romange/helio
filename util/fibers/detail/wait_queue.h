// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive/list.hpp>
#include <functional>
#include <variant>

namespace util {
namespace fb2 {
namespace detail {

class FiberInterface;

class Waiter {
  friend class FiberInterface;

  explicit Waiter(FiberInterface* cntx) : cntx_(cntx) {
  }

 public:
  using Callback = std::function<void()>;
  explicit Waiter(Callback&& cb) : cntx_{std::move(cb)} {
  }

  // For some boost versions/distributions, the default move c'tor does not work well,
  // so we implement it explicitly.
  Waiter(Waiter&& o) : cntx_{std::move(o.cntx_)} {
    // it does not work well for slist because its reference is used by slist members
    // (probably when caching last).
    o.wait_hook.swap_nodes(wait_hook);
    o.cntx_ = nullptr;
  }

  // safe_link is used in assertions via IsLinked() method.
  using ListHookType =
      boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>;

  bool IsLinked() const {
    return wait_hook.is_linked();
  }

  decltype(auto) Take() {
    return std::move(cntx_);
  }

  ListHookType wait_hook;

 private:
  std::variant<FiberInterface*, Callback> cntx_;
};

// All WaitQueue are not thread safe and must be run under a lock.

class WaitQueue {
 public:
  bool empty() const {
    return wait_list_.empty();
  }

  void Link(Waiter* waiter);

  void Unlink(Waiter* waiter) {
    auto it = WaitList::s_iterator_to(*waiter);
    wait_list_.erase(it);
  }

  bool NotifyOne(FiberInterface* active);
  bool NotifyAll(FiberInterface* active);

 private:
  using WaitList = boost::intrusive::list<
      Waiter, boost::intrusive::member_hook<Waiter, Waiter::ListHookType, &Waiter::wait_hook>,
      boost::intrusive::constant_time_size<false>>;

  void NotifyImpl(FiberInterface* suspended, FiberInterface* active);
  void NotifyImpl(const Waiter::Callback&, FiberInterface* active);

  WaitList wait_list_;
};

}  // namespace detail
}  // namespace fb2
}  // namespace util

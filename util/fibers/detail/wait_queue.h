// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive/slist.hpp>

namespace util {
namespace fb2 {
namespace detail {

class FiberInterface;

class Waiter {
  friend class FiberInterface;

  explicit Waiter(FiberInterface* cntx) : cntx_(cntx) {
  }

 public:
  using ListHookType =
      boost::intrusive::slist_member_hook<boost::intrusive::link_mode<boost::intrusive::safe_link>>;

  FiberInterface* cntx() const {
    return cntx_;
  }

  bool IsLinked() const {
    return wait_hook.is_linked();
  }

  ListHookType wait_hook;

 private:
  FiberInterface* cntx_;
};

// All WaitQueue are not thread safe and must be run under a spinlock.

class WaitQueue {
 public:
  bool empty() const {
    return wait_list_.empty();
  }

  void Link(Waiter* waiter) {
    wait_list_.push_back(*waiter);
  }

  void Unlink(Waiter* waiter) {
    auto it = WaitList::s_iterator_to(*waiter);
    wait_list_.erase(it);
  }

  bool NotifyOne(FiberInterface* active) {
    if (wait_list_.empty())
      return false;

    Waiter* waiter = &wait_list_.front();
    wait_list_.pop_front();
    NotifyImpl(waiter->cntx(), active);

    return true;
  }

  void NotifyAll(FiberInterface* active);

 private:
  using WaitList = boost::intrusive::slist<
      Waiter, boost::intrusive::member_hook<Waiter, Waiter::ListHookType, &Waiter::wait_hook>,
      boost::intrusive::constant_time_size<false>, boost::intrusive::cache_last<true>>;

  void NotifyImpl(FiberInterface* suspended, FiberInterface* active);

  WaitList wait_list_;
};

}  // namespace detail
}  // namespace fb2
}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/detail/wait_queue.h"

#include "base/logging.h"
#include "util/fibers/detail/fiber_interface.h"

namespace util {
namespace fb2 {
namespace detail {

[[maybe_unused]] constexpr size_t WakeOtherkSizeOfWaitQ = sizeof(WaitQueue);

void Waiter::Notify(FiberInterface* active) const {
  visit([this, active](const auto& v) { NotifyImpl(v, active); }, cntx_);
}

void Waiter::NotifyImpl(FiberInterface* suspended, FiberInterface* active) const {
  active->ActivateOther(suspended);
}

void Waiter::NotifyImpl(const Waiter::Callback& cb, FiberInterface* active) const {
  cb();
}

void WaitQueue::Link(Waiter* waiter) {
  DCHECK(waiter);
  wait_list_.push_back(*waiter);
  DCHECK(!wait_list_.empty());
}

bool WaitQueue::NotifyOne(FiberInterface* active) {
  if (wait_list_.empty())
    return false;

  Waiter* waiter = &wait_list_.front();
  DCHECK(waiter) << wait_list_.empty();

  // A persistent waiter at the front will always be selected by NotifyOne().
  // If other waiters are queued behind it, they will be starved because the persistent waiter never
  // leaves the front of the line. Persistent subscriptions are intended for notifyAll() broadcasts.
  DCHECK(!waiter->is_persistent() || &wait_list_.front() == &wait_list_.back())
      << "NotifyOne on a persistent waiter with others queued behind it causes starvation. "
         "Use notifyAll() with persistent waiters.";

  // If the waiter is one-shot (default), remove it from the list before
  // notification. If it is persistent, leave it at the front so it can
  // receive subsequent notifications until explicitly unlinked.
  if (!waiter->is_persistent()) {
    wait_list_.pop_front();
  }

  waiter->Notify(active);  // lifetime might end after this call
  return true;
}

bool WaitQueue::NotifyAll(FiberInterface* active) {
  bool notified = false;
  auto it = wait_list_.begin();

  // Note: No iterator invalidation here (waiter.Notify() is safe while iterating)
  // - For fiber-based (one-shot) waiters, ActivateOther() only enqueues the fiber on the ready
  // queue without yielding, so no notified fiber runs during this loop.
  // - For callback-based (persistent) waiters, the iterator is advanced before Notify(), so even if
  // the callback modifies *this* waiter's linkage, the iterator remains valid.
  while (it != wait_list_.end()) {
    Waiter& waiter = *it;
    if (waiter.is_persistent()) {
      ++it;
    } else {
      it = wait_list_.erase(it);
    }
    waiter.Notify(active);
    notified = true;
  }
  return notified;
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

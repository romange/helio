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
  wait_list_.pop_front();

  waiter->Notify(active);  // lifetime might end after this call
  return true;
}

bool WaitQueue::NotifyAll(FiberInterface* active) {
  bool notified = false;
  while (NotifyOne(active))
    notified = true;
  return notified;
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

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

  auto value = waiter->Take();
  wait_list_.pop_front();

  std::visit([this, active](const auto& v) { NotifyImpl(v, active); }, value);
  return true;
}

bool WaitQueue::NotifyAll(FiberInterface* active) {
  bool notified = false;
  while (NotifyOne(active))
    notified = true;
  return notified;
}

void WaitQueue::NotifyImpl(FiberInterface* suspended, FiberInterface* active) {
  active->ActivateOther(suspended);
}

void WaitQueue::NotifyImpl(const Waiter::Callback& cb, FiberInterface* active) {
  cb();
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

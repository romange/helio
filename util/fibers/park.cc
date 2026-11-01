// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/park.h"

#include "base/logging.h"
#include "util/fibers/detail/fiber_interface_impl.h"

namespace util {
namespace fb2 {

void ParkHandle::Park() noexcept {
  DCHECK(fiber_ == detail::FiberActive());
  DCHECK(!detail::IsFiberAtomicSection());
  fiber_->Suspend();
}

void WakeList::Wake() noexcept {
  ParkNode* node = std::exchange(head_, nullptr);
  if (!node)
    return;

  detail::FiberInterface* active = detail::FiberActive();
  do {
    // Clear the link so the node can be published again after Park() returns.
    ParkNode* next = std::exchange(node->next_, nullptr);
    detail::FiberInterface* fiber = node->fiber_;
    bool to_front = node->wake_to_front_;
    DCHECK(fiber != active) << "A fiber cannot wake its own node";

    // `node` may be destroyed once `fiber` runs.
    active->ActivateOther(fiber, to_front);
    node = next;
  } while (node);
}

}  // namespace fb2
}  // namespace util

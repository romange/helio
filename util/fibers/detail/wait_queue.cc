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

void WaitQueue::NotifyAll(FiberInterface* active) {
  while (!wait_list_.empty()) {
    Waiter* waiter = &wait_list_.front();
    wait_list_.pop_front();

    FiberInterface* cntx = waiter->cntx();
    DVLOG(2) << "Scheduling " << cntx->name() << " from " << active->name();

    active->WakeOther(waiter->epoch(), cntx);
  }
}

void WaitQueue::NotifyImpl(uint32_t epoch, FiberInterface* suspended, FiberInterface* active) {
  active->WakeOther(epoch, suspended);
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

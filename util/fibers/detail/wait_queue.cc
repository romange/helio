// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/detail/wait_queue.h"

#include "base/logging.h"
#include "util/fibers/detail/fiber_interface.h"

#include "util/proactor_pool.h"

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
    auto flags = cntx->flags_.load(std::memory_order_acquire);
    auto dbg = cntx->debug_flag_.load(std::memory_order_relaxed);
    DCHECK_EQ(flags & 4, 0u) << dbg;

    active->ActivateOther(cntx, int64_t(active));
  }
}

bool WaitQueue::NotifyOne(FiberInterface* active) {
  if (wait_list_.empty())
    return false;

  Waiter* waiter = &wait_list_.front();
  wait_list_.pop_front();
  NotifyImpl(waiter->cntx(), active);

  return true;
}

void WaitQueue::Link(Waiter* waiter) {
  int64_t balance = waiter->cntx()->balance.fetch_add(1, std::memory_order_relaxed);
  // CHECK_EQ(balance, 0) << " me: " << int64_t(this) << " f-reff: " << waiter->cntx()->debug_flag_.load(std::memory_order_relaxed) << " flags: " << waiter->cntx()->flags_.load(std::memory_order_relaxed);
  DCHECK(!waiter->IsLinked());
  wait_list_.push_back(*waiter);
  // fibs.push_back(waiter->cntx());
}

void WaitQueue::Unlink(Waiter* waiter) {
  // CHECK_EQ(waiter->cntx()->balance.fetch_sub(1, std::memory_order_relaxed), 1);
  auto it = WaitList::s_iterator_to(*waiter);
  wait_list_.erase(it);
  // fibs.erase(find(fibs.begin(), fibs.end(), waiter->cntx()));
}

bool WaitQueue::NotifyImpl(FiberInterface* suspended, FiberInterface* active) {
  return active->ActivateOther(suspended, -int64_t(this));
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

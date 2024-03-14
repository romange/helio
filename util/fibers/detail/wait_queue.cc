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
  while (!fibs.empty()) {
    FiberInterface* fib = fibs.front();
    fibs.pop_front();

    DVLOG(2) << "Scheduling " << fib->name() << " from " << active->name();

    active->ActivateOther(fib, int64_t(this));
  }
}

bool WaitQueue::NotifyOne(FiberInterface* active) {
  //if (wait_list_.empty())
  //  return false;
  //FiberInterface* fib = wait_list_.front().cntx();
  //wait_list_.pop_front();

  if (fibs.empty())
    return false;
  auto* fib = fibs.front();
  fibs.pop_front();

  if (!active->ActivateOther(fib, int64_t(this))) {
    //CHECK(false);
  }

  return true;
}

void WaitQueue::Link(Waiter* waiter) {
  int64_t balance = waiter->cntx()->balance.fetch_add(1, std::memory_order_relaxed);
  CHECK_EQ(balance, 0) << " me: " << int64_t(this) << " f-reff: " << waiter->cntx()->debug_flag_.load(std::memory_order_relaxed) << " flags: " << waiter->cntx()->flags_.load(std::memory_order_relaxed);
  //wait_list_.push_back(*waiter);
  fibs.push_back(waiter->cntx());
}

void WaitQueue::Unlink(Waiter* waiter) {
  CHECK_EQ(waiter->cntx()->balance.fetch_sub(1, std::memory_order_relaxed), 1);
  //auto it = WaitList::s_iterator_to(*waiter);
  //wait_list_.erase(it);
  fibs.erase(find(fibs.begin(), fibs.end(), waiter->cntx()));
}

bool WaitQueue::NotifyImpl(FiberInterface* suspended, FiberInterface* active) {
  return false;
  //return active->ActivateOther(suspended);
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

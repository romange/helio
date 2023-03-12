// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/synchronization.h"

#include "base/logging.h"

namespace util {
namespace fb2 {

void Mutex::lock() {
  while (true) {
    detail::FiberInterface* active = detail::FiberActive();

    {
      detail::SpinLockHolder lk{&wait_queue_splk_};

      if (nullptr == owner_) {
        owner_ = active;
        return;
      }

      CHECK(active != owner_);

      wait_queue_.push_back(*active);
    }
    active->scheduler()->Preempt();
  }
}

bool Mutex::try_lock() {
  detail::FiberInterface* active = detail::FiberActive();
  {
    detail::SpinLockHolder lk{&wait_queue_splk_};
    if (nullptr == owner_) {
      owner_ = active;
      return true;
    }
  }
  return false;
}

void Mutex::unlock() {
  detail::FiberInterface* active = detail::FiberActive();

  wait_queue_splk_.Lock();
  CHECK(owner_ == active);
  owner_ = nullptr;
  if (wait_queue_.empty()) {
    wait_queue_splk_.Unlock();
    return;
  }

  detail::FiberInterface* fi = &wait_queue_.front();
  wait_queue_.pop_front();
  wait_queue_splk_.Unlock();
  active->ActivateOther(fi);
}


void
CondVarAny::notify_one() noexcept {
  wait_queue_splk_.Lock();
  if (wait_queue_.empty()) {
    wait_queue_splk_.Unlock();
    return;
  }

  detail::FiberInterface* fi = &wait_queue_.front();
  wait_queue_.pop_front();
  wait_queue_splk_.Unlock();
  detail::FiberInterface* active = detail::FiberActive();
  active->ActivateOther(fi);
}

}  // namespace fb2
}  // namespace util

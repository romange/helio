// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/synchronization.h"

#include "base/logging.h"

namespace util {
namespace fb2 {

void Mutex::lock() {
  detail::FiberInterface* active = detail::FiberActive();

  while (true) {
    {
      std::unique_lock lk(wait_queue_splk_);

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
    std::unique_lock lk{wait_queue_splk_};
    if (nullptr == owner_) {
      owner_ = active;
      return true;
    }
  }
  return false;
}

void Mutex::unlock() {
  detail::FiberInterface* active = detail::FiberActive();

  std::unique_lock lk(wait_queue_splk_);
  CHECK(owner_ == active);
  owner_ = nullptr;
  if (wait_queue_.empty()) {
    return;
  }

  detail::FiberInterface* fi = &wait_queue_.front();
  wait_queue_.pop_front();
  lk.unlock();
  active->ActivateOther(fi);
}

void CondVarAny::notify_one() noexcept {
  std::unique_lock lk(wait_queue_splk_);
  if (wait_queue_.empty()) {
    return;
  }

  detail::FiberInterface* fi = &wait_queue_.front();
  wait_queue_.pop_front();
  lk.unlock();
  detail::FiberInterface* active = detail::FiberActive();
  active->ActivateOther(fi);
}

void CondVarAny::notify_all() noexcept {
  decltype(wait_queue_) tmp_queue;
  wait_queue_splk_.lock();
  tmp_queue.swap(wait_queue_);
  wait_queue_splk_.unlock();

  detail::FiberInterface* active = detail::FiberActive();
  while (!tmp_queue.empty()) {
    detail::FiberInterface* fi = &tmp_queue.front();
    tmp_queue.pop_front();
    active->ActivateOther(fi);
  }
}

}  // namespace fb2
}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/synchronization.h"

#include "base/logging.h"

namespace util {
namespace fb2 {

using namespace std;

void Mutex::lock() {
  detail::FiberInterface* active = detail::FiberActive();
  detail::Waiter waiter(active->CreateWaiter());

  while (true) {
    {
      unique_lock lk(wait_queue_splk_);

      if (nullptr == owner_) {
        DCHECK(!waiter.IsLinked());

        owner_ = active;
        return;
      }

      CHECK(active != owner_);
      wait_queue_.Register(&waiter);
    }
    active->Suspend();
  }
}

bool Mutex::try_lock() {
  detail::FiberInterface* active = detail::FiberActive();

  {
    unique_lock lk{wait_queue_splk_};
    if (nullptr == owner_) {
      owner_ = active;
      return true;
    }
  }
  return false;
}

void Mutex::unlock() {
  detail::FiberInterface* active = detail::FiberActive();

  unique_lock lk(wait_queue_splk_);
  CHECK(owner_ == active);
  owner_ = nullptr;

  wait_queue_.NotifyOne(active);
}

void CondVarAny::notify_one() noexcept {
  detail::FiberInterface* active = detail::FiberActive();

  unique_lock lk(wait_queue_splk_);
  wait_queue_.NotifyOne(active);
}

void CondVarAny::notify_all() noexcept {
  detail::FiberInterface* active = detail::FiberActive();
  unique_lock lk(wait_queue_splk_);
  wait_queue_.NotifyAll(active);
}

Barrier::Barrier(size_t initial) : initial_{initial}, current_{initial_} {
  DCHECK_NE(0u, initial);
}

bool Barrier::Wait() {
  unique_lock lk{mtx_};
  const size_t cycle = cycle_;
  if (0 == --current_) {
    ++cycle_;
    current_ = initial_;
    lk.unlock();  // no pessimization
    cond_.notify_all();
    return true;
  }

  cond_.wait(
      lk, [&] { return (cycle != cycle_) || (cycle_ == std::numeric_limits<std::size_t>::max()); });
  return false;
}

void Barrier::Cancel() {
  {
    lock_guard lg{mtx_};
    cycle_ = numeric_limits<size_t>::max();
  }
  cond_.notify_all();
}

}  // namespace fb2
}  // namespace util

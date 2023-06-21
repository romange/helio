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
  DCHECK(!active->wait_hook.is_linked());

  while (true) {
    {
      unique_lock lk(wait_queue_splk_);

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
  DCHECK(!active->wait_hook.is_linked());

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
  assert(!active->wait_hook.is_linked());

  unique_lock lk(wait_queue_splk_);
  CHECK(owner_ == active);
  owner_ = nullptr;
  if (wait_queue_.empty()) {
    return;
  }

  detail::FiberInterface* fi = &wait_queue_.front();
  wait_queue_.pop_front();
  active->ActivateOther(fi);
}

void CondVarAny::notify_one() noexcept {
  unique_lock lk(wait_queue_splk_);
  if (wait_queue_.empty()) {
    return;
  }

  detail::FiberInterface* active = detail::FiberActive();
  DCHECK(!active->wait_hook.is_linked());

  detail::FiberInterface* fi = &wait_queue_.front();
  wait_queue_.pop_front();
  active->ActivateOther(fi);
}

void CondVarAny::notify_all() noexcept {
  decltype(wait_queue_) tmp_queue;
  detail::FiberInterface* active = detail::FiberActive();
  DCHECK(!active->wait_hook.is_linked());

  unique_lock lk(wait_queue_splk_);
  tmp_queue.swap(wait_queue_);

  while (!tmp_queue.empty()) {
    detail::FiberInterface* fi = &tmp_queue.front();
    tmp_queue.pop_front();
    active->ActivateOther(fi);
  }
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

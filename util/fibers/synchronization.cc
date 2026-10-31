// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/synchronization.h"

#include "base/logging.h"

namespace util {
namespace fb2 {

using namespace std;

std::cv_status EventCount::wait_until(uint32_t epoch,
                                      const std::chrono::steady_clock::time_point& tp) noexcept {
  detail::FiberInterface* active = detail::FiberActive();

  cv_status status = cv_status::no_timeout;

  CHECK(!active->IsScheduledRemotely());
  std::unique_lock lk(lock_);
  if ((val_.load(std::memory_order_relaxed) >> kEpochShift) == epoch) {
    detail::Waiter waiter(active->CreateWaiter());

    wait_queue_.Link(&waiter);
    lk.unlock();

    bool timed_out = active->WaitUntil(tp);

    // We must protect wait_hook because we modify it in notification thread.
    lk.lock();
    if (waiter.IsLinked()) {
      DCHECK(!wait_queue_.empty());

      // We were woken up by timeout, lets remove ourselves from the queue.
      wait_queue_.Unlink(&waiter);
      DCHECK(timed_out);
      status = cv_status::timeout;
    } else if (timed_out) {  // we can still reach timeout even if we are not in the wait queue.
      status = cv_status::timeout;
    }
    lk.unlock();

    // A remote notifier may have detached us while we were timing out.
    active->AwaitRemoteWake();
  }
  return status;
}

void Mutex::lock() {
  detail::FiberInterface* active = detail::FiberActive();

  while (true) {
    detail::Waiter waiter(active->CreateWaiter());
    wait_queue_splk_.lock();
    if (nullptr == owner_) {
      DCHECK(!waiter.IsLinked());

      owner_ = active;
      wait_queue_splk_.unlock();
      return;
    }

    CHECK(active != owner_);
    wait_queue_.Link(&waiter);
    wait_queue_splk_.unlock();
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

void CondVarAny::PostWaitTimeout(detail::Waiter* waiter) {
  {
    std::unique_lock lk(lock_);
    if (waiter->IsLinked()) {
      wait_queue_.Unlink(waiter);
      return;
    }
  }

  // A remote notifier may have detached us while we were timing out.
  detail::FiberActive()->AwaitRemoteWake();
}

bool EmbeddedBlockingCounter::WaitUntil(const std::chrono::steady_clock::time_point tp) {
  uint64_t cnt;
  std::cv_status status = ec_.await_until(WaitCondition(&cnt), tp);
  return status == std::cv_status::no_timeout && (cnt & kCancelFlag) == 0;
}

void EmbeddedBlockingCounter::Start(unsigned cnt) {
  DCHECK_EQ(count_.load(memory_order_relaxed) & ~kCancelFlag, 0u);
  count_.store(cnt, std::memory_order_relaxed);
}

void EmbeddedBlockingCounter::Dec() {
  uint64_t prev = count_.fetch_sub(1, std::memory_order_acq_rel);
  DCHECK_GT(prev, 0u);
  if (prev == 1)
    ec_.notifyAll();
}

void EmbeddedBlockingCounter::Cancel() {
  count_.fetch_or(kCancelFlag, std::memory_order_acq_rel);
  ec_.notifyAll();
}

std::optional<EventCount::SubKey> EmbeddedBlockingCounter::OnCompletion(detail::Waiter* w) {
  uint64_t v = 0;
  return ec_.check_or_subscribe(WaitCondition(&v), w);
}

bool EmbeddedBlockingCounter::IsCompleted() const {
  uint64_t v = 0;
  bool result = WaitCondition(&v)();
  if (result)  // acquire semantics for "if completed, then action"
    std::atomic_thread_fence(std::memory_order_acquire);
  return result;
}

uint64_t EmbeddedBlockingCounter::DEBUG_Count() const {
  return count_.load(memory_order_relaxed);
}

BlockingCounter::BlockingCounter(unsigned start_count)
    : counter_{std::make_shared<EmbeddedBlockingCounter>(start_count)} {
}

FiberBlockingCounter::FiberBlockingCounter(unsigned start_count) : owner_{detail::FiberActive()} {
  owner_->blocking_counter().Start(start_count);
}

void FiberBlockingCounter::Release() && {
  DCHECK(owner_);
  auto owner = std::move(owner_);

  // Note: Wait() does not guarantee that "owner" refcount will decrease atomically together
  // with blocking_counter. In other words DEBUG_use_count() is
  // eventually consistent and may read a higher value right after Wait() resumes.
  owner->blocking_counter().Dec();
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

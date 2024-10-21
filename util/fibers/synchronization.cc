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
    bool clear_remote = true;

    // We must protect wait_hook because we modify it in notification thread.
    lk.lock();
    if (waiter.IsLinked()) {
      DCHECK(!wait_queue_.empty());

      // We were woken up by timeout, lets remove ourselves from the queue.
      wait_queue_.Unlink(&waiter);
      DCHECK(timed_out);
      status = cv_status::timeout;
      clear_remote = false;
    } else if (timed_out) {  // we can still reach timeout even if we are not in the wait queue.
      status = cv_status::timeout;
    }
    lk.unlock();

    // We must pull ourselves from the scheduler's remote_ready_queue in case we are there.
    if (clear_remote) {
      active->PullMyselfFromRemoteReadyQueue();
    } else {
      CHECK(!active->IsScheduledRemotely());
    }
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

std::cv_status CondVarAny::PostWaitTimeout(detail::Waiter waiter, bool timed_out,
                                           detail::FiberInterface* active) {
  std::cv_status status = std::cv_status::no_timeout;
  bool clear_remote = true;

  // Is called under the external lock so it's safe to access wait_queue_ here.
  if (waiter.IsLinked()) {
    wait_queue_.Unlink(&waiter);

    status = std::cv_status::timeout;
    clear_remote = false;
  } else if (timed_out) {
    status = std::cv_status::timeout;
  }

  if (clear_remote) {
    active->PullMyselfFromRemoteReadyQueue();
  } else {
    CHECK(!active->IsScheduledRemotely());
  }
  return status;
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

uint64_t EmbeddedBlockingCounter::DEBUG_Count() const {
  return count_.load(memory_order_relaxed);
}

BlockingCounter::BlockingCounter(unsigned start_count)
    : counter_{std::make_shared<EmbeddedBlockingCounter>(start_count)} {
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

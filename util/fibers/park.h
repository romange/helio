// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <utility>

#include "util/fibers/detail/fiber_interface.h"

namespace util {
namespace fb2 {

// Parking primitives for state protected by a caller-owned, non-sleeping lock (e.g. a spinlock).
//
// The caller's lock is the only synchronization domain: waiters publish themselves and wakers
// detach them under that lock. Activations are collected in a WakeList and run after the lock is
// released, so no cross-thread wakeup (remote queue push, proactor notify, possibly a syscall)
// ever runs inside the critical section.
//
// Protocol:
//   Waiter:  lock; publish `node` (ParkQueue::Push or a pointer in the protected state); unlock;
//            node.Park();
//   Waker:   WakeList wakes;      // declared before the lock guard: destroyed after it
//            lock; detach nodes into `wakes`; unlock;   // ~WakeList wakes them
//
// Rules:
//   1. A published node is woken exactly once, by the party that detached it under the lock.
//   2. Park() suspends unconditionally. It never re-checks a predicate and has no spurious
//      wakeups, so a waiter must not return without being woken. A wake that arrives before
//      Park() suspends is queued, not lost, provided the waiter does not yield or suspend
//      between publishing the node and calling Park(): a fiber that sits in its ready queue
//      absorbs a remote wake without being queued again.
//   3. A node may be destroyed as soon as its fiber runs. WakeList reads everything it needs
//      before activating a node and never touches it afterwards.
//   4. Writes made by the waker before Wake() are visible to the woken fiber after Park().
//      This allows direct handoff: a waker may transfer ownership on the waiter's behalf.
//
// Timeouts and cancellation are not supported.

// A handle to one fiber that can be parked. Binds the constructing fiber and must stay alive
// until Park() returns. Users may derive from it to pass handoff data from the waker to the waiter.
class ParkHandle {
 public:
  ParkHandle() noexcept : fiber_(detail::FiberActive()) {
  }

  ParkHandle(const ParkHandle&) = delete;
  ParkHandle& operator=(const ParkHandle&) = delete;

  // Suspends the calling fiber until this handle is woken. Must be called by the bound fiber
  // after publishing the node and releasing every lock that the waker needs.
  void Park() noexcept;

 private:
  friend class WakeList;

  detail::FiberInterface* fiber_;
};

// Intrusive queue node for a ParkHandle.
class ParkNode : public ParkHandle {
 public:
  // With `wake_to_front`, WakeList queues the woken fiber ahead of the ready fibers of its
  // priority on its thread instead of behind them. Use it for waiters that others wait for, such
  // as a fiber that is handed a lock when it is woken.
  explicit ParkNode(bool wake_to_front = false) noexcept : wake_to_front_(wake_to_front) {
  }

  ParkNode(const ParkNode&) = delete;
  ParkNode& operator=(const ParkNode&) = delete;

 private:
  friend class ParkQueue;
  friend class WakeList;

  ParkNode* next_ = nullptr;
  bool wake_to_front_ = false;
};

// Activations collected while a lock is held. Not thread-safe; usually lives on the waker's
// stack. Wake() runs them, and the destructor calls Wake(), so declare the list before the lock
// guard that protects the detached nodes. For example:
// {
//   SpinLock sp;
//   WakeList wakes;
//   {
//     std::lock_guard lock(m);
//     NotifySome(&wakes);
//     // detach nodes into wakes
//   }
//   // wakes are activated here, outside the lock
// }
//
class WakeList {
 public:
  WakeList() = default;

  WakeList(const WakeList&) = delete;
  WakeList& operator=(const WakeList&) = delete;

  ~WakeList() {
    Wake();
  }

  bool empty() const noexcept {
    return head_ == nullptr;
  }

  // Adds a detached node. Order of activation within the list is unspecified.
  void Add(ParkNode* node) noexcept {
    assert(node->next_ == nullptr);
    node->next_ = head_;
    head_ = node;
  }

  // Activates every collected node. Call with no lock held that the woken fibers need.
  void Wake() noexcept;

 private:
  friend class ParkQueue;

  ParkNode* head_ = nullptr;
};

// FIFO of parked nodes. Not thread-safe: protected by the caller's lock.
class ParkQueue {
 public:
  ParkQueue() = default;

  ParkQueue(const ParkQueue&) = delete;
  ParkQueue& operator=(const ParkQueue&) = delete;

  ~ParkQueue() {
    assert(empty());
  }

  bool empty() const noexcept {
    return head_ == nullptr;
  }

  void Push(ParkNode* node) noexcept {
    assert(node->next_ == nullptr);
    if (tail_) {
      tail_->next_ = node;
    } else {
      head_ = node;
    }
    tail_ = node;
  }

  // Detaches the oldest node, or returns nullptr if the queue is empty.
  ParkNode* Pop() noexcept {
    ParkNode* node = head_;
    if (node) {
      head_ = std::exchange(node->next_, nullptr);
      if (!head_)
        tail_ = nullptr;
    }
    return node;
  }

  // Detaches the oldest node into `wakes`. Returns false if the queue is empty.
  bool WakeOne(WakeList* wakes) noexcept {
    ParkNode* node = Pop();
    if (node)
      wakes->Add(node);
    return node != nullptr;
  }

  // Detaches every node into `wakes` in O(1) by splicing the chain in front of the list's nodes.
  // Only the tail node is touched, and only if `wakes` is not empty: queued nodes live on other
  // fibers' stacks, so walking them under the caller's lock would incur a cache miss per node.
  void WakeAll(WakeList* wakes) noexcept {
    if (!head_)
      return;
    if (wakes->head_)
      tail_->next_ = wakes->head_;
    wakes->head_ = std::exchange(head_, nullptr);
    tail_ = nullptr;
  }

 private:
  ParkNode* head_ = nullptr;
  ParkNode* tail_ = nullptr;
};

}  // namespace fb2
}  // namespace util

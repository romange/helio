// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <cassert>
#include <cstdint>
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
  friend class LocalityParkQueue;

  detail::FiberInterface* fiber_;
};

// Intrusive queue node for a ParkHandle.
class ParkNode : public ParkHandle {
 public:
  ParkNode() noexcept = default;

  ParkNode(const ParkNode&) = delete;
  ParkNode& operator=(const ParkNode&) = delete;

 private:
  friend class ParkQueue;
  friend class LocalityParkQueue;
  friend class WakeList;

  ParkNode* next_ = nullptr;
};

// Intrusive queue node for LocalityParkQueue.
class LocalityParkNode : public ParkNode {
 public:
  LocalityParkNode() noexcept = default;

 private:
  friend class LocalityParkQueue;

  // Push time of the successor in its FIFO. Popping this node reads the new head's push time
  // from here, without touching the new head.
  uint64_t next_push_cycles_ = 0;
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
  friend class LocalityParkQueue;

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

// Parked nodes in one FIFO per thread. Not thread-safe: protected by the caller's lock.
//
// With direct handoff, a node woken on another thread holds the handed-off resource unused until
// that thread pulls its remote ready queue between scheduler rounds. PopNear() therefore prefers
// a node of the waker's own thread, which needs no cross-thread activation, and bounds the
// unfairness: it takes the globally oldest node once that node has waited `max_wait_usec`, or
// after it was bypassed `max_bypass` times in a row.
//
// Push() records the waiter's thread and push time. Each FIFO's head push time lives in the queue
// object and the successor's push time in its predecessor, so no operation walks queued nodes or
// touches a node other than the popped head and the tail it links to. A FIFO holds only nodes of
// one thread, so Push(), and PopNear() when it takes a near node, touch only nodes on the stacks
// of the caller's thread. Every operation scans the non-empty FIFOs, at most one per thread.
//
// A near node still runs only after the waker and the fibers ahead of it suspend, so preferring
// it can serialize work on the waker's thread. util/fibers/park_bench.cc compares the policies.
class LocalityParkQueue {
 public:
  explicit LocalityParkQueue(uint32_t max_wait_usec = 50, uint32_t max_bypass = 16) noexcept
      : max_wait_usec_(max_wait_usec), max_bypass_(max_bypass) {
  }

  LocalityParkQueue(const LocalityParkQueue&) = delete;
  LocalityParkQueue& operator=(const LocalityParkQueue&) = delete;

  ~LocalityParkQueue() {
    assert(empty());
  }

  bool empty() const noexcept {
    return lanes_.empty();
  }

  // Must be called by the fiber bound to `node`.
  void Push(LocalityParkNode* node) noexcept;

  // Detaches the oldest node, or returns nullptr if the queue is empty.
  LocalityParkNode* Pop() noexcept;

  // Detaches a node of the calling thread if the fairness bounds allow it, and the oldest node
  // otherwise. Returns nullptr if the queue is empty.
  LocalityParkNode* PopNear() noexcept;

  bool WakeOne(WakeList* wakes) noexcept {
    return Wake(Pop(), wakes);
  }

  bool WakeNear(WakeList* wakes) noexcept {
    return Wake(PopNear(), wakes);
  }

  // Detaches every node into `wakes`. Touches the tail node of each FIFO.
  void WakeAll(WakeList* wakes) noexcept;

 private:
  struct Lane {
    detail::Scheduler* sched;
    LocalityParkNode* head;
    LocalityParkNode* tail;
    uint64_t head_push_cycles;
  };

  static bool Wake(LocalityParkNode* node, WakeList* wakes) noexcept {
    if (node)
      wakes->Add(node);
    return node != nullptr;
  }

  unsigned OldestLane() const noexcept;
  LocalityParkNode* PopLane(unsigned index) noexcept;

  // Non-empty FIFOs, in no particular order. Grows past the inline capacity only when more
  // threads have waiters at once, and then allocates under the caller's lock.
  absl::InlinedVector<Lane, 4> lanes_;
  uint32_t max_wait_usec_;
  uint32_t max_bypass_;
  uint32_t bypasses_ = 0;  // Consecutive PopNear() calls that skipped the oldest node.
};

}  // namespace fb2
}  // namespace util

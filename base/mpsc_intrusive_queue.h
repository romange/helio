// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstddef>
#include <utility>

namespace base {

// a MPSC queue where multiple threads push and a single thread pops.
//
// Requires global functions for T:
//
// T* MPSC_intrusive_load_next(const T& src)
// void MPSC_intrusive_store_next(T* dest, T* next_node);
//
// based on the design from here:
// https://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
// Also see: https://int08h.com/post/ode-to-a-vyukov-queue/
template <typename T> class MPSCIntrusiveQueue {
 private:
  static constexpr size_t cache_alignment = 64;
  static constexpr size_t cacheline_length = 64;

  // It is the first item popped from the queue.
  alignas(cache_alignment) T* head_;

  char pad_[cacheline_length];

  // The tail of the queue. It is the last item pushed to the queue.
  alignas(cache_alignment) std::atomic<T*> tail_;

  alignas(cache_alignment) typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_{};

  T* stub() {
    return reinterpret_cast<T*>(&storage_);
  }

 public:
  MPSCIntrusiveQueue() : head_{stub()}, tail_{stub()} {
    MPSC_intrusive_store_next(head_, nullptr);
  }

  MPSCIntrusiveQueue(MPSCIntrusiveQueue const&) = delete;
  MPSCIntrusiveQueue& operator=(MPSCIntrusiveQueue const&) = delete;

  // Pushes an item to the queue on producer thread.
  // The queue grows from the tail.
  void Push(T* item) noexcept {
    // item becomes a new tail.
    MPSC_intrusive_store_next(item, nullptr);

    // we need it to write with at least `release` MO, so that this write won't be reordered
    // before resetting item.next above. Otherwise, another producer could push its own item
    // after this CAS, and its item.next will be overriden.
    T* prev = tail_.exchange(item, std::memory_order_acq_rel);

    // link the previous tail to the new tail.
    // (*) Also a potential blocking point!
    // For more details see the linked article above!
    // Until (*) completes, the chain is cut at `prev` and Pop can not reach the item
    // and its subsequent items.
    MPSC_intrusive_store_next(prev, item);  // (*)
  }

  // Pops the first item at the head or returns nullptr if the queue is empty.
  T* Pop() noexcept {
    while (true) {
      auto [elem, empty] = PopWeak();
      if (elem || empty)
        return elem;
    }
    return nullptr;
  }

  // Returns an item at the head if exists and a status whether the queue is empty.
  // There can be a state where the queue is in the middle of Push transaction
  // and Pop can not pull the element yet. In that case, PopWeak returns {null, false}.
  std::pair<T*, bool> PopWeak() noexcept;

  // Can be run only on a consumer thread.
  bool Empty() const noexcept {
    T* head = head_;
    T* next = MPSC_intrusive_load_next(*head);

    return reinterpret_cast<const T*>(&storage_) == head && next == nullptr;
  }
};

template <typename T> std::pair<T*, bool> MPSCIntrusiveQueue<T>::PopWeak() noexcept {
  // Unlike with tail_, this is the only thread that touches head_
  T* head = head_;

  T* next = MPSC_intrusive_load_next(*head);  // load(std::memory_order_acquire)
  if (stub() == head) {
    if (nullptr == next) {
      // Empty state.
      // Caveat: if Push() called on an empty queue but has not crossed the blocking point yet,
      // we may reach this condition because head_ is a stub and stub.next is nullptr.
      // Unfortunately it may lead to a bizarre scenario where the arbitrary number of
      // subsequent pushes will fully complete, but the queue will still be observerd
      // as empty by the consumer because the chain will be cut by the Push that is stuck updating
      // the stub.
      //
      // More comments: if we had a single Push that is not completed yet, then returning
      // an empty state is fine. The problem arises when we have multiple pushes in parallel,
      // the first one has not completed yet, others completed but they are absolutely
      // invisible to the consumer.
      //
      // To disambiguite, we load the tail_ and check if it is the same as the head.
      // To sum up:
      // 1. if tail is not head, it is quaranteed that the queue is not empty.
      // 2. Otherwise, it's most likely empty, due to eventual consistency semantics of
      //    load/store operations.
      // 3. If store is guaranted to be visible due to external conditions, (2) becomes exact.
      T* tail = tail_.load(std::memory_order_relaxed);
      return {nullptr, tail == head};
    }

    // skip the stub if needed and continue.
    head_ = next;
    head = next;
    next = MPSC_intrusive_load_next(*next);
  }

  if (nullptr != next) {
    // non-empty
    head_ = next;
    return {head, false};
  }

  T* tail = tail_.load(std::memory_order_relaxed);
  if (tail != head) {
    // non-empty, we are in the middle of push - see a blocking point above.
    return {nullptr, false};
  }

  // tail and head are the same, pointing to the last element in the queue.
  // Link stub to the tail to introduce an empty state. Before the tail_.load above:
  // head -> item, tail -> item
  Push(stub());

  // Unless we had concurrent pushes, now:
  // head->item, item.next=stub
  // tail->stub, stub.next = nullptr
  next = MPSC_intrusive_load_next(*head);
  if (nullptr != next) {
    head_ = next;
    return {head, false};
  }

  // non-empty, we are still adding.
  return {nullptr, false};
}

}  // namespace base

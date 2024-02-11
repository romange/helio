// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstddef>

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
    T* prev = tail_.exchange(item, std::memory_order_acq_rel);

    // link the previous tail to the new tail. Also a potential blocking point.
    MPSC_intrusive_store_next(prev, item);
  }

  // Poops the first item at the head or returns nullptr if the queue is empty.
  T* Pop() noexcept;

  // Can be run only on a consumer thread.
  bool Empty() const noexcept {
    T* head = head_;
    T* next = MPSC_intrusive_load_next(*head);

    return reinterpret_cast<const T*>(&storage_) == head && next == nullptr;
  }
};

template <typename T> T* MPSCIntrusiveQueue<T>::Pop() noexcept {
  T* head = head_;

  //  head->next_.load(std::memory_order_acquire);
  T* next = MPSC_intrusive_load_next(*head);
  if (stub() == head) {
    if (nullptr == next) {
      // empty
      return nullptr;
    }
    head_ = next;
    head = next;
    next = MPSC_intrusive_load_next(*next);
  }

  if (nullptr != next) {
    // non-empty
    head_ = next;
    return head;
  }

  T* tail = tail_.load(std::memory_order_acquire);
  if (tail != head) {
    // non-empty, we are in the middle of push - see a blocking point above.
    return nullptr;
  }

  // tail and head are the same, pointing to the last element in the queue.
  // Link stub to the tail to introduce an empty state.
  Push(stub());

  next = MPSC_intrusive_load_next(*head);
  if (nullptr != next) {
    head_ = next;
    return head;
  }

  // non-empty, we are still adding.
  return nullptr;
}

}  // namespace base

// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/context.hpp>

#include "base/ProducerConsumerQueue.h"
#include "base/mpmc_bounded_queue.h"
#include "util/fibers/event_count.h"
#include "util/fibers/fibers_ext.h"

namespace util {
namespace fibers_ext {

namespace detail {
template <typename Q> class QueueTraits;
}  // namespace detail

/*!
  \brief Thread-safe, fiber-friendly channel. Can be SPSC or MPMC depending on the underlying
         queue implementation.

  Fiber friendly - means that multiple fibers within a single thread at each end-point
  can use the channel: K fibers from producer thread can push and N fibers from consumer thread
  can pull the records. It has optional blocking interface that suspends blocked fibers upon
  empty/full conditions. This class designed to be pretty efficient by reducing the contention
  on its synchronization primitives to minimum. It assumes that number of producer
  threads is known in advance. The queue is in Closing state when all the producers called
  StartClosing();
*/
template <typename T, typename Queue = folly::ProducerConsumerQueue<T>> class SimpleChannel {
  using QTraits = detail::QueueTraits<Queue>;

 public:
  SimpleChannel(size_t n, unsigned num_producers = 1) : q_(n), num_producers_(num_producers) {
  }

  template <typename... Args> void Push(Args&&... recordArgs) noexcept;

  // Blocking call. Returns false if channel is closed, true otherwise with the popped value.
  bool Pop(T& dest);

  /*! /brief Should be called only from the producer side.

      Signals the consumers that the channel is going to be close.
      Consumers may still pop the existing items until Pop() return false.
      This function does not block, only puts the channel into closing state.
      It's responsibility of the caller to wait for the consumers to empty the remaining items
      and stop using the channel.
  */
  void StartClosing();

  //! Non blocking push.
  template <typename... Args> bool TryPush(Args&&... args) noexcept {
    if (QTraits::TryEnqueue(q_, std::forward<Args>(args)...)) {
      if (true /* ++throttled_pushes_ > q_.capacity() / 3*/) {
        pop_ec_.notify();
        throttled_pushes_ = 0;
      }
      return true;
    }
    return false;
  }

  //! Non blocking pop.
  bool TryPop(T& val) {
    if (QTraits::TryDequeue(q_, val)) {
      return true;
    }
    push_ec_.notify();
    return false;
  }

  bool IsClosing() const {
    // It's safe to use relaxed due to monotonicity of is_closing_.
    return is_closing_.load(std::memory_order_relaxed) >= num_producers_;
  }

 private:
  Queue q_;
  unsigned throttled_pushes_ = 0;
  unsigned num_producers_;

  std::atomic_uint32_t is_closing_{0};

  // Event counts provide almost negligible contention during fast-path (a single atomic add).
  EventCount push_ec_, pop_ec_;
};

template <typename T, typename Q>
template <typename... Args>
void SimpleChannel<T, Q>::Push(Args&&... args) noexcept {
  if (TryPush(std::forward<Args>(args)...))  // fast path.
    return;

  while (true) {
    EventCount::Key key = push_ec_.prepareWait();
    if (TryPush(std::forward<Args>(args)...)) {
      break;
    }
    push_ec_.wait(key.epoch());
  }
}

template <typename T, typename Q> bool SimpleChannel<T, Q>::Pop(T& dest) {
  if (TryPop(dest))  // fast path
    return true;

  while (true) {
    EventCount::Key key = pop_ec_.prepareWait();
    if (TryPop(dest)) {
      return true;
    }

    if (IsClosing()) {
      return false;
    }

    pop_ec_.wait(key.epoch());
  }
}

template <typename T, typename Q> void SimpleChannel<T, Q>::StartClosing() {
  is_closing_.fetch_add(1, std::memory_order_acq_rel);
  pop_ec_.notifyAll();
}

namespace detail {

template <typename T> class QueueTraits<folly::ProducerConsumerQueue<T>> {
  using Queue = folly::ProducerConsumerQueue<T>;

 public:
  template <typename... Args> static bool TryEnqueue(Queue& q, Args&&... args) noexcept {
    return q.write(std::forward<Args>(args)...);
  }

  static bool TryDequeue(Queue& q, T& val) noexcept {
    return q.read(val);
  }
};

template <typename T> class QueueTraits<base::mpmc_bounded_queue<T>> {
  using Queue = base::mpmc_bounded_queue<T>;

 public:
  template <typename... Args> static bool TryEnqueue(Queue& q, Args&&... args) noexcept {
    return q.try_enqueue(std::forward<Args>(args)...);
  }

  static bool TryDequeue(Queue& q, T& val) noexcept {
    return q.try_dequeue(val);
  }
};

}  // namespace detail

}  // namespace fibers_ext
}  // namespace util

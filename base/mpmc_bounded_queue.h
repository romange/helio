/*
http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue

licensed by Dmitry Vyukov under the terms below:

Simplified BSD license

Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.
Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of
conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list
of conditions and the following disclaimer in the documentation and/or other materials
provided with the distribution.
THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
SHALL DMITRY VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
The views and conclusions contained in the software and documentation are those of the authors and
should not be interpreted as representing official policies, either expressed or implied, of Dmitry
Vyukov.

Minor changes by Roman Gershman (romange@gmail.com)
*/

#pragma once

#include <atomic>
#include <memory>
#include <stdexcept>

namespace base {

template <typename T> class mpmc_bounded_queue {
 public:
  using item_type = T;

  explicit mpmc_bounded_queue(size_t buffer_size)
      : buffer_(new cell_t[buffer_size]), buffer_mask_(buffer_size - 1) {
    // queue size must be power of two
    if (buffer_size < 2 || (buffer_size & (buffer_size - 1)) != 0)
      throw std::runtime_error("async logger queue size must be power of two");

    for (size_t i = 0; i != buffer_size; i += 1)
      buffer_[i].sequence.store(i, std::memory_order_relaxed);

    enqueue_pos_.store(0, std::memory_order_relaxed);
    dequeue_pos_.store(0, std::memory_order_relaxed);
  }

  mpmc_bounded_queue(mpmc_bounded_queue const&) = delete;
  mpmc_bounded_queue& operator=(mpmc_bounded_queue const&) = delete;

  ~mpmc_bounded_queue() {
    for (;;) {
      size_t pos = dequeue_pos_.fetch_add(1, std::memory_order_relaxed);
      cell_t& cell = buffer_[pos & buffer_mask_];
      size_t seq = cell.sequence.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
      if (dif < 0) {
        return;
      }
      reinterpret_cast<T*>(&cell.storage)->~T();
    }
  }

  // It's super important to leave try_enqueue as a template function of free type U.
  // Otherwise, moveable objects of different from T type (i.e. U) that can be
  // moved into T will be moved regardless if try_enqueue succeeds.
  // With this signature it's guaranteed that data is moved only if it's stored in the queue.
  // Added bonus, it seems we do not need the "const T&" version of this function.
  template <typename U> bool try_enqueue(U&& data) {
    size_t pos;
    cell_t* cell;

    while (true) {
      pos = enqueue_pos_.load(std::memory_order_relaxed);
      cell = &buffer_[pos & buffer_mask_];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)(pos);
      if (dif == 0) {  // available cell.
        // advance enque index.
        if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
          break;
      } else if (dif < 0) {
        return false;  // the queue is full.
      }
    }

    new (&cell->storage) T(std::forward<U>(data));
    cell->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  bool try_dequeue(T& data) {
    cell_t* cell;
    size_t pos;

    for (;;) {
      pos = dequeue_pos_.load(std::memory_order_relaxed);
      cell = &buffer_[pos & buffer_mask_];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
      if (dif == 0) {
        if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
          break;
      } else if (dif < 0) {
        return false;  // the queue is empty.
      }
    }

    data = std::forward<T>(reinterpret_cast<T&>(cell->storage));

    // Commit transaction, free up the cell.
    cell->sequence.store(pos + buffer_mask_ + 1, std::memory_order_release);
    return true;
  }

  size_t capacity() const {
    return buffer_mask_ + 1;
  }

  // Represents a point in time. The state could change one cpu cycle later.
  // For single producer cases if a queue is empty it will stay empty until the producer enqueue
  // into it. For single consumer cases a queue that is not empty will stay not empty until it
  // deques from it.
  bool empty() const {
    size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
    cell_t* cell = &buffer_[pos & buffer_mask_];
    size_t seq = cell->sequence.load(std::memory_order_relaxed);
    intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
    return dif < 0;
  }

 private:
  struct cell_t {
    std::atomic<size_t> sequence;
    std::aligned_storage_t<sizeof(T)> storage;
  };

  typedef char cacheline_pad_t[64];

  cacheline_pad_t pad0_;
  std::unique_ptr<cell_t[]> buffer_;
  size_t const buffer_mask_;
  cacheline_pad_t pad1_;
  std::atomic<size_t> enqueue_pos_;
  cacheline_pad_t pad2_;
  std::atomic<size_t> dequeue_pos_;
  cacheline_pad_t pad3_;
};

}  // namespace base

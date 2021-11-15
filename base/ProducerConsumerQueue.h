/*
 * Copyright 2014 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// @author Bo Hu (bhu@fb.com)
// @author Jordan DeLong (delong.j@fb.com)
// Cosmetics changes - by Roman Gershman romange@gmail.com
//
#pragma once

#include <atomic>
#include <cassert>
#include <memory>
#include <type_traits>
#include <utility>

namespace folly {

/*
 * ProducerConsumerQueue is a one producer and one consumer queue
 * without locks.
 */
template <typename T> class ProducerConsumerQueue {
 public:
  typedef T value_type;
  typedef ::std::allocator<T> allocator_type;

  // size must be >= 2.
  //
  // Also, note that the number of usable slots in the queue at any
  // given time is actually (size-1), so if you start with an empty queue,
  // isFull() will return true after size-1 insertions.
  explicit ProducerConsumerQueue(uint32_t size);

  ~ProducerConsumerQueue() {
    // We need to destruct anything that may still exist in our queue.
    // (No real synchronization needed at destructor time: only one
    // thread can be doing this.)
    destroy();

    alloc_.deallocate(records_, size_);
  }

  template <class... Args> bool write(Args&&... recordArgs) noexcept {
    auto const currentWrite = writeIndex_.load(std::memory_order_relaxed);
    auto nextRecord = currentWrite + 1;
    if (nextRecord == size_) {
      nextRecord = 0;
    }
    if (nextRecord != readIndex_.load(std::memory_order_acquire)) {
      std::allocator_traits<allocator_type>::construct(alloc_, &records_[currentWrite],
                                                       std::forward<Args>(recordArgs)...);
      writeIndex_.store(nextRecord, std::memory_order_release);
      return true;
    }

    // queue is full
    return false;
  }

  // move (or copy) the value at the front of the queue to given variable
  bool read(T& record) noexcept {
    auto const currentRead = readIndex_.load(std::memory_order_relaxed);
    if (currentRead == writeIndex_.load(std::memory_order_acquire)) {
      // queue is empty
      return false;
    }

    auto nextRecord = currentRead + 1;
    if (nextRecord == size_) {
      nextRecord = 0;
    }
    record = std::move(records_[currentRead]);
    std::allocator_traits<allocator_type>::destroy(alloc_, &records_[currentRead]);
    readIndex_.store(nextRecord, std::memory_order_release);
    return true;
  }

  // pointer to the value at the front of the queue (for use in-place) or
  // nullptr if empty.
  T* frontPtr() {
    auto const currentRead = readIndex_.load(std::memory_order_relaxed);
    if (currentRead == writeIndex_.load(std::memory_order_acquire)) {
      // queue is empty
      return nullptr;
    }
    return &records_[currentRead];
  }

  // queue must not be empty
  void popFront() {
    auto const currentRead = readIndex_.load(std::memory_order_relaxed);
    assert(currentRead != writeIndex_.load(std::memory_order_acquire));

    auto nextRecord = currentRead + 1;
    if (nextRecord == size_) {
      nextRecord = 0;
    }
    std::allocator_traits<allocator_type>::destroy(alloc_, &records_[currentRead]);
    readIndex_.store(nextRecord, std::memory_order_release);
  }

  bool isEmpty() const noexcept {
    return readIndex_.load(std::memory_order_consume) ==
           writeIndex_.load(std::memory_order_consume);
  }

  bool isFull() const noexcept {
    auto nextRecord = writeIndex_.load(std::memory_order_consume) + 1;
    if (nextRecord == size_) {
      nextRecord = 0;
    }
    if (nextRecord != readIndex_.load(std::memory_order_consume)) {
      return false;
    }
    // queue is full
    return true;
  }

  // * If called by consumer, then true size may be more (because producer may
  //   be adding items concurrently).
  // * If called by producer, then true size may be less (because consumer may
  //   be removing items concurrently).
  // * It is undefined to call this from any other thread.
  size_t sizeGuess() const noexcept {
    int ret =
        writeIndex_.load(std::memory_order_consume) - readIndex_.load(std::memory_order_consume);
    if (ret < 0) {
      ret += size_;
    }
    return ret;
  }

  size_t capacity() const { return size_; }

 private:
  void destroy() {
    if (std::is_trivially_destructible<T>::value)
      return;
    uint32_t read = readIndex_;
    uint32_t end = writeIndex_;

    while (read != end) {
      std::allocator_traits<allocator_type>::destroy(alloc_, &records_[read]);
      if (++read == size_) {
        read = 0;
      }
    }
  }

  allocator_type alloc_;
  const uint32_t size_;
  T* const records_;

  std::atomic<uint32_t> readIndex_;
  std::atomic<uint32_t> writeIndex_;

  ProducerConsumerQueue(const ProducerConsumerQueue&) = delete;
  ProducerConsumerQueue& operator=(const ProducerConsumerQueue&) = delete;
};

template <typename T>
ProducerConsumerQueue<T>::ProducerConsumerQueue(uint32_t size)
    : size_(size), records_(alloc_.allocate(size)), readIndex_(0), writeIndex_(0) {
  assert(size >= 2);
}

}  // namespace folly

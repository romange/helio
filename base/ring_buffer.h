// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>

namespace base {

// Simple, single-threaded ring buffer.
template <typename T> class RingBuffer {
 public:
  using value_type = T;

  // cap must be power of 2.
  RingBuffer(unsigned cap) : capacity_(cap) {
    buf_.reset(new T[cap]);
    mask_ = cap - 1;
  }

  unsigned size() const {
    return tail_ - head_;
  }

  bool empty() const {
    return tail_ == head_;
  }

  // Try to inserts into tail of the buffer.
  template <typename U> bool TryEmplace(U&& u) {
    // due to how 2s compliment work, this check works even in case of overflows.
    // for example,
    constexpr unsigned kHead = UINT_MAX;
    constexpr unsigned kTail = kHead + 10;
    static_assert(kTail == 9);
    static_assert(kTail - kHead == 10);

    if (tail_ - head_ < capacity_) {
      buf_[tail_ & mask_] = std::forward<U>(u);
      ++tail_;
      return true;
    }
    return false;
  }

  // Inserts into tail. If buffer is full overrides the first inserted item at head
  // and keeps the ring at maximal capacity.
  // Returns true if an item was overriden and false otherwise.
  template <typename U> bool EmplaceOrOverride(U&& u) {
    buf_[tail_ & mask_] = std::forward<U>(u);
    ++tail_;
    if (tail_ <= head_ + capacity_) {
      return false;
    }

    ++head_;  // override.
    return true;
  }

  // Gets the pointer to the next tail entry, or null if buffer is full
  // advances the tail to the next position.
  T* GetTail() {
    T* res = nullptr;
    if (tail_ - head_ < capacity_) {
      res = buf_.get() + (tail_ & mask_);
      ++tail_;
    }
    return res;
  }

  // Tries to pull out of buffer's head.
  bool TryDeque(T& t) {
    if (size() == 0) {
      return false;
    }
    t = std::forward<T>(buf_[head_ & mask_]);
    ++head_;
    return true;
  }

  unsigned capacity() const {
    return capacity_;
  }

  // Zero-copy deque interface.
  // Returns pointer to array of length size().
  T* GetItem(unsigned index) {
    return buf_.get() + ((head_ + index) & mask_);
  }

  // Requires: n <= size()
  void ConsumeHead(unsigned n) {
    head_ += n;
  }

 private:
  unsigned capacity_;
  unsigned mask_;
  unsigned head_ = 0;
  unsigned tail_ = 0;

  std::unique_ptr<T[]> buf_;
};

}  // namespace base

// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cassert>
#include <climits>
#include <memory>
#include <utility>

namespace base {

// Simple, single-threaded ring buffer.
// T must be default constructible.
template <typename T> class RingBuffer {
 public:
  using value_type = T;

  explicit RingBuffer(unsigned cap) : capacity_(cap) {
    buf_.reset(new T[cap]);
    mask_ = cap - 1;
    // cap must be power of 2.
    assert((mask_ & capacity_) == 0);
  }

  unsigned size() const {
    return tail_ - head_;
  }

  [[nodiscard]] bool empty() const {
    return tail_ == head_;
  }

  [[nodiscard]] bool Full() const {
    return tail_ - head_ == capacity_;
  }

  // Try to inserts into tail of the buffer.
  template <typename U> bool TryEmplace(U&& u) {
    T* slot = GetTail();
    if (slot) {
      *slot = std::forward<U>(u);
    }
    return bool(slot);
  }

  // Inserts into tail. If buffer is full overrides the first inserted item at head
  // and keeps the ring at maximal capacity.
  // Returns true if an item was overriden and false otherwise.
  template <typename U> bool EmplaceOrOverride(U&& u) {
    bool is_full = Full();
    *GetTail(/*override=*/true) = std::forward<U>(u);
    return is_full;
  }

  // Gets a pointer to the next tail entry, if `override` is set, override existing
  // entries when needed.
  // Advances the tail to the next position.
  [[nodiscard]] T* GetTail(bool override = false) {
    T* res = nullptr;
    if (Full()) {
      if (override)
        head_++;
      else
        return res;
    }

    res = buf_.get() + (tail_ & mask_);
    ++tail_;
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

  T& operator[](unsigned index) {
    assert(index < size());
    return buf_[(head_ + index) & mask_];
  }

  const T& operator[](unsigned index) const {
    assert(index < size());
    return buf_[(head_ + index) & mask_];
  }

  // Requires: n <= size()
  void ConsumeHead(unsigned n) {
    assert(n <= size());
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

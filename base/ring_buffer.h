// Copyright 2021, Beeri 15.  All rights reserved.
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

 private:
  unsigned capacity_;
  unsigned mask_;
  unsigned head_ = 0;
  unsigned tail_ = 0;

  std::unique_ptr<T[]> buf_;
};

}  // namespace base

// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/numeric/bits.h>
#include <absl/types/span.h>

#include <cstring>

namespace base {

class IoBuf {
 public:
  IoBuf(const IoBuf&) = delete;
  IoBuf& operator=(const IoBuf&) = delete;

  using Bytes = absl::Span<uint8_t>;
  using ConstBytes = absl::Span<const uint8_t>;

  explicit IoBuf(size_t capacity = 256) {
    assert(capacity > 0);

    Reserve(capacity);
  }

  IoBuf(size_t capacity, std::align_val_t align) : alignment_(size_t(align)) {
    Reserve(capacity);
  }

  ~IoBuf() {
    delete[] buf_;
  }

  size_t Capacity() const {
    return capacity();
  }

  ConstBytes InputBuffer() const {
    return ConstBytes{buf_ + offs_, InputLen()};
  }

  Bytes InputBuffer() {
    return Bytes{buf_ + offs_, InputLen()};
  }

  size_t InputLen() const {
    return size_ - offs_;
  }

  void ConsumeInput(size_t offs);

  void ReadAndConsume(size_t sz, void* dest) {
    ConstBytes b = InputBuffer();
    assert(b.size() >= sz);
    memcpy(dest, b.data(), sz);
    ConsumeInput(sz);
  }

  Bytes AppendBuffer() {
    return Bytes{buf_ + size_, Available()};
  }

  void CommitWrite(size_t sz) {
    size_ += sz;
  }

  void Reserve(size_t sz) {
    if (sz < capacity_)
      return;

    sz = absl::bit_ceil(sz);
    uint8_t* nb = new (std::align_val_t{alignment_}) uint8_t[sz];
    if (buf_) {
      if (size_ > offs_) {
        memcpy(nb, buf_ + offs_, size_ - offs_);
        size_ -= offs_;
        offs_ = 0;
      } else {
        size_ = offs_ = 0;
      }
      delete[] buf_;
    }

    buf_ = nb;
    capacity_ = sz;
  }

 private:
  size_t Available() const {
    return capacity() - size_;
  }

  uint32_t capacity() const {
    return capacity_;
  }

  uint8_t* buf_ = nullptr;
  uint32_t offs_ = 0;
  uint32_t size_ = 0;
  uint32_t alignment_ = 8;
  uint32_t capacity_ = 0;
};

inline void IoBuf::ConsumeInput(size_t sz) {
  if (offs_ + sz >= size_) {
    size_ = 0;
    offs_ = 0;
  } else {
    offs_ += sz;
    if (2 * offs_ > size_ && size_ - offs_ < 512) {
      memcpy(buf_, buf_ + offs_, size_ - offs_);
      size_ -= offs_;
      offs_ = 0;
    }
  }
}

}  // namespace base

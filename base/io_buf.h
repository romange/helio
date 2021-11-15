// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/types/span.h>

#include <cstring>

#include "base/pod_array.h"
namespace base {

class IoBuf {
 public:
  using Bytes = absl::Span<uint8_t>;
  using ConstBytes = absl::Span<const uint8_t>;

  explicit IoBuf(size_t capacity = 256)  {
    buf_.reserve(capacity);
  }

  size_t Capacity() const {
    return buf_.capacity();
  }

  ConstBytes InputBuffer() const {
    return ConstBytes{buf_.begin() + offs_, InputLen()};
  }

  Bytes InputBuffer() {
    return Bytes{buf_.begin() + offs_, InputLen()};
  }

  size_t InputLen() const { return buf_.size() - offs_; }

  void ConsumeInput(size_t offs);

  void ReadAndConsume(size_t sz, void* dest) {
    ConstBytes b = InputBuffer();
    assert(b.size() >= sz);
    memcpy(dest, b.data(), sz);
    ConsumeInput(sz);
  }

  Bytes AppendBuffer() {
    return Bytes{buf_.end(), Available()};
  }

  void CommitWrite(size_t sz) {
     buf_.resize_assume_reserved(buf_.size() + sz);
  }

  void Reserve(size_t sz) {
    buf_.reserve(sz);
  }

 private:
  size_t Available() const {
    return buf_.capacity() - buf_.size();
  }

  base::PODArray<uint8_t, 8> buf_;
  size_t offs_ = 0;
};

inline void IoBuf::ConsumeInput(size_t sz) {
  if (offs_ + sz >= buf_.size()) {
    buf_.clear();
    offs_ = 0;
  } else {
    offs_ += sz;
    if (2 * offs_ > buf_.size() && buf_.size() - offs_ < 512) {
      memcpy(buf_.data(), buf_.data() + offs_, buf_.size() - offs_);
      buf_.resize(buf_.size() - offs_);
      offs_ = 0;
    }
  }
}

}  // namespace base

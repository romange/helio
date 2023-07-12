// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/io_buf.h"

namespace base {

IoBuf::~IoBuf() {
  operator delete[](buf_, std::align_val_t{alignment_});
}

void IoBuf::ConsumeInput(size_t sz) {
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

void IoBuf::ReadAndConsume(size_t sz, void* dest) {
  ConstBytes b = InputBuffer();
  assert(b.size() >= sz);
  memcpy(dest, b.data(), sz);
  ConsumeInput(sz);
}

void IoBuf::WriteAndCommit(const void* source, size_t num_write) {
  EnsureCapacity(num_write);
  memcpy(AppendBuffer().data(), source, num_write);
  CommitWrite(num_write);
}

void IoBuf::Reserve(size_t sz) {
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

void IoBuf::Swap(IoBuf& other) {
  std::swap(buf_, other.buf_);
  std::swap(offs_, other.offs_);
  std::swap(size_, other.size_);
  std::swap(alignment_, other.alignment_);
  std::swap(capacity_, other.capacity_);
}

};  // namespace base

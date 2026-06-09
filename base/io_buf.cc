// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/io_buf.h"

namespace base {

IoBuf::~IoBuf() {
  operator delete[](buf_, std::align_val_t{alignment_});
}

void IoBuf::ConsumeInput(size_t sz) {
  static constexpr size_t kCompactThreshold = 512;

  if (offs_ + sz >= size_) {
    size_ = 0;
    offs_ = 0;
  } else {
    offs_ += sz;
    // size_ == capacity_: buffer is completely full, AppendLen == 0. If the caller
    // needs to write more data into the buffer it will find no space. Compact immediately
    // when any bytes are consumed from a full buffer to restore append space.
    // The second condition alone cannot cover this: it requires remaining input < kCompactThreshold,
    // but a partial command tail can be arbitrarily large, so a full buffer with a large
    // tail would never compact, leaving AppendLen == 0 permanently.
    //
    // offs_ > size_ / 2 && size_ - offs_ < kCompactThreshold: opportunistic compaction when
    // consumed bytes outnumber remaining input bytes, AND remaining input is tiny.
    // Reclaims wasted space before it would require a reallocation.
    if ((size_ == capacity_) || ((offs_ > size_ / 2) && (size_ - offs_ < kCompactThreshold))) {
      Compact();
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
    ::operator delete[](buf_, std::align_val_t{alignment_});
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

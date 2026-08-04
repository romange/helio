// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/numeric/bits.h>
#include <absl/types/span.h>

#include <cassert>
#include <cstdint>
#include <cstring>

namespace base {

// Generic buffer for reads and writes.
// Write directly to AppendBuffer() and mark bytes as written with CommitWrite.
// Read from InputBuffer() and mark bytes as read with ConsumeInput.
//
// Here's how IoBuf looks in memory:
//
// buf_:    [  consumed  |  unread input  |  append room  ]
//          ^            ^                ^               ^
//          0           offs_            size_         capacity_
//
// InputBuffer(): immutable view [offs_, size_): bytes ready to parse.
// AppendBuffer(): mutable view [size_, capacity_): room for new reads or writes.
class IoBuf {
 public:
  using Bytes = absl::Span<uint8_t>;
  using ConstBytes = absl::Span<const uint8_t>;

  explicit IoBuf(size_t capacity = 256) {
    assert(capacity > 0);
    Reserve(capacity);
  }

  IoBuf(size_t capacity, std::align_val_t align) : alignment_(size_t(align)) {
    Reserve(capacity);
  }

  IoBuf(const IoBuf&) = delete;
  IoBuf& operator=(const IoBuf&) = delete;

  IoBuf(IoBuf&& other) {
    Swap(other);
  }
  IoBuf& operator=(IoBuf&& other) {
    Swap(other);
    return *this;
  }

  ~IoBuf();

  // ============== INPUT =======================

  size_t InputLen() const {
    return size_ - offs_;
  }

  ConstBytes InputBuffer() const {
    return ConstBytes{buf_ + offs_, InputLen()};
  }

  Bytes InputBuffer() {
    return Bytes{buf_ + offs_, InputLen()};
  }

  // Mark num_read bytes from the input as read.
  void ConsumeInput(size_t num_read);

  // Write num_write bytes to dest and mark them as read.
  void ReadAndConsume(size_t num_write, void* dest);

  // ============== OUTPUT ============

  size_t AppendLen() const {
    return capacity_ - size_;
  }

  Bytes AppendBuffer() {
    return Bytes{buf_ + size_, AppendLen()};
  }

  // Mark num_written bytes as written and transform them to input.
  void CommitWrite(size_t num_written) {
    size_ += num_written;
  }

  // Copy num_copy bytes from source to append buffer and mark them as written.
  // Ensures append buffer is large enough.
  void WriteAndCommit(const void* source, size_t num_copy);

  // Ensure required append buffer size without reallocating when the current append region fits.
  void EnsureCapacity(size_t sz) {
    if (sz > AppendLen()) {
      Reserve(size_ + sz);
    }
  }

  // Ensures the whole buffer has at least full_size capacity. An equal-size request (full_size is
  // equal to the current capacity) reallocates the raw buffer and compacts unread input. Use
  // EnsureCapacity for append space instead.
  void Reserve(size_t full_size);

  // Reduces capacity while preserving unread input. The new capacity is the smallest power of two
  // that is at least target_capacity; a zero target selects one byte. Returns false if the target
  // is at least the current capacity, the resulting capacity cannot hold InputLen(), or the target
  // rounds to the current capacity.
  [[nodiscard]] bool ShrinkTo(size_t target_capacity);

  // ============== GENERIC ===========

  // Clear all input.
  void Clear() {
    size_ = 0;
    offs_ = 0;
  }

  // Moves remaining input to the front of the buffer, setting offs_=0.
  // WARNING: invalidates any outstanding spans from InputBuffer() or AppendBuffer().
  //
  // Use this when the buffer is at max capacity (AppendLen==0) and
  // cannot grow further, but still hold unprocessed input that must not be discarded.
  // In that case neither ConsumeInput nor EnsureCapacity can help: Compact() is the
  // only way to reclaim the already-consumed space at the front without data loss
  // or new allocation.
  void Compact() {
    if (offs_ > 0) {
      assert(offs_ <= size_);
      memmove(buf_, buf_ + offs_, size_ - offs_);
      size_ -= offs_;
      offs_ = 0;
    }
  }

  // Return capacity of whole buffer.
  size_t Capacity() const {
    return capacity_;
  }

  // Returns a counter identifying the current raw buffer memory. It changes only when Reserve or
  // ShrinkTo replaces that memory.
  uint64_t generation() const {
    return generation_;
  }

  struct MemoryUsage {
    size_t consumed = 0;
    size_t input_length = 0;
    size_t append_length = 0;

    size_t GetTotalSize() const {
      return consumed + input_length + append_length;
    }

    MemoryUsage& operator+=(const MemoryUsage& o) {
      consumed += o.consumed;
      input_length += o.input_length;
      append_length += o.append_length;
      return *this;
    }
  };

  MemoryUsage GetMemoryUsage() const {
    return {
        .consumed = offs_,
        .input_length = InputLen(),
        .append_length = AppendLen(),
    };
  }

 private:
  void Reallocate(size_t new_capacity);
  void Swap(IoBuf& other);

  uint8_t* buf_ = nullptr;
  // Offset to unread input within buf_.
  size_t offs_ = 0;
  // Number of bytes written into buf_, including consumed input before offs_.
  size_t size_ = 0;
  size_t alignment_ = 8;
  // Total size of buf_.
  size_t capacity_ = 0;
  // Tracks raw buffer memory replacements.
  uint64_t generation_ = 0;
};

}  // namespace base

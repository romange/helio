// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/numeric/bits.h>
#include <absl/types/span.h>

#include <cstring>

namespace base {

// Generic buffer for reads and writes.
// Write directly to AppendBuffer() and mark bytes as written with CommitWrite.
// Read from InputBuffer() and mark bytes as read with ConsumeInput.
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

  // Ensure required append buffer size.
  void EnsureCapacity(size_t sz) {
    Reserve(size_ + sz);
  }

  // Reserve by whole buffer size.
  // Use EnsureCapacity instead for resizing only by desired append buffer size.
  void Reserve(size_t full_size);

  // ============== GENERIC ===========

  // Clear all input.
  void Clear() {
    size_ = 0;
    offs_ = 0;
  }

  // Return capacity of whole buffer.
  size_t Capacity() const {
    return capacity_;
  }

  struct MemoryUsage {
    size_t consumed = 0;
    size_t input_length = 0;
    size_t append_length = 0;

    size_t GetTotalSize() const { return consumed + input_length + append_length; }

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
  void Swap(IoBuf& other);

  uint8_t* buf_ = nullptr;
  size_t offs_ = 0;
  size_t size_ = 0;
  size_t alignment_ = 8;
  size_t capacity_ = 0;
};

}  // namespace base

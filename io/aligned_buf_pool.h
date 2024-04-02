// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/cleanup/cleanup.h>

#include <deque>
#include <memory>
#include <optional>

#include "io/io.h"

namespace io {

// Buffer aligned to 4kb that is optionally backed by a registered buffer (from register_buffers)
struct AlignedBuf {
  AlignedBuf(MutableBytes buf, std::optional<unsigned> registered_idx = std::nullopt)
      : buf(buf), registered_idx(registered_idx) {
  }

  const MutableBytes buf;                        // buf, nbytes
  const std::optional<unsigned> registered_idx;  // optional buf_index for prep_read/write_fixed
};

// Hands out cunks of a large registered buffer, falls back to allocating non-registered
// chunk if no free space is available.
struct AlignedBufPool {
  const static size_t kAlign = 4096;

  // Request aligned buffer. Returns buffer and cleanup that recycles the buffer once dropped.
  static auto Request(size_t length) {
    AlignedBuf buf = RequestChunk(length);
    return std::make_pair(buf, absl::MakeCleanup([ptr = buf.buf.data()] { ReturnChunk(ptr); }));
  }

  // Initialize buffer on current thread and register backing in io_uring
  static void Init(size_t backing_size);

  // Free resources on current thread
  static void Destroy();

 private:
  static AlignedBuf RequestChunk(size_t length /* in bytes */);
  static void ReturnChunk(void* ptr);

  std::optional<size_t /* offset in pages */> RequestSegment(size_t length /* in pages */);
  void ReturnSegment(size_t offset /* in pages */);

  // If the pointer belongs to the buffer, calculate offset
  std::optional<size_t> OffsetByPtr(const void* ptr);

 private:
  unsigned registered_id_ = 0;  // id from io_uring_register_buffers

  size_t buffer_size_ = 0;
  uint8_t* buffer_ = nullptr;  // allocated backing

  // List of borrowed segments, offset and size in aligned pages
  std::deque<std::pair<size_t /* offset */, size_t /* size */>> borrowed_;
};

}  // namespace io
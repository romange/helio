// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/cleanup/cleanup.h>

#include <cstdint>
#include <new>
#include <optional>
#include <system_error>

#include "base/segment_pool.h"
#include "io/io.h"

namespace util::fb2 {

// Aligned buffer that is optionally part of registered buffer (io_uring_register_buffers)
struct UringBuf {
  io::MutableBytes bytes;           // buf, nbytes
  std::optional<unsigned> buf_idx;  // buf_idx
};

// Thread local pool for UringBufs. Hands out registered while there is space, falls back to
// allocating temporary if none are available.
struct UringBufPool {
  const static size_t kAlign = 4096;

  static std::error_code Init(size_t lenght);
  static void Destroy();

  // Returns pair of buffer and cleanup that recycles buffer once dropped.
  static auto RequestTL(size_t length) {
    UringBuf buf = RequestTLInternal(length);
    auto clean_cb = [ptr = buf.bytes.data()] { ReturnTL(ptr); };
    return std::make_pair(buf, absl::Cleanup(clean_cb));
  }

 private:
  static UringBuf RequestTLInternal(size_t length);
  static void ReturnTL(uint8_t* ptr);

  UringBuf Request(size_t length);
  void Return(uint8_t* ptr);

 private:
  unsigned buf_idx_ = 0;
  uint8_t* backing_ = nullptr;
  base::SegmentPool segments_{};
};

}  // namespace util::fb2

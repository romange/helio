// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "io/io.h"

namespace util {
namespace fb2 {

// A slice that is sub-region of registered buffer (io_uring_register_buffers)
struct RegisteredSlice {
  io::MutableBytes bytes;  // buf, nbytes
  unsigned buf_idx;        // registered buffer id
};

}  // namespace fb2
}  // namespace util

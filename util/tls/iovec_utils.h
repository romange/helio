// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <sys/uio.h>

#include <cstddef>
#include <cstdint>

namespace util {

// Advances the iovec array pointer and decrements the count by skipping 'bytes_to_advance'.
// Modifies both the base pointer of the current iovec and the array cursor itself.
void AdvanceIovec(iovec** iov, uint32_t* len, size_t bytes_to_advance);

// Returns true if the iovec array is empty (len == 0) or if all entries
// within the array have a length of zero. Useful for early-exit validation.
bool IsEmptyIovec(const iovec* iov, uint32_t len);

// Calculates the sum of all iov_len fields in the array.
// Note: This returns the total byte count and does not check for overflow.
size_t GetIovecTotalBytes(const iovec* iov, uint32_t len);

}  // namespace util
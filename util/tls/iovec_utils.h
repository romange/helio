// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <sys/uio.h>

#include <cstddef>
#include <cstdint>

namespace util {

/**
 * @brief Advances the iovec iterator by the specified number of bytes.
 * @param[in,out] iov Pointer to the array pointer (will be incremented).
 * @param[in,out] len Pointer to the array count (will be decremented).
 * @param bytes_to_advance Number of bytes to skip.
 */
void AdvanceIovec(iovec** iov, uint32_t* len, size_t bytes_to_advance);

// Returns true if all entries in the iovec array have zero length or if the iovec length is zero.
// @param iov Pointer to the iovec array.
// @param len Number of entries in the iovec array.
bool IsEmptyIovec(const iovec* iov, uint32_t len);

}  // namespace util
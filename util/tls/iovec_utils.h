// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <cstddef>
#include <sys/uio.h>

namespace util {

/**
 * @brief Advances the iovec iterator by the specified number of bytes.
 * @param[in,out] iov Pointer to the array pointer (will be incremented).
 * @param[in,out] len Pointer to the array count (will be decremented).
 * @param bytes_to_advance Number of bytes to skip.
 */
void AdvanceIovec(iovec** iov, uint32_t* len, size_t bytes_to_advance);

} // namespace util
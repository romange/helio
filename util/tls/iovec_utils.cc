// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/tls/iovec_utils.h"

#include "base/logging.h"

namespace util {

void AdvanceIovec(iovec** iov, uint32_t* len, size_t bytes_to_advance) {
  DCHECK_NE(*iov, nullptr);
  DCHECK_GT(*len, 0u);

  while ((*len > 0) && (bytes_to_advance > 0)) {
    if (bytes_to_advance >= (*iov)->iov_len) {  // case 1: remove the entry
      bytes_to_advance -= (*iov)->iov_len;
      ++(*iov);
      --(*len);
    } else {  // case 2: adjust the current entry's start and length
      (*iov)->iov_base = static_cast<char*>((*iov)->iov_base) + bytes_to_advance;
      (*iov)->iov_len -= bytes_to_advance;
      bytes_to_advance = 0;
      break;
    }
  }
  CHECK_EQ(bytes_to_advance, 0u) << "AdvanceIovec logic error: unconsumed bytes remaining or "
                                    "bytes_to_advance is larger than total iovec size";
}

bool IsEmptyIovec(const iovec* iov, uint32_t len) {
  DCHECK_NE(iov, nullptr);
  for (size_t i{}; i < len; ++i) {
    if (iov[i].iov_len != 0) {
      return false;
    }
  }
  return true;
}

size_t GetIovecTotalBytes(const iovec* iov, uint32_t len) {
  DCHECK_NE(iov, nullptr);
  size_t total_bytes{};
  for (size_t i{}; i < len; ++i) {
    size_t prev = total_bytes;
    total_bytes += iov[i].iov_len;
    CHECK(total_bytes >= prev) << "Overflow detected in GetIovecTotalBytes";
  }
  return total_bytes;
}

}  // namespace util
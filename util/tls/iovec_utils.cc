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
  DCHECK_EQ(bytes_to_advance, 0u) << "AdvanceIovec logic error: unconsumed bytes remaining";
}

} // namespace util
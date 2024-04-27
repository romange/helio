// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>
#include <optional>

namespace base {

// Simple pool for borrowing subsegments inside a large segment.
// Does not guarantee optimal space usage and instaead prioritizes performance.
struct SegmentPool {
  SegmentPool(unsigned size = 0) : size_(size), taken_() {
  }

  // Request segment of specified length, returns offset if a free segment found.
  // Constant-time, but can fail even if there is theoretically space available
  std::optional<unsigned /* offset */> Request(unsigned length);

  // Return segment by offset returned from Request().
  // Logarithmic by number of borrowed segments on average.
  void Return(unsigned offset);

  // Grow internal segment size
  void Grow(unsigned additional);

  unsigned Size() const {
    return size_;
  }

 private:
  unsigned size_;
  std::deque<std::pair<unsigned /* offset */, unsigned /* length */>> taken_;
};

}  // namespace base

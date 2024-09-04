// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/segment_pool.h"

#include <algorithm>

#include "base/logging.h"

namespace base {

using namespace std;

optional<unsigned> SegmentPool::Request(unsigned length) {
  // Invariant: elem.first + elem.second <= size_ for any elem in the queue.
  if (taken_.empty()) {
    if (length > size_)
      return nullopt;
    return taken_.emplace_back(0, length).first;
  }

  // If possible, squeeze segment before first occupied segment.
  if (size_t head_mark = taken_.front().first; length <= head_mark)
    return taken_.emplace_front(head_mark - length, length).first;

  // Otherwise, try appending after last occupied segment.
  size_t tail_mark = taken_.back().first + taken_.back().second;
  if (tail_mark + length <= size_)
    return taken_.emplace_back(tail_mark, length).first;

  return nullopt;
}

void SegmentPool::Return(unsigned offset) {
  auto it = taken_.end();

  if (taken_.front().first == offset)  // fast path
    it = taken_.begin();
  else
    it = lower_bound(taken_.begin(), taken_.end(), make_pair(offset, 0u));
  DCHECK(it != taken_.end());

  it->second = 0;  // clear length

  // Deleting from mid-point has linear complexity, so wait till
  // head or tail are emptied and we can collapse free segments.
  while (!taken_.empty() && taken_.front().second == 0)
    taken_.pop_front();

  while (!taken_.empty() && taken_.back().second == 0)
    taken_.pop_back();
}

void SegmentPool::Grow(unsigned additional) {
  size_ += additional;
}

}  // namespace base

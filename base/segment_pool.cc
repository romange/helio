#include "base/segment_pool.h"

#include <algorithm>

#include "base/logging.h"

namespace base {

std::optional<unsigned> SegmentPool::Request(unsigned length) {
  if (taken_.empty())
    return taken_.emplace_back(0, length).first;

  // If possible, squeeze segment before first occupied segment
  if (size_t head_mark = taken_.front().first; length <= head_mark)
    return taken_.emplace_front(head_mark - length, length).first;

  // Otherwise, try appending after last occupied segment
  size_t tail_mark = taken_.back().first + taken_.back().second;
  if (size_ - tail_mark >= length)
    return taken_.emplace_back(tail_mark, length).first;

  return std::nullopt;
}

void SegmentPool::Return(unsigned offset) {
  auto it = std::lower_bound(taken_.begin(), taken_.end(), std::make_pair(offset, 0u));
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

#include "io/aligned_buf_pool.h"

#include "base/logging.h"
#include "io/io.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/uring_proactor.h"

namespace io {

namespace {

thread_local AlignedBufPool tl_buf_pool;

}  // namespace

void AlignedBufPool::Init(size_t backing_size) {
  auto& pool = tl_buf_pool;
  CHECK_EQ(pool.buffer_, nullptr);

  pool.buffer_size_ = backing_size;
  pool.buffer_ = new (std::align_val_t(kAlign)) uint8_t[backing_size];

  CHECK_EQ(util::fb2::ProactorBase::me()->GetKind(), util::fb2::ProactorBase::IOURING);
  auto* uring = static_cast<util::fb2::UringProactor*>(util::fb2::ProactorBase::me());

  iovec vec{static_cast<void*>(pool.buffer_), backing_size};
  CHECK_EQ(uring->RegisterBuffers(&vec, 1), 0);
}

void AlignedBufPool::Destroy() {
  auto& pool = tl_buf_pool;
  if (!pool.buffer_)
    return;

  ::operator delete[](pool.buffer_, std::align_val_t(kAlign));
  pool.buffer_ = nullptr;

  auto* uring = static_cast<util::fb2::UringProactor*>(util::fb2::ProactorBase::me());
  uring->UnregisterBuffers();
}

AlignedBuf AlignedBufPool::RequestChunk(size_t length) {
  std::optional<size_t> offset = tl_buf_pool.RequestSegment((length + kAlign - 1) / kAlign);

  uint8_t* ptr = nullptr;
  if (offset)
    ptr = &tl_buf_pool.buffer_[*offset * kAlign];
  else
    ptr = new (std::align_val_t(kAlign)) uint8_t[length];

  return {io::MutableBytes(ptr, length),
          offset ? std::make_optional(tl_buf_pool.registered_id_) : std::nullopt};
}

void AlignedBufPool::ReturnChunk(void* ptr) {
  if (auto offset = tl_buf_pool.OffsetByPtr(ptr)) {
    tl_buf_pool.ReturnSegment(*offset);
  } else {
    ::operator delete[](ptr, std::align_val_t(kAlign));
  }
}

std::optional<size_t /* offset */> AlignedBufPool::RequestSegment(size_t length) {
  // If possible, squeeze right before first taken segment
  if (size_t head_mark = borrowed_.empty() ? 0 : borrowed_.front().first; length <= head_mark)
    return borrowed_.emplace_back(head_mark - length, length).first;

  // Otherwise, add segment after last occupied segment
  size_t tail_mark = borrowed_.empty() ? 0 : borrowed_.back().first + borrowed_.back().second;
  if (buffer_size_ - tail_mark >= length)
    return borrowed_.emplace_back(tail_mark, length).first;

  return std::nullopt;
}

void AlignedBufPool::ReturnSegment(size_t offset) {
  auto it =
      std::lower_bound(borrowed_.begin(), borrowed_.end(), std::pair<size_t, size_t>{offset, 0});
  DCHECK(it != borrowed_.end());

  it->second = 0;  // clear length

  while (!borrowed_.empty() && borrowed_.front().second == 0)
    borrowed_.pop_front();

  while (!borrowed_.empty() && borrowed_.back().second == 0)
    borrowed_.pop_back();
}

std::optional<size_t> AlignedBufPool::OffsetByPtr(const void* ptr) {
  DCHECK_EQ(size_t(ptr) % kAlign, 0u);

  if (ptr >= buffer_ && ptr <= buffer_ + buffer_size_ * kAlign) {
    return (size_t(ptr) - size_t(buffer_)) / kAlign;
  }
  return std::nullopt;
}

}  // namespace io
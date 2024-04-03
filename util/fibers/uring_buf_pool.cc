// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/uring_buf_pool.h"

#include <cstdint>
#include <new>
#include <optional>
#include <system_error>

#include "base/logging.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/uring_proactor.h"

namespace util::fb2 {

namespace {

thread_local UringBufPool tl_buf_pool;

}

std::error_code UringBufPool::Init(size_t length) {
  CHECK_EQ(length % kAlign, 0u);

  tl_buf_pool.backing_ = new (std::align_val_t(kAlign)) uint8_t[length];
  tl_buf_pool.segments_.Grow(length / kAlign);
  tl_buf_pool.buf_idx_ = 0;

  CHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  auto* uring = static_cast<UringProactor*>(ProactorBase::me());

  iovec vec{&tl_buf_pool.backing_, length};
  if (int res = uring->RegisterBuffers(&vec, 1); res < 0) {
    Destroy();
    return std::error_code{-res, std::system_category()};
  }

  return {};
}

void UringBufPool::Destroy() {
  CHECK(tl_buf_pool.backing_);

  ::operator delete[](tl_buf_pool.backing_, std::align_val_t(kAlign));
  CHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  auto* uring = static_cast<UringProactor*>(ProactorBase::me());
  uring->UnregisterBuffers();
}

UringBuf UringBufPool::RequestTLInternal(size_t length) {
  CHECK(tl_buf_pool.backing_);
  return tl_buf_pool.Request(length);
}

void UringBufPool::ReturnTL(uint8_t* ptr) {
  CHECK(tl_buf_pool.backing_);
  tl_buf_pool.Return(ptr);
}

UringBuf UringBufPool::Request(size_t length) {
  length = (length + kAlign - 1) / kAlign * kAlign;

  if (auto offset = segments_.Request(length / kAlign))
    return {io::MutableBytes{backing_ + *offset * kAlign, length}, buf_idx_};

  return {{new (std::align_val_t(kAlign)) uint8_t[length], length}, std::nullopt};
}

void UringBufPool::Return(uint8_t* ptr) {
  if (ptr >= backing_ && ptr <= backing_ + segments_.Size() * kAlign) {
    segments_.Return((ptr - backing_) / kAlign);
  } else {
    ::operator delete[](ptr, std::align_val_t(kAlign));
  }
}

}  // namespace util::fb2

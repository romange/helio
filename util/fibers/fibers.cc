// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/fibers.h"

#include "base/logging.h"

using namespace std;

namespace util {
namespace fb2 {

Fiber::~Fiber() {
  CHECK(!IsJoinable());
}

Fiber& Fiber::operator=(Fiber&& other) noexcept {
  CHECK(!IsJoinable());

  if (this == &other) {
    return *this;
  }

  impl_.swap(other.impl_);
  return *this;
}

void Fiber::Detach() {
  impl_.reset();
}

void Fiber::Join() {
  CHECK(IsJoinable());
  impl_->Join();
  DVLOG(1) << "Fiber::Joined() " << impl_->name() << " " << impl_->DEBUG_use_count();
  impl_.reset();
}

void Fiber::JoinIfNeeded() {
  if (IsJoinable())
    Join();
}

void SetDefaultStackResource(PMR_NS::memory_resource* mr, size_t default_size) {
  CHECK(detail::default_stack_resource == nullptr);
  detail::default_stack_resource = mr;
  detail::default_stack_size = default_size;
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

void SetDefaultStackSize(size_t default_size) {
  detail::default_stack_size = default_size;
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

void* StdMallocResource::do_allocate(size_t size, size_t align) {
  void* res = malloc(size);
  if (res == nullptr) {
    throw bad_alloc();
  }
  return res;
}

void StdMallocResource::do_deallocate(void* ptr, size_t size, size_t align) {
  free(ptr);
}

StdMallocResource std_malloc_resource;

}  // namespace fb2
}  // namespace util

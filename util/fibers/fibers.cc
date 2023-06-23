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
  DVLOG(1) << "Fiber::Joined() " << impl_->name() << " "
           << impl_->DEBUG_use_count();
  impl_.reset();
}

void Fiber::JoinIfNeeded() {
  if (IsJoinable())
    Join();
}

}  // namespace fb2
}  // namespace util
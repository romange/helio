// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/fiber2.h"

#include "base/logging.h"

using namespace std;

namespace util {
namespace fb2 {

Fiber::~Fiber() {
  CHECK(!joinable());
}

Fiber& Fiber::operator=(Fiber&& other) noexcept {
  CHECK(!joinable());

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
  CHECK(joinable());
  impl_->Join();
  impl_.reset();
}

}  // namespace fb2
}  // namespace util
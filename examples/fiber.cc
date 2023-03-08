// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "examples/fiber.h"

#include "base/logging.h"

using namespace std;

namespace example {

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


}  // namespace example
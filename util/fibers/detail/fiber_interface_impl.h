// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "util/fibers/detail/scheduler.h"

namespace util {
namespace fb2 {
namespace detail {

inline void FiberInterface::Yield() {
  scheduler_->AddReady(this);
  scheduler_->Preempt();
}

inline void FiberInterface::WaitUntil(std::chrono::steady_clock::time_point tp) {
  scheduler_->WaitUntil(tp, this);
}

inline void FiberInterface::Suspend() {
  scheduler_->Preempt();
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

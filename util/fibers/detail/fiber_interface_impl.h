// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "util/fibers/detail/scheduler.h"

namespace util {
namespace fb2 {
namespace detail {

inline bool FiberInterface::WaitUntil(std::chrono::steady_clock::time_point tp) {
  return scheduler_->WaitUntil(tp, this);
}

inline void FiberInterface::Suspend() {
  auto res = scheduler_->Preempt();
  assert(!res);
  assert(!IsScheduledRemotely());
}

inline void FiberInterface::Yield() {
  scheduler_->Yield(this);
}

}  // namespace detail
}  // namespace fb2
}  // namespace util

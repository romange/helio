// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>

namespace base {

class SpinLock {
 public:
  SpinLock() = default;
  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;

  void lock() {
    lock_.Lock();
  }

  void unlock() {
    lock_.Unlock();
  }

  bool try_lock() {
    return lock_.TryLock();
  }

 private:
  absl::base_internal::SpinLock lock_;
};

}  // namespace base

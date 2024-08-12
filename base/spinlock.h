// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/spinlock.h>

namespace base {

class ABSL_LOCKABLE SpinLock {
 public:
  SpinLock() = default;
  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    lock_.Lock();
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    lock_.Unlock();
  }

  bool try_lock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    return lock_.TryLock();
  }

 private:
  absl::base_internal::SpinLock lock_;
};

}  // namespace base

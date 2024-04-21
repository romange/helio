// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <memory>
#include <optional>

#include "util/fibers/synchronization.h"

namespace util {
namespace fb2 {

// Simple thread-safe fiber-blocking future for waiting for value. Pass by value.
template <typename T> struct Future {
  Future() : block{std::make_shared<Block>()} {
  }

  T Get() {
    block->waker.await(
        [this] { return block->has_value.exchange(false, std::memory_order_relaxed); });
    return std::move(block->value);
  }

  void Resolve(T result) {
    block->value = std::move(result);
    block->has_value.store(true, std::memory_order_relaxed);
    block->waker.notify();
  }

 private:
  struct Block {
    T value{};  // replace with aligned_storage or optional if T is not default-constructible
    std::atomic_bool has_value = false;
    util::fb2::EventCount waker{};
  };
  std::shared_ptr<Block> block;
};

}  // namespace fb2
}  // namespace util

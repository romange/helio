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
        [this] { return block->has_value.exchange(false, std::memory_order_acquire); });
    return std::move(block->value);
  }

  std::optional<T> GetFor(std::chrono::steady_clock::duration dur) {
    std::cv_status st =block->waker.await_until(
        [this] { return block->has_value.exchange(false, std::memory_order_acquire); },
        std::chrono::steady_clock::now() + dur);
    if (st == std::cv_status::timeout) {
      return std::nullopt;
    }
    return std::move(block->value);
  }

  void Resolve(T result) {
    block->value = std::move(result);
    block->has_value.store(true, std::memory_order_release);
    block->waker.notify();
  }

  bool IsResolved() const {
    return block->has_value.load(std::memory_order_acquire);
  }

 private:
  struct Block {
    T value{};  // replace with aligned_storage or optional if T is not default-constructible
    std::atomic_bool has_value = false;
    util::fb2::EventCount waker;
  };
  std::shared_ptr<Block> block;
};

}  // namespace fb2
}  // namespace util

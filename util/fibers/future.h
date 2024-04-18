// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

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
    block->waker.await([this] { return block->value.has_value(); });
    return std::move(*block->value);
  }

  void Resolve(T result) {
    block->value = std::move(result);
    block->waker.notify();
  }

 private:
  struct Block {
    std::optional<T> value;
    util::fb2::EventCount waker;
  };
  std::shared_ptr<Block> block;
};

}  // namespace fb2
}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/proactor_pool.h"

namespace util {
namespace fb2 {

class Pool : public ProactorPool {
 public:
  static Pool* Epoll(size_t pool_size = 0);
  static Pool* IOUring(size_t ring_depth, size_t pool_size = 0);

 private:
  Pool(ProactorBase::Kind kind, size_t pool_size) : ProactorPool(pool_size), kind_(kind) {
  }

  ProactorBase::Kind kind_;
  unsigned ring_depth_ = 0;

  ProactorBase* CreateProactor() final;
  void InitInThread(unsigned index) final;
};
}  // namespace fb2
}  // namespace util

// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/uring_pool.h"

#include "base/logging.h"
#include "util/uring/proactor.h"

using namespace std;

namespace util {
namespace uring {

UringPool::~UringPool() {
}

ProactorBase* UringPool::CreateProactor() {
  return new Proactor;
}

void UringPool::InitInThread(unsigned index) {
  Proactor* p = static_cast<Proactor*>(proactor_[index]);
  p->Init(ring_depth_);
}

}  // namespace uring
}  // namespace util

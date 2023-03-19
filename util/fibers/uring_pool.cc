// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/uring_pool.h"

#include "base/logging.h"
#include "util/fibers/uring_proactor.h"

using namespace std;

namespace util {
namespace fb2 {

UringPool::~UringPool() {
}

ProactorBase* UringPool::CreateProactor() {
  return new UringProactor;
}

void UringPool::InitInThread(unsigned index) {
  UringProactor* p = static_cast<UringProactor*>(proactor_[index]);
  p->Init(ring_depth_);
}

}  // namespace fb2
}  // namespace util

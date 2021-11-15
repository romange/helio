// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/epoll/ev_pool.h"

#include "base/logging.h"
#include "base/pthread_utils.h"
#include "util/epoll/ev_controller.h"

using namespace std;

namespace util {
namespace epoll {

EvPool::EvPool(std::size_t pool_size) : ProactorPool(pool_size) {
}

EvPool::~EvPool() {
}

ProactorBase* EvPool::CreateProactor() {
  return new EvController;
}

void EvPool::InitInThread(unsigned index) {
}


}  // namespace epoll
}  // namespace util

// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/epoll/epoll_pool.h"

#include "base/logging.h"
#include "base/pthread_utils.h"
#include "util/epoll/proactor.h"

using namespace std;

namespace util {
namespace epoll {

EpollPool::EpollPool(std::size_t pool_size) : ProactorPool(pool_size) {
}

EpollPool::~EpollPool() {
}

ProactorBase* EpollPool::CreateProactor() {
  return new EpollProactor;
}

void EpollPool::InitInThread(unsigned index) {
  EpollProactor* p = static_cast<EpollProactor*>(proactor_[index]);
  p->Init();
}


}  // namespace epoll
}  // namespace util

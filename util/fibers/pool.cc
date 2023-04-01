// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/pool.h"

#include "util/fibers/epoll_proactor.h"
#include "util/fibers/uring_proactor.h"

namespace util {
namespace fb2 {

Pool* Pool::Epoll(size_t pool_size) {
  Pool* res = new Pool(ProactorBase::Kind::EPOLL, pool_size);
  return res;
}

Pool* Pool::IOUring(size_t ring_depth, size_t pool_size) {
  Pool* res = new Pool(ProactorBase::Kind::IOURING, pool_size);
  res->ring_depth_ = ring_depth;
  return res;
}

ProactorBase* Pool::CreateProactor() {
  switch (kind_) {
    case ProactorBase::Kind::EPOLL:
      return new EpollProactor;
    case ProactorBase::Kind::IOURING:
      return new UringProactor;
  }
  return nullptr;
}

void Pool::InitInThread(unsigned index) {
  switch (kind_) {
    case ProactorBase::Kind::EPOLL: {
      EpollProactor* p = static_cast<EpollProactor*>(proactor_[index]);
      p->Init();
      break;
    }

    case ProactorBase::Kind::IOURING: {
      UringProactor* p = static_cast<UringProactor*>(proactor_[index]);
      p->Init(ring_depth_);
      break;
    }
  }
}

}  // namespace fb2

}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/pool.h"

#include "base/logging.h"
#include "util/fibers/epoll_proactor.h"

#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#endif

namespace util {
namespace fb2 {

Pool* Pool::Epoll(size_t pool_size) {
  Pool* res = new Pool(ProactorBase::Kind::EPOLL, pool_size);
  return res;
}

#ifdef __linux__
Pool* Pool::IOUring(size_t ring_depth, size_t pool_size) {
  Pool* res = new Pool(ProactorBase::Kind::IOURING, pool_size);
  res->ring_depth_ = ring_depth;
  return res;
}
#endif

ProactorBase* Pool::CreateProactor() {
  switch (kind_) {
    case ProactorBase::Kind::EPOLL:
      return new EpollProactor;
    case ProactorBase::Kind::IOURING:
#ifdef __linux__
      return new UringProactor;
#else
      LOG(FATAL) << "IOUring is not supported on this platform";
#endif
  }
  return nullptr;
}

void Pool::InitInThread(unsigned pool_index) {
  switch (kind_) {
    case ProactorBase::Kind::EPOLL: {
      EpollProactor* p = static_cast<EpollProactor*>(proactor_[pool_index]);
      p->Init(pool_index);
      break;
    }

    case ProactorBase::Kind::IOURING: {
#ifdef __linux__
      UringProactor* p = static_cast<UringProactor*>(proactor_[pool_index]);
      p->Init(pool_index, ring_depth_);
#else
      CHECK(false);
#endif
      break;
    }
  }
}

}  // namespace fb2

}  // namespace util

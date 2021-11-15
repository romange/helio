// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fiber_sched_algo.h"

namespace util {
namespace epoll {

class EpollFiberAlgo : public FiberSchedAlgo {

 public:
   explicit EpollFiberAlgo(ProactorBase* proactor);
  ~EpollFiberAlgo();

 private:
  void SuspendWithTimer(const time_point& tp) noexcept final;

  unsigned arm_index_;
};

}  // namespace uring
}  // namespace util

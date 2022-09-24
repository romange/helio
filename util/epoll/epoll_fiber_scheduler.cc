// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/epoll/epoll_fiber_scheduler.h"

#include <sys/epoll.h>
#include <sys/timerfd.h>

#include "base/logging.h"
#include "util/epoll/proactor.h"

namespace util {
namespace epoll {
using namespace boost;
using namespace std;

EpollFiberAlgo::EpollFiberAlgo(ProactorBase* ev_cntr) : FiberSchedAlgo(ev_cntr) {
  auto cb = [tfd = timer_fd_](uint32_t event_mask, EpollProactor*) {
    uint64_t val;
    int res = read(tfd, &val, sizeof(val));
    DVLOG(2) << "this_fiber::yield " << event_mask << "/" << res;

    this_fiber::yield();
  };

  arm_index_ = static_cast<EpollProactor*>(ev_cntr)->Arm(timer_fd_, std::move(cb), EPOLLIN);
}

EpollFiberAlgo::~EpollFiberAlgo() {
  static_cast<EpollProactor*>(proactor_)->Disarm(timer_fd_, arm_index_);
}

void EpollFiberAlgo::SuspendWithTimer(const time_point& abs_time) noexcept {
  using namespace chrono;
  constexpr uint64_t kNsFreq = 1000000000ULL;
  const chrono::time_point<steady_clock, nanoseconds>& tp = abs_time;
  int64_t ns = time_point_cast<nanoseconds>(tp).time_since_epoch().count();

  struct itimerspec abs_spec;

  abs_spec.it_value.tv_sec = ns / kNsFreq;
  abs_spec.it_value.tv_nsec = ns - abs_spec.it_value.tv_sec * kNsFreq;

  memset(&abs_spec.it_interval, 0, sizeof(abs_spec.it_interval));

  int res = timerfd_settime(timer_fd_, TFD_TIMER_ABSTIME, &abs_spec, NULL);
  CHECK_EQ(0, res) << strerror(errno);
}

}  // namespace epoll
}  // namespace util

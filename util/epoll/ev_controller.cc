// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/epoll/ev_controller.h"

#include <signal.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

#include "absl/time/clock.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "util/epoll/epoll_fiber_scheduler.h"
#include "util/epoll/fiber_socket.h"

#define EV_CHECK(x)                                                              \
  do {                                                                           \
    int __res_val = (x);                                                         \
    if (ABSL_PREDICT_FALSE(__res_val < 0)) {                                     \
      LOG(FATAL) << "Error " << (-__res_val) << " evaluating '" #x "': "         \
                 << detail::SafeErrorMessage(-__res_val);                        \
    }                                                                            \
  } while (false)

using namespace boost;
namespace ctx = boost::context;

namespace util {
namespace epoll {

namespace {

constexpr uint64_t kIgnoreIndex = 0;
constexpr uint64_t kNopIndex = 2;
constexpr uint64_t kUserDataCbIndex = 1024;
constexpr uint32_t kSpinLimit = 30;

}  // namespace

EvController::EvController() : ProactorBase() {
  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  CHECK_GE(epoll_fd_, 0);
  VLOG(1) << "Created epoll_fd_ " << epoll_fd_;
}

EvController::~EvController() {
  CHECK(is_stopped_);
  close(epoll_fd_);

  DVLOG(1) << "~EvController";
}

void EvController::Run() {
  VLOG(1) << "EvController::Run";
  Init();

  main_loop_ctx_ = fibers::context::active();
  fibers::scheduler* sched = main_loop_ctx_->get_scheduler();

  EpollFiberAlgo* algo = new EpollFiberAlgo(this);
  sched->set_algo(algo);
  this_fiber::properties<FiberProps>().set_name("ioloop");

  is_stopped_ = false;

  constexpr size_t kBatchSize = 64;
  struct epoll_event cevents[kBatchSize];

  uint32_t tq_seq = 0;
  uint32_t num_stalls = 0;
  uint32_t spin_loops = 0, num_task_runs = 0;
  Tasklet task;

  while (true) {
    num_task_runs = 0;

    uint64_t task_start = 0;

    tq_seq = tq_seq_.load(std::memory_order_acquire);

    // This should handle wait-free and "submit-free" short CPU tasks enqued using Async/Await
    // calls. We allocate the quota of 500K nsec (500usec) of CPU time per iteration.
    while (task_queue_.try_dequeue(task)) {
      ++num_task_runs;
      tl_info_.monotonic_time = GetClockNanos();
      task();
      if (task_start == 0) {
        task_start = tl_info_.monotonic_time;
      } else if (task_start + 500000 < tl_info_.monotonic_time) {
        break;
      }
    }

    if (num_task_runs) {
      task_queue_avail_.notifyAll();
    }

    int timeout = 0;  // By default we do not block on epoll_wait.

    if (spin_loops >= kSpinLimit) {
      spin_loops = 0;

      if (tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_acquire)) {
        // We check stop condition when all the pending events were processed.
        // It's up to the app-user to make sure that the incoming flow of events is stopped before
        // stopping EvController.
        if (is_stopped_)
          break;
        ++num_stalls;
        timeout = -1;  // We gonna block on epoll_wait.
      }
    }

    DVLOG(2) << "EpollWait " << timeout;
    int epoll_res = epoll_wait(epoll_fd_, cevents, kBatchSize, timeout);
    if (epoll_res < 0) {
      epoll_res = errno;
      if (epoll_res == EINTR)
        continue;
      LOG(FATAL) << "TBD: " << errno << " " << strerror(errno);
    }

    uint32_t cqe_count = epoll_res;
    if (cqe_count) {
      DispatchCompletions(cevents, cqe_count);
    }

    if (timeout == -1) {
      // Reset all except the LSB bit that signals that we need to switch to dispatch fiber.
      tq_seq_.fetch_and(1, std::memory_order_release);
    }

    if (tq_seq & 1) {  // We allow dispatch fiber to run.
      tq_seq_.fetch_and(~1U, std::memory_order_relaxed);
      this_fiber::yield();
    }

    if (sched->has_ready_fibers()) {
      // Suspend this fiber until others finish runnning and get blocked.
      // Eventually UringFiberAlgo will resume back this fiber in suspend_until
      // function.
      DVLOG(2) << "Suspend ioloop";
      auto now = GetClockNanos();
      tl_info_.monotonic_time = now;
      algo->SuspendIoLoop(now);

      DVLOG(2) << "Resume ioloop";
      spin_loops = 0;
    }
    ++spin_loops;
  }

  VLOG(1) << "wakeups/stalls: " << tq_wakeup_ev_.load() << "/" << num_stalls;

  VLOG(1) << "centries size: " << centries_.size();
}

unsigned EvController::Arm(int fd, CbType cb, uint32_t event_mask) {
  epoll_event ev;
  ev.events = event_mask;
  if (next_free_ce_ < 0) {
    RegrowCentries();
    CHECK_GT(next_free_ce_, 0);
  }

  ev.data.u32 = next_free_ce_ + kUserDataCbIndex;
  DCHECK_LT(unsigned(next_free_ce_), centries_.size());

  auto& e = centries_[next_free_ce_];
  DCHECK(!e.cb);  // cb is undefined.
  DVLOG(1) << "Arm: " << fd << ", index: " << next_free_ce_;

  unsigned ret = next_free_ce_;
  next_free_ce_ = e.index;
  e.cb = std::move(cb);
  e.index = -1;

  CHECK_EQ(0, epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev));
  return ret;
}

void EvController::UpdateCb(unsigned arm_index, CbType cb) {
  CHECK_LT(arm_index, centries_.size());
  centries_[arm_index].cb = cb;
}

void EvController::Disarm(int fd, unsigned arm_index) {
  DVLOG(1) << "Disarming " << fd << " on " << arm_index;
  CHECK_LT(arm_index, centries_.size());

  centries_[arm_index].cb = nullptr;
  centries_[arm_index].index = next_free_ce_;

  next_free_ce_ = arm_index;
  CHECK_EQ(0, epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, NULL));
}

void EvController::Init() {
  CHECK_EQ(0U, thread_id_) << "Init was already called";

  centries_.resize(512);  // .index = -1
  next_free_ce_ = 0;
  for (size_t i = 0; i < centries_.size() - 1; ++i) {
    centries_[i].index = i + 1;
  }

  thread_id_ = pthread_self();
  tl_info_.owner = this;

  auto cb = [ev_fd = wake_fd_](uint32_t mask, auto*) {
    DVLOG(1) << "EventFdCb called " << mask;
    uint64_t val;
    CHECK_EQ(8, read(ev_fd, &val, sizeof(val)));
  };
  Arm(wake_fd_, std::move(cb), EPOLLIN);
}

LinuxSocketBase* EvController::CreateSocket(int fd) {
  FiberSocket* res = new FiberSocket(fd);
  res->SetProactor(this);

  return res;
}

void EvController::SchedulePeriodic(uint32_t id, PeriodicItem* item) {
  int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  CHECK_GE(tfd, 0);
  itimerspec ts;
  ts.it_value = item->period;
  ts.it_interval = item->period;
  item->val1 = tfd;

  auto cb = [item](uint32_t event_mask, EvController*) {
    if (!item->in_map) {
      delete item;
      return;
    }

    item->task();
    uint64_t res;
    if (read(item->val1, &res, sizeof(res)) == -1) {
      LOG(ERROR) << "Error reading from timer, errno " << errno;
    }
  };

  unsigned arm_id = Arm(tfd, std::move(cb), EPOLLIN);
  item->val2 = arm_id;

  CHECK_EQ(0, timerfd_settime(tfd, 0, &ts, NULL));
}

void EvController::CancelPeriodicInternal(uint32_t val1, uint32_t val2) {
  auto arm_index = val2;

  // we call the callback one more time explicitly in order to make sure it
  // deleted PeriodicItem.
  if (centries_[arm_index].cb) {
    centries_[arm_index].cb(0, this);
    centries_[arm_index].cb = nullptr;
  }

  Disarm(val1, val2);
  if (close(val1) == -1) {
    LOG(ERROR) << "Could not close timer, error " << errno;
  }
}

void EvController::DispatchCompletions(epoll_event* cevents, unsigned count) {
  DVLOG(2) << "DispatchCompletions " << count << " cqes";
  for (unsigned i = 0; i < count; ++i) {
    const auto& cqe = cevents[i];

    // I allocate range of 1024 reserved values for the internal EvController use.
    uint32_t user_data = cqe.data.u32;

    if (cqe.data.u32 >= kUserDataCbIndex) {  // our heap range surely starts higher than 1k.
      size_t index = user_data - kUserDataCbIndex;
      DCHECK_LT(index, centries_.size());
      const auto& item = centries_[index];

      if (item.cb) {  // We could disarm an event and get this completion afterwards.
        item.cb(cqe.events, this);
      }
      continue;
    }

    if (user_data == kIgnoreIndex || kNopIndex)
      continue;

    LOG(ERROR) << "Unrecognized user_data " << user_data;
  }
}

void EvController::RegrowCentries() {
  size_t prev = centries_.size();
  VLOG(1) << "RegrowCentries from " << prev << " to " << prev * 2;

  centries_.resize(prev * 2);  // grow by 2.
  next_free_ce_ = prev;
  for (; prev < centries_.size() - 1; ++prev)
    centries_[prev].index = prev + 1;
}

}  // namespace epoll
}  // namespace util

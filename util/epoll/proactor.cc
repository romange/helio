// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/epoll/proactor.h"

#include <signal.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

#include "absl/time/clock.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "util/epoll/epoll_fiber_scheduler.h"
#include "util/epoll/epoll_socket.h"

#define EV_CHECK(x)                                                                   \
  do {                                                                                \
    int __res_val = (x);                                                              \
    if (ABSL_PREDICT_FALSE(__res_val < 0)) {                                          \
      LOG(FATAL) << "Error " << (-__res_val)                                          \
                 << " evaluating '" #x "': " << detail::SafeErrorMessage(-__res_val); \
    }                                                                                 \
  } while (false)

#define VPRO(verbosity) VLOG(verbosity) << "PRO[" << tl_info_.proactor_index << "] "

namespace ctx = boost::context;
namespace fibers = boost::fibers;
namespace this_fiber = boost::this_fiber;
using namespace std;

namespace util {
namespace epoll {

namespace {

constexpr uint64_t kIgnoreIndex = 0;
constexpr uint64_t kNopIndex = 2;
constexpr uint64_t kUserDataCbIndex = 1024;

}  // namespace

EpollProactor::EpollProactor() : ProactorBase() {
  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  CHECK_GE(epoll_fd_, 0);
  VLOG(1) << "Created epoll_fd_ " << epoll_fd_;
}

EpollProactor::~EpollProactor() {
  CHECK(is_stopped_);
  close(epoll_fd_);

  DVLOG(1) << "~EpollProactor";
}

void EpollProactor::Init() {
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

void EpollProactor::Run() {
  VLOG(1) << "EpollProactor::Run";

  main_loop_ctx_ = fibers::context::active();
  fibers::scheduler* sched = main_loop_ctx_->get_scheduler();

  EpollFiberAlgo* algo = new EpollFiberAlgo(this);
  sched->set_algo(algo);
  this_fiber::properties<FiberProps>().set_name("ioloop");

  is_stopped_ = false;

  constexpr size_t kBatchSize = 128;
  struct epoll_event cevents[kBatchSize];

  uint32_t tq_seq = 0;
  uint64_t num_stalls = 0, cqe_fetches = 0, loop_cnt = 0, num_suspends = 0;
  uint32_t spin_loops = 0, num_task_runs = 0, task_interrupts = 0;
  uint32_t cqe_count = 0;
  bool should_suspend = false;
  Tasklet task;

  while (true) {
    ++loop_cnt;
    num_task_runs = 0;

    tq_seq = tq_seq_.load(memory_order_acquire);

    if (task_queue_.try_dequeue(task)) {
      uint32_t cnt = 0;
      uint64_t task_start = GetClockNanos();

      // update thread-local clock service via GetMonotonicTimeNs().
      tl_info_.monotonic_time = task_start;
      do {
        task();
        ++num_task_runs;
        ++cnt;
        tl_info_.monotonic_time = GetClockNanos();
        if (task_start + 500000 < tl_info_.monotonic_time) {  // Break after 500usec
          ++task_interrupts;
          break;
        }

        if (cnt == 32) {
          // we notify threads if we unloaded a bunch of tasks.
          // if in parallel they start pushing we may unload them in parallel
          // via this loop thus increasing its efficiency.
          task_queue_avail_.notifyAll();
        }
      } while (task_queue_.try_dequeue(task));

      num_task_runs += cnt;
      DVLOG(2) << "Tasks runs " << num_task_runs << "/" << spin_loops;

      // We notify second time to avoid deadlocks.
      // Without it ProactorTest.AsyncCall blocks.
      task_queue_avail_.notifyAll();
    }

    // we got notifications from 3rd party threads via tq_seq_ lsb.
    if (tq_seq & 1) {  // We allow dispatch fiber to run.
      // we use ack_req so that this store won't be reordered with `this_fiber::yield` below.
      // That makes sure that subsequent notifications via tq_seq_ won't get lost.
      // In other words, if another thread sets the bit right after fetch_and -
      // we will call again yield() at some point, but if `fetch_and` would move
      // after the yield and someone would notify us after the dispatcher runs,
      // we would loose that notification.
      tq_seq_.fetch_and(~1U, memory_order_acq_rel);
      tq_seq &= ~1;

      // This eventually calls dispatcher fiber.
      this_fiber::yield();

      // we preempted without passing through suspend_until so we should not block on epoll_wait
      // before we call suspend_until.
      should_suspend = true;
    }

    int timeout = 0;  // By default we do not block on epoll_wait.

    // Check if we can block on I/O.
    // There are few ground rules before we can set timeout=-1 (i.e. block indefinitely)
    // 1. No other fibers are active.
    // 2. Specifically SuspendIoLoop was called and returned true.
    // 3. Moreover dispatch fiber was switched to at least once since RequestDispatcher
    //    has been called (i.e. tq_seq_ has a flag on that says we should
    //    switch to dispatcher fiber). This is verified with (tq_seq & 1) check.
    //    These rules a bit awkward because we hack into 3rd party fibers framework
    //    without the ability to build a straightforward epoll/fibers scheduler.

    bool has_fiber_work = should_suspend || sched->has_ready_fibers();

    if (!has_fiber_work && spin_loops >= kMaxSpinLimit) {
      spin_loops = 0;

      if (tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, memory_order_acquire)) {
        // We check stop condition when all the pending events were processed.
        // It's up to the app-user to make sure that the incoming flow of events is stopped before
        // stopping EpollProactor.
        if (is_stopped_)
          break;
        ++num_stalls;
        timeout = -1;  // We gonna block on epoll_wait.
      }
    }

    DCHECK(!has_fiber_work || timeout == 0);

    DVLOG(2) << "EpollWait " << timeout << " " << tq_seq;

    int epoll_res = epoll_wait(epoll_fd_, cevents, kBatchSize, timeout);
    if (epoll_res < 0) {
      epoll_res = errno;
      if (epoll_res == EINTR)
        continue;
      LOG(FATAL) << "TBD: " << errno << " " << strerror(errno);
    }

    if (timeout == -1) {
      // Zero all bits except the lsb which signals we need to switch to dispatch fiber.
      tq_seq_.fetch_and(1, memory_order_release);
    }

    cqe_count = epoll_res;
    if (cqe_count) {
      ++cqe_fetches;
      tl_info_.monotonic_time = GetClockNanos();

      while (true) {
        VPRO(2) << "Fetched " << cqe_count << " cqes";
        DispatchCompletions(cevents, cqe_count);

        if (cqe_count < kBatchSize) {
          break;
        }
        epoll_res = epoll_wait(epoll_fd_, cevents, kBatchSize, 0);
        if (epoll_res < 0) {
          break;
        }
        cqe_count = epoll_res;
      };
    }

    if (has_fiber_work) {
      // Suspend this fiber until others finish runnning and get blocked.
      // Eventually UringFiberAlgo will resume back this fiber in suspend_until
      // function.
      DVLOG(2) << "Suspend ioloop";
      auto now = GetClockNanos();
      tl_info_.monotonic_time = now;
      if (algo->SuspendIoLoop(now)) {
        should_suspend = false;
        ++num_suspends;
      }

      DVLOG(2) << "Resume ioloop " << tq_seq_.load(memory_order_relaxed);
      spin_loops = 0;
      continue;
    }

    // TODO: to handle idle tasks.
    Pause(spin_loops);
    ++spin_loops;
  }

  VPRO(1) << "total/stalls/cqe_fetches/num_suspends: " << loop_cnt << "/" << num_stalls << "/"
          << cqe_fetches << "/" << num_suspends;

  VLOG(1) << "wakeups/stalls: " << tq_wakeup_ev_.load() << "/" << num_stalls;

  VLOG(1) << "centries size: " << centries_.size();
}

unsigned EpollProactor::Arm(int fd, CbType cb, uint32_t event_mask) {
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

void EpollProactor::UpdateCb(unsigned arm_index, CbType cb) {
  CHECK_LT(arm_index, centries_.size());
  centries_[arm_index].cb = cb;
}

void EpollProactor::Disarm(int fd, unsigned arm_index) {
  DVLOG(1) << "Disarming " << fd << " on " << arm_index;
  CHECK_LT(arm_index, centries_.size());

  centries_[arm_index].cb = nullptr;
  centries_[arm_index].index = next_free_ce_;

  next_free_ce_ = arm_index;
  CHECK_EQ(0, epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, NULL));
}

LinuxSocketBase* EpollProactor::CreateSocket(int fd) {
  EpollSocket* res = new EpollSocket(fd);
  res->SetProactor(this);

  return res;
}

void EpollProactor::SchedulePeriodic(uint32_t id, PeriodicItem* item) {
  int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  CHECK_GE(tfd, 0);
  itimerspec ts;
  ts.it_value = item->period;
  ts.it_interval = item->period;
  item->val1 = tfd;

  auto cb = [this, item](uint32_t event_mask, EpollProactor*) {
    this->PeriodicCb(item);
  };

  unsigned arm_id = Arm(tfd, std::move(cb), EPOLLIN);
  item->val2 = arm_id;

  CHECK_EQ(0, timerfd_settime(tfd, 0, &ts, NULL));
}

void EpollProactor::CancelPeriodicInternal(uint32_t tfd, uint32_t arm_id) {
  // we call the callback one more time explicitly in order to make sure it
  // deleted PeriodicItem.
  if (centries_[arm_id].cb) {
    centries_[arm_id].cb(0, this);
    centries_[arm_id].cb = nullptr;
  }

  Disarm(tfd, arm_id);
  if (close(tfd) == -1) {
    LOG(ERROR) << "Could not close timer, error " << errno;
  }
}

void EpollProactor::PeriodicCb(PeriodicItem* item) {
  if (!item->in_map) {
    delete item;
    return;
  }

  item->task();
  uint64_t res;
  if (read(item->val1, &res, sizeof(res)) == -1) {
    LOG(ERROR) << "Error reading from timer, errno " << errno;
  }
}

void EpollProactor::DispatchCompletions(epoll_event* cevents, unsigned count) {
  DVLOG(2) << "DispatchCompletions " << count << " cqes";
  for (unsigned i = 0; i < count; ++i) {
    const auto& cqe = cevents[i];

    // I allocate range of 1024 reserved values for the internal EpollProactor use.
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

void EpollProactor::RegrowCentries() {
  size_t prev = centries_.size();
  VLOG(1) << "RegrowCentries from " << prev << " to " << prev * 2;

  centries_.resize(prev * 2);  // grow by 2.
  next_free_ce_ = prev;
  for (; prev < centries_.size() - 1; ++prev)
    centries_[prev].index = prev + 1;
}

}  // namespace epoll
}  // namespace util

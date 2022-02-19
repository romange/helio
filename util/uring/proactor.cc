// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include <liburing.h>
#include <string.h>
#include <sys/eventfd.h>
#include <poll.h>

#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>

#include "absl/base/attributes.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/uring_socket.h"

DEFINE_bool(proactor_register_fd, false, "If true tries to register file descriptors");

#define URING_CHECK(x)                                                           \
  do {                                                                           \
    int __res_val = (x);                                                         \
    if (ABSL_PREDICT_FALSE(__res_val < 0)) {                                     \
      char buf[128];                                                             \
      strerror_r(-__res_val, buf, sizeof(buf));                      \
      LOG(FATAL) << "Error " << (-__res_val) << " evaluating '" #x "': " << buf; \
    }                                                                            \
  } while (false)

#ifndef __NR_io_uring_enter
#define __NR_io_uring_enter 426
#endif

#define VPRO(verbosity) VLOG(verbosity) << "PRO[" << tl_info_.proactor_index << "] "

using namespace boost;
namespace ctx = boost::context;

namespace util {
namespace uring {

namespace {

inline int sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete, unsigned flags,
                              sigset_t* sig) {
  return syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, sig, _NSIG / 8);
}

ABSL_ATTRIBUTE_NOINLINE void wait_for_cqe(io_uring* ring, unsigned wait_nr, sigset_t* sig = NULL) {
  // res must be 0 or -1.
  int res = sys_io_uring_enter(ring->ring_fd, 0, wait_nr, IORING_ENTER_GETEVENTS, sig);
  if (res == 0 || errno == EINTR)
    return;
  DCHECK_EQ(-1, res);
  res = errno;

  LOG(FATAL) << "Error " << (res) << " evaluating sys_io_uring_enter: " << strerror(res);
}

inline unsigned CQReadyCount(const io_uring& ring) {
  return io_uring_smp_load_acquire(ring.cq.ktail) - *ring.cq.khead;
}

unsigned IoRingPeek(const io_uring& ring, io_uring_cqe* cqes, unsigned count) {
  unsigned ready = CQReadyCount(ring);
  if (!ready)
    return 0;

  count = count > ready ? ready : count;
  unsigned head = *ring.cq.khead;
  unsigned mask = *ring.cq.kring_mask;
  unsigned last = head + count;
  for (int i = 0; head != last; head++, i++) {
    cqes[i] = ring.cq.cqes[head & mask];
  }
  return count;
}

constexpr uint64_t kIgnoreIndex = 0;
constexpr uint64_t kWakeIndex = 1;

constexpr uint64_t kUserDataCbIndex = 1024;

// Important to spin a bit, otherwise we put too much pressure on  eventfd_write.
constexpr uint32_t kSpinLimit = 10;

}  // namespace

Proactor::Proactor() : ProactorBase() {
}

Proactor::~Proactor() {
  idle_map_.clear();

  CHECK(is_stopped_);
  if (thread_id_ != -1U) {
    io_uring_queue_exit(&ring_);
  }
  VLOG(1) << "Closing wake_fd " << wake_fd_ << " ring fd: " << ring_.ring_fd;
}

void Proactor::Run() {
  VLOG(1) << "Proactor::Run";
  CHECK(tl_info_.owner) << "Init was not called";

  main_loop_ctx_ = fibers::context::active();
  fibers::scheduler* sched = main_loop_ctx_->get_scheduler();

  scheduler_ = new UringFiberAlgo(this);
  sched->set_algo(scheduler_);
  this_fiber::properties<FiberProps>().set_name("ioloop");

  is_stopped_ = false;

  constexpr size_t kBatchSize = 128;
  struct io_uring_cqe cqes[kBatchSize];
  static_assert(sizeof(cqes) == 2048);

  uint64_t num_stalls = 0, cqe_syscalls = 0, cqe_fetches = 0, loop_cnt = 0;
  uint32_t tq_seq = 0;
  uint32_t spin_loops = 0, num_task_runs = 0, task_interrupts = 0;
  uint32_t busy_sq_cnt = 0;
  Tasklet task;

  bool suspend_until_called = false;
  bool should_suspend = false;

  while (true) {
    ++loop_cnt;

    int num_submitted = io_uring_submit(&ring_);
    bool ring_busy = false;

    if (num_submitted >= 0) {
      if (num_submitted)
        DVLOG(3) << "Submitted " << num_submitted;
    } else if (num_submitted == -EBUSY) {
      VLOG(2) << "EBUSY " << io_uring_sq_ready(&ring_);
      ring_busy = true;
      num_submitted = 0;
      ++busy_sq_cnt;
    } else {
      URING_CHECK(num_submitted);
    }

    num_task_runs = 0;

    tq_seq = tq_seq_.load(std::memory_order_acquire);

    // This should handle wait-free and "brief" CPU-only tasks enqued using Async/Await
    // calls. We allocate the quota of 500K nsec (500usec) of CPU time per iteration
    // To save redundant timer-calls we start measuring time only when if the queue is not empty.
    if (task_queue_.try_dequeue(task)) {
      uint64_t task_start = GetClockNanos();
      // update thread-local clock service via GetMonotonicTimeNs().
      tl_info_.monotonic_time = task_start;
      do {
        task();
        ++num_task_runs;
        tl_info_.monotonic_time = GetClockNanos();
        if (task_start + 500000 < tl_info_.monotonic_time) {  // Break after 500usec
          ++task_interrupts;
          break;
        }
      } while (task_queue_.try_dequeue(task));

      task_queue_avail_.notifyAll();

      DVLOG(2) << "Tasks runs " << num_task_runs << "/" << spin_loops;
    }

    uint32_t cqe_count = IoRingPeek(ring_, cqes, kBatchSize);
    if (cqe_count) {
      unsigned total_fetched = cqe_count;
      while (true) {
        // Once we copied the data we can mark the cqe consumed.
        io_uring_cq_advance(&ring_, cqe_count);
        DVLOG(2) << "Fetched " << cqe_count << " cqes";
        DispatchCompletions(cqes, cqe_count);

        if (cqe_count < kBatchSize) {
          if (total_fetched > kBatchSize) {
            wait_for_cqe(&ring_, 0);
            total_fetched = 0;
          } else {
            break;
          }
        }
        cqe_count = IoRingPeek(ring_, cqes, kBatchSize);
        total_fetched += cqe_count;
      };

      for (auto& task_pair : schedule_periodic_list_) {
        SchedulePeriodic(task_pair.first, task_pair.second);
      }
      schedule_periodic_list_.clear();
      sqe_avail_.notifyAll();
    }

    // Check if we are notified by FiberSchedAlgo::notify().
    if (tq_seq & 1) {
      // We allow dispatch fiber to run.

      // We must reset LSB for both tq_seq and tq_seq_  so that if notify() was called after
      // yield(), tq_seq_ would be invalidated.

      tq_seq_.fetch_and(~1, std::memory_order_relaxed);
      tq_seq &= ~1;
      this_fiber::yield();
      DVLOG(2) << "this_fiber::yield_end " << spin_loops;

      // we preempted without passing through suspend_until.
      suspend_until_called = false;
    }

    if (should_suspend || sched->has_ready_fibers()) {
      // Suspend this fiber until others will run and get blocked.
      // Eventually UringFiberAlgo will resume back this fiber in suspend_until
      // function.
      DVLOG(2) << "Suspend ioloop " << should_suspend;
      uint64_t now = GetClockNanos();
      tl_info_.monotonic_time = now;

      // In general, SuspendIoLoop would always return true since it should execute all
      // fibers till exhaustion. However, since our scheduler can break off the execution and
      // switch back to ioloop, the control may resume without visiting in suspend_until().
      suspend_until_called = scheduler_->SuspendIoLoop(now);
      should_suspend = false;
      DVLOG(2) << "Resume ioloop " << suspend_until_called;
      continue;
    }

    if (cqe_count || !task_queue_.empty()) {
      continue;
    }

    ++cqe_syscalls;
    wait_for_cqe(&ring_, 0);  // nonblocking syscall to dive into kernel space.
    if (CQReadyCount(ring_)) {
      ++cqe_fetches;
      spin_loops = 0;
      continue;
    }

    if (!idle_map_.empty()) {  // TODO: to break upon timer constraints (~20usec).
      for (auto it = idle_map_.begin(); it != idle_map_.end(); ++it) {
        if (!it->second()) {
          idle_map_.erase(it);
          break;
        }
      }
      continue;
    }

    // Dispatcher runs the scheduling loop. Every time a fiber preempts it awakens dispatcher
    // so that when that we eventually get to the dispatcher fiber again. dispatcher calls
    // suspend_until() only when pick_next returns null, i.e. there are no active fibers to run.
    // Therefore we should not block on I/O before making sure that dispatcher has run with all
    // fibers being suspended so that dispatcher could call suspend_until in order to update timeout
    // if needed. We track whether suspend_until was called since the last preemption of io-loop and
    // if not, we suspend this fiber to allow the dispatch fiber to call suspend_until().
    if (!suspend_until_called) {
      should_suspend = true;
      continue;
    }

    // Lets spin a bit to make a system a bit more responsive.
    if (!ring_busy && ++spin_loops < kSpinLimit) {
      DVLOG(3) << "spin_loops " << spin_loops;
      // We should not spin too much using sched_yield or it burns a fuckload of cpu.
      continue;
    }

    spin_loops = 0;  // Reset the spinning.

    DCHECK_EQ(0U, tq_seq & 1) << tq_seq;
    DCHECK(suspend_until_called);

    /**
     * If tq_seq_ has changed since it was cached into tq_seq, then
     * EmplaceTaskQueue succeeded and we might have more tasks to execute - lets
     * run the loop again. Otherwise, set tq_seq_ to WAIT_SECTION_STATE, hinting that
     * we are going to stall now. Other threads will need to wake-up the ring
     * (see WakeRing()) but only one will actually call the syscall.
     */
    if (tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_acquire)) {
      if (is_stopped_)
        break;
      DVLOG(2) << "wait_for_cqe";
      wait_for_cqe(&ring_, 1);
      DVLOG(2) << "Woke up after wait_for_cqe " << tq_seq_.load(std::memory_order_acquire);

      tq_seq = 0;
      ++num_stalls;
      ++suspend_cnt_;

      // Reset all except the LSB bit that signals that we need to switch to dispatch fiber.
      tq_seq_.fetch_and(1, std::memory_order_release);
    }
  }

  VPRO(1) << "total/stalls/cqe_syscalls/cqe_fetches: " << loop_cnt << "/" << num_stalls << "/"
          << cqe_syscalls << "/" << cqe_fetches;

  VPRO(1) << "tq_wakeups/tq_full/tq_task_int/algo_notifies: " << tq_wakeup_ev_.load() << "/"
          << tq_full_ev_.load() << "/" << task_interrupts << "/" << algo_notify_cnt_.load();
  VPRO(1) << "busy_sq/get_entry_sq_full/get_entry_sq_err/get_entry_awaits/suspend_timer_fail: "
          << busy_sq_cnt << "/" << get_entry_sq_full_ << "/" << get_entry_submit_fail_ << "/"
          << get_entry_await_ << "/" << dispatch_suspend_timer_fail_;

  VPRO(1) << "centries size: " << centries_.size();
  centries_.clear();
}

LinuxSocketBase* Proactor::CreateSocket(int fd) {
  return new UringSocket{fd, this};
}

void Proactor::Init(size_t ring_size, int wq_fd) {
  CHECK_EQ(0U, ring_size & (ring_size - 1));
  CHECK_GE(ring_size, 8U);
  CHECK_EQ(0U, thread_id_) << "Init was already called";

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  CHECK(kver.kernel > 5 || (kver.kernel = 5 && kver.major >= 4))
      << "Versions 5.4 or higher are supported";

  io_uring_params params;
  memset(&params, 0, sizeof(params));

  if (FLAGS_proactor_register_fd && geteuid() == 0) {
    params.flags |= IORING_SETUP_SQPOLL;
    LOG_FIRST_N(INFO, 1) << "Root permissions - setting SQPOLL flag";
  }

  // Optionally reuse the already created work-queue from another uring.
  if (wq_fd > 0) {
    params.flags |= IORING_SETUP_ATTACH_WQ;
    params.wq_fd = wq_fd;
  }

  // it seems that SQPOLL requires registering each fd, including sockets fds.
  // need to check if its worth pursuing.
  // For sure not in short-term.
  // params.flags = IORING_SETUP_SQPOLL;
  VLOG(1) << "Create uring of size " << ring_size;

  // If this fails with 'can not allocate memory' most probably you need to increase maxlock limit.
  URING_CHECK(io_uring_queue_init_params(ring_size, &ring_, &params));
  fast_poll_f_ = (params.features & IORING_FEAT_FAST_POLL) != 0;
  sqpoll_f_ = (params.flags & IORING_SETUP_SQPOLL) != 0;

  wake_fixed_fd_ = wake_fd_;
  register_fd_ = FLAGS_proactor_register_fd;
  if (register_fd_) {
    register_fds_.resize(64, -1);
    register_fds_[0] = wake_fd_;
    wake_fixed_fd_ = 0;

    absl::Time start = absl::Now();
    int res = io_uring_register_files(&ring_, register_fds_.data(), register_fds_.size());
    absl::Duration duration = absl::Now() - start;
    VLOG(1) << "io_uring_register_files took " << absl::ToInt64Milliseconds(duration) << "ms";
    if (res < 0) {
      register_fd_ = 0;
      register_fds_ = decltype(register_fds_){};
    }
  }

  CHECK(fast_poll_f_);
  CHECK(params.features & IORING_FEAT_NODROP)
      << "IORING_FEAT_NODROP feature is not present in the kernel";

  CHECK(params.features & IORING_FEAT_SINGLE_MMAP);
  size_t sz = ring_.sq.ring_sz + params.sq_entries * sizeof(struct io_uring_sqe);
  LOG_FIRST_N(INFO, 1) << "IORing with " << params.sq_entries << " entries, allocated " << sz
                       << " bytes, cq_entries is " << *ring_.cq.kring_entries;
  CHECK_EQ(ring_size, params.sq_entries);  // Sanity.

  VerifyTimeoutSupport();
  ArmWakeupEvent();
  centries_.resize(params.sq_entries);  // .val = -1
  next_free_ce_ = 0;
  for (size_t i = 0; i < centries_.size() - 1; ++i) {
    centries_[i].val = i + 1;
  }

  thread_id_ = pthread_self();
  tl_info_.owner = this;
}

void Proactor::DispatchCompletions(io_uring_cqe* cqes, unsigned count) {
  for (unsigned i = 0; i < count; ++i) {
    auto& cqe = cqes[i];

    // I allocate range of 1024 reserved values for the internal Proactor use.

    if (cqe.user_data >= kUserDataCbIndex) {  // our heap range surely starts higher than 1k.
      size_t index = cqe.user_data - kUserDataCbIndex;
      DCHECK_LT(index, centries_.size());
      auto& e = centries_[index];
      DCHECK(e.cb) << index;

      CbType func;
      auto payload = e.val;
      func.swap(e.cb);

      // Set e to be the head of free-list.
      e.val = next_free_ce_;
      next_free_ce_ = index;

      func(cqe.res, cqe.flags, payload);
      continue;
    }

    if (cqe.user_data == kIgnoreIndex)
      continue;

    if (cqe.user_data == kWakeIndex) {
      // We were woken up. Need to rearm wake_fd_ poller.
      DCHECK_EQ(cqe.res, 8);
      DVLOG(2) << "PRO[" << tl_info_.proactor_index << "] Wakeup " << cqe.res << "/" << cqe.flags;

      // TODO: to move io_uring_get_sqe call from here to before we stall.
      ArmWakeupEvent();
      continue;
    }
    LOG(ERROR) << "Unrecognized user_data " << cqe.user_data;
  }
}

SubmitEntry Proactor::GetSubmitEntry(CbType cb, int64_t payload) {
  io_uring_sqe* res = io_uring_get_sqe(&ring_);
  if (res == NULL) {
    ++get_entry_sq_full_;
    int submitted = io_uring_submit(&ring_);
    if (submitted > 0) {
      res = io_uring_get_sqe(&ring_);
    } else {
      ++get_entry_submit_fail_;
      LOG_FIRST_N(INFO, 50) << "io_uring_submit err " << submitted;
    }

    if (res == NULL) {
      fibers::context* current = fibers::context::active();

      // TODO: we should call io_uring_submit() from here and busy poll on errors like
      // EBUSY, EAGAIN etc.
      CHECK(current != main_loop_ctx_) << "SQE overflow in the main context";
      CHECK(!current->is_context(fibers::type::dispatcher_context));
      ++get_entry_await_;
      WaitTillAvailable(1);
      res = io_uring_get_sqe(&ring_);  // now we should have the space.
      CHECK(res);
    }
  }

  memset(res, 0, sizeof(io_uring_sqe));

  if (cb) {
    if (next_free_ce_ < 0) {
      RegrowCentries();
      DCHECK_GT(next_free_ce_, 0);
    }

    res->user_data = next_free_ce_ + kUserDataCbIndex;
    DCHECK_LT(unsigned(next_free_ce_), centries_.size());

    auto& e = centries_[next_free_ce_];
    DCHECK(!e.cb);  // cb is undefined.
    DVLOG(2) << "GetSubmitEntry: index: " << next_free_ce_ << ", socket: " << payload;

    next_free_ce_ = e.val;
    e.cb = std::move(cb);
    e.val = payload;
    e.opcode = -1;
  } else {
    res->user_data = kIgnoreIndex;
  }

  return SubmitEntry{res};
}

void Proactor::RegrowCentries() {
  size_t prev = centries_.size();
  VLOG(1) << "RegrowCentries from " << prev << " to " << prev * 2;

  centries_.resize(prev * 2);  // grow by 2.
  next_free_ce_ = prev;
  for (; prev < centries_.size() - 1; ++prev)
    centries_[prev].val = prev + 1;
}

void Proactor::ArmWakeupEvent() {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  CHECK_NOTNULL(sqe);

  io_uring_prep_poll_add(sqe, wake_fixed_fd_, POLLIN);
  sqe->user_data = kIgnoreIndex;
  sqe->flags |= (register_fd_ ? IOSQE_FIXED_FILE : 0);
  sqe->flags |= IOSQE_IO_LINK;
  sqe = io_uring_get_sqe(&ring_);

  // drain the signal.
  static uint64_t donot_care;
  io_uring_prep_read(sqe, wake_fixed_fd_, &donot_care, 8, 0);
  sqe->user_data = kWakeIndex;
}

void Proactor::SchedulePeriodic(uint32_t id, PeriodicItem* item) {
  SubmitEntry se = GetSubmitEntry(
      [this, item](IoResult res, uint32_t flags, int64_t task_id) {
        DCHECK_GE(task_id, 0);
        this->PeriodicCb(res, task_id, std::move(item));
      },
      id);

  se.PrepTimeout(&item->period, false);
  item->val1 = se.sqe()->user_data;
}

void Proactor::PeriodicCb(IoResult res, uint32 task_id, PeriodicItem* item) {
  if (!item->in_map) { // has been removed from the map.
    delete item;
    return;
  }

  // -ECANCELED can happen only if in_map is false and it's handled above.
  CHECK_EQ(res, -ETIME);

  DCHECK(periodic_map_.find(task_id) != periodic_map_.end());
  item->task();

  schedule_periodic_list_.emplace_back(task_id, item);
}

void Proactor::CancelPeriodicInternal(uint32_t val1, uint32_t val2) {
  auto* me = fibers::context::active();
  auto cb = [me](IoResult res, uint32_t flags, int64_t task_id) {
    fibers::context::active()->schedule(me);
  };
  SubmitEntry se = GetSubmitEntry(std::move(cb), 0);
  se.PrepTimeoutRemove(val1);  // cancel using userdata id sent to io-uring.
  me->suspend();
}

unsigned Proactor::RegisterFd(int source_fd) {
  if (!register_fd_)
    return source_fd;

  auto next = std::find(register_fds_.begin() + next_free_fd_, register_fds_.end(), -1);
  if (next == register_fds_.end()) {
    size_t prev_sz = register_fds_.size();
    register_fds_.resize(prev_sz * 2, -1);
    register_fds_[prev_sz] = source_fd;
    next_free_fd_ = prev_sz + 1;

    CHECK_EQ(0, io_uring_register_files(&ring_, register_fds_.data(), register_fds_.size()));
    return prev_sz;
  }

  *next = source_fd;
  next_free_fd_ = next - register_fds_.begin();
  CHECK_EQ(1, io_uring_register_files_update(&ring_, next_free_fd_, &source_fd, 1));
  ++next_free_fd_;

  return next_free_fd_ - 1;
}

void Proactor::UnregisterFd(unsigned fixed_fd) {
  if (!register_fd_)
    return;

  CHECK_LT(fixed_fd, register_fds_.size());
  CHECK_GE(register_fds_[fixed_fd], 0);
  register_fds_[fixed_fd] = -1;

  CHECK_EQ(1, io_uring_register_files_update(&ring_, fixed_fd, &register_fds_[fixed_fd], 1));
  if (fixed_fd < next_free_fd_) {
    next_free_fd_ = fixed_fd;
  }
}

void Proactor::VerifyTimeoutSupport() {
  io_uring_sqe* sqe = CHECK_NOTNULL(io_uring_get_sqe(&ring_));
  timespec ts = {.tv_sec = 0, .tv_nsec = 10000};
  static_assert(sizeof(__kernel_timespec) == sizeof(timespec));

  io_uring_prep_timeout(sqe, (__kernel_timespec*)&ts, 0, IORING_TIMEOUT_ABS);
  sqe->user_data = 2;
  int submitted = io_uring_submit(&ring_);
  CHECK_EQ(1, submitted);

  io_uring_cqe* cqe = nullptr;
  while (true) {
    int res = io_uring_wait_cqe(&ring_, &cqe);
    if (res == 0)
      break;
    if (res != -EINTR) {
      LOG(FATAL) << "Unexpected error " << strerror(-res);
    }
  }

  CHECK_EQ(2U, cqe->user_data);

  CHECK(cqe->res == -ETIME || cqe->res == 0);
  io_uring_cq_advance(&ring_, 1);
}

const static uint64_t wake_val = 1;
void Proactor::WakeRing() {
  DVLOG(2) << "WakeRing " << tq_seq_.load(std::memory_order_relaxed);

  tq_wakeup_ev_.fetch_add(1, std::memory_order_relaxed);

  Proactor* caller = static_cast<Proactor*>(ProactorBase::me());

  // Disabled because it deadlocks in github actions.
  // It could be kernel issue or a bug in my code - needs investigation.
  if (false && caller) {
    SubmitEntry se = caller->GetSubmitEntry(nullptr, 0);
    se.PrepWrite(wake_fd_, &wake_val, sizeof(wake_val), 0);
  } else {
    CHECK_EQ(8, write(wake_fd_, &wake_val, sizeof(wake_val)));
  }
}

FiberCall::FiberCall(Proactor* proactor, uint32_t timeout_msec) : me_(fibers::context::active()) {
  auto waker = [this](Proactor::IoResult res, uint32_t flags, int64_t) {
    io_res_ = res;
    res_flags_ = flags;
    fibers::context::active()->schedule(me_);
  };

  if (timeout_msec != UINT32_MAX) {
    proactor->WaitTillAvailable(2);
  }
  se_ = proactor->GetSubmitEntry(std::move(waker), 0);

  if (timeout_msec != UINT32_MAX) {
    se_.sqe()->flags |= IOSQE_IO_LINK;

    tm_ = proactor->GetSubmitEntry(nullptr, 0);

    // We must keep ts_ as member function so it could be accessed after this function scope.
    ts_.tv_sec = (timeout_msec / 1000);
    ts_.tv_nsec = (timeout_msec % 1000) * 1000000;
    tm_.PrepLinkTimeout(&ts_);  // relative timeout.
  }
}

FiberCall::~FiberCall() {
  CHECK(!me_) << "Get was not called!";
}

}  // namespace uring
}  // namespace util

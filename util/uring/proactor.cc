// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include <liburing.h>
#include <poll.h>
#include <string.h>
#include <sys/eventfd.h>

#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>

#include "absl/base/attributes.h"
#include "base/flags.h"
#include "base/histogram.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/uring_socket.h"

ABSL_FLAG(bool, proactor_register_fd, false, "If true tries to register file descriptors");

#define URING_CHECK(x)                                                                \
  do {                                                                                \
    int __res_val = (x);                                                              \
    if (ABSL_PREDICT_FALSE(__res_val < 0)) {                                          \
      LOG(FATAL) << "Error " << (-__res_val)                                          \
                 << " evaluating '" #x "': " << detail::SafeErrorMessage(-__res_val); \
    }                                                                                 \
  } while (false)

#ifndef __NR_io_uring_enter
#define __NR_io_uring_enter 426
#endif

#define VPRO(verbosity) VLOG(verbosity) << "PRO[" << tl_info_.proactor_index << "] "

using namespace boost;
using namespace std;
namespace ctx = boost::context;

namespace util {
namespace uring {

namespace {

void wait_for_cqe(io_uring* ring, unsigned wait_nr, sigset_t* sig = NULL) {
  struct io_uring_cqe* cqe_ptr = nullptr;

  int res = io_uring_wait_cqes(ring, &cqe_ptr, wait_nr, NULL, sig);
  if (res < 0) {
    res = -res;
    LOG_IF(ERROR, res != EAGAIN && res != EINTR) << detail::SafeErrorMessage(res);
  }
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

}  // namespace

Proactor::Proactor() : ProactorBase() {
}

Proactor::~Proactor() {
  on_idle_map_.clear();

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

  uint64_t num_stalls = 0, cqe_fetches = 0, loop_cnt = 0, num_suspends = 0;
  uint32_t tq_seq = 0;
  uint32_t spin_loops = 0, num_task_runs = 0, task_interrupts = 0;
  uint32_t busy_sq_cnt = 0;
  Tasklet task;

  bool suspendioloop_called = false, should_suspend = false;
  base::Histogram hist;

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

  spin_start:
    tq_seq = tq_seq_.load(memory_order_acquire);

    // This should handle wait-free and "brief" CPU-only tasks enqued using Async/Await
    // calls. We allocate quota of 500K nsec (500usec) of CPU time per iteration
    // To save redundant timer-calls we start measuring time only when if the queue is not empty.
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

#ifndef NDEBUG
      hist.Add(cnt);
#endif
    }

    uint32_t cqe_count = IoRingPeek(ring_, cqes, kBatchSize);
    if (cqe_count) {
      ++cqe_fetches;
      tl_info_.monotonic_time = GetClockNanos();

      while (true) {
        // Once we copied the data we can mark the cqe consumed.
        io_uring_cq_advance(&ring_, cqe_count);
        VPRO(2) << "Fetched " << cqe_count << " cqes";
        DispatchCompletions(cqes, cqe_count);

        if (cqe_count < kBatchSize) {
          break;
        }
        cqe_count = IoRingPeek(ring_, cqes, kBatchSize);
      };

      // In case some of the timer completions filled schedule_periodic_list_.
      for (auto& task_pair : schedule_periodic_list_) {
        SchedulePeriodic(task_pair.first, task_pair.second);
      }
      schedule_periodic_list_.clear();
      sqe_avail_.notifyAll();
    }

    // Check if we are notified by FiberSchedAlgo::notify() via RequestDispatcher
    // that sets lsb of tq_seq_.
    if (tq_seq & 1) {
      // We allow dispatch fiber to run.

      unsigned read_cnt = scheduler_->ready_cnt();

      // We must reset LSB for both tq_seq and tq_seq_  so that if notify() was called after
      // yield(), tq_seq_ would be invalidated.
      tq_seq_.fetch_and(~1, memory_order_acq_rel);
      tq_seq &= ~1;
      this_fiber::yield();
      VPRO(2) << "this_fiber::yield_end " << loop_cnt << " " << read_cnt << " "
              << scheduler_->ready_cnt();

      // we preempted without passing through suspend_until.
      suspendioloop_called = false;
    }

    if (should_suspend || sched->has_ready_fibers() ||
        tq_seq_.load(memory_order_relaxed) != tq_seq) {
      // Suspend this fiber until others will run and get blocked.
      // Eventually UringFiberAlgo will resume back this fiber in suspend_until
      // function.
      DVLOG(2) << "Suspend ioloop " << should_suspend;
      uint64_t now = GetClockNanos();
      tl_info_.monotonic_time = now;

      // In general, SuspendIoLoop would always return true since it should execute all
      // fibers till exhaustion. However, our scheduler can break off the execution of
      // worker fibers and switch back to ioloop earlier.
      // We should not halt in wait_for_cqe before suspending here again and exhausting
      // all the ready fibers.
      suspendioloop_called = scheduler_->SuspendIoLoop(now);
      should_suspend = false;
      ++num_suspends;
      VPRO(2) << "Resuming ioloop " << loop_cnt << " " << scheduler_->ready_cnt();

      continue;
    }

    if (cqe_count) {
      VPRO(2) << "cqe_count, continue at " << loop_cnt;
      continue;
    }

    // Dispatcher runs the scheduling loop. Every time a fiber preempts it awakens dispatcher
    // so that when that we eventually get to the dispatcher fiber again. dispatcher calls
    // suspend_until() only when pick_next returns null, i.e. there are no active fibers to run.
    // Therefore we should not block on I/O before making sure that dispatcher has run with all
    // fibers being suspended so that dispatcher could call suspend_until in order to update timeout
    // if needed. We track whether suspend_until was called since the last preemption of io-loop and
    // if not, we suspend this fiber to allow the dispatch fiber to call suspend_until().
    if (!suspendioloop_called) {
      should_suspend = true;
      goto spin_start;
    }

    if (!on_idle_map_.empty()) {
      // if on_idle_map_ is not empty we should not block on WAIT_SECTION_STATE.
      // Instead we use the cpu time on doing on_idle work.
      wait_for_cqe(&ring_, 0);  // a dip into kernel to fetch more cqes.

      if (!CQReadyCount(ring_)) {
        RunOnIdleTasks();
      }

      continue;  // continue spinning until on_idle_map_ is empty.
    }

    // Lets spin a bit to make a system a bit more responsive.
    // Important to spin a bit, otherwise we put too much pressure on  eventfd_write.
    // and we enter too often into kernel space.
    if (!ring_busy && spin_loops++ < kMaxSpinLimit) {
      DVLOG(3) << "spin_loops " << spin_loops;

      // We should not spin too much using sched_yield or it burns a fuckload of cpu.
      Pause(spin_loops);
      goto spin_start;
    }

    spin_loops = 0;  // Reset the spinning.

    DCHECK_EQ(0U, tq_seq & 1) << tq_seq;
    DCHECK(suspendioloop_called);

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
      DCHECK(!should_suspend);
      DCHECK(!sched->has_ready_fibers());

      if (task_queue_.empty()) {
        this_fiber::yield();
        if (!sched->has_ready_fibers()) {
          VPRO(2) << "wait_for_cqe " << loop_cnt;
          wait_for_cqe(&ring_, 1);
          VPRO(2) << "Woke up after wait_for_cqe ";

          ++num_stalls;
        }
      }

      tq_seq = 0;
      // Reset all except the LSB bit that signals that we need to switch to dispatch fiber.
      tq_seq_.fetch_and(1, std::memory_order_release);
    }
  }

#ifndef NDEBUG
  VPRO(1) << "Task runs histogram: " << hist.ToString();
#endif

  VPRO(1) << "total/stalls/cqe_fetches/num_suspends: " << loop_cnt << "/" << num_stalls << "/"
          << cqe_fetches << "/" << num_suspends;
  VPRO(1) << "Tasks/loop: " << double(num_task_runs) / loop_cnt;
  VPRO(1) << "tq_wakeups/tq_full/tq_task_int/algo_notifies: " << tq_wakeup_ev_.load() << "/"
          << tq_full_ev_.load() << "/" << task_interrupts << "/" << algo_notify_cnt_.load();
  VPRO(1) << "busy_sq/get_entry_sq_full/get_entry_sq_err/get_entry_awaits/pending_callbacks: "
          << busy_sq_cnt << "/" << get_entry_sq_full_ << "/" << get_entry_submit_fail_ << "/"
          << get_entry_await_ << "/" << pending_cb_cnt_;

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

  CHECK(kver.kernel > 5 || (kver.kernel = 5 && kver.major >= 8))
      << "Versions 5.8 or higher are supported";

  io_uring_params params;
  memset(&params, 0, sizeof(params));

#if 0
  if (FLAGS_proactor_register_fd && geteuid() == 0) {
    params.flags |= IORING_SETUP_SQPOLL;
    LOG_FIRST_N(INFO, 1) << "Root permissions - setting SQPOLL flag";
  }

  // Optionally reuse the already created work-queue from another uring.
  if (wq_fd > 0) {
    params.flags |= IORING_SETUP_ATTACH_WQ;
    params.wq_fd = wq_fd;
  }
#endif

  // it seems that SQPOLL requires registering each fd, including sockets fds.
  // need to check if its worth pursuing.
  // For sure not in short-term.
  // params.flags = IORING_SETUP_SQPOLL;
  VLOG(1) << "Create uring of size " << ring_size;

  // If this fails with 'can not allocate memory' most probably you need to increase maxlock limit.
  int init_res = io_uring_queue_init_params(ring_size, &ring_, &params);
  if (init_res < 0) {
    init_res = -init_res;
    if (init_res == ENOMEM) {
      LOG(ERROR)
          << "io_uring does not have enough memory. That can happen when your max locked "
             "memory is too limited. If you run me via docker, try adding '--ulimit memlock=-1' to"
             "docker run command";
      exit(1);
    }
    LOG(FATAL) << "Error initializing io_uring: (" << init_res << ") "
               << detail::SafeErrorMessage(init_res);
  }
  sqpoll_f_ = (params.flags & IORING_SETUP_SQPOLL) != 0;

  unsigned req_feats = IORING_FEAT_SINGLE_MMAP | IORING_FEAT_FAST_POLL | IORING_FEAT_NODROP;
  CHECK_EQ(req_feats, params.features & req_feats)
      << "required feature feature is not present in the kernel";

  int res = io_uring_register_ring_fd(&ring_);
  VLOG_IF(1, res < 0) << "io_uring_register_ring_fd failed: " << -res;

  wake_fixed_fd_ = wake_fd_;
  register_fd_ = absl::GetFlag(FLAGS_proactor_register_fd);
  if (register_fd_) {
    register_fds_.resize(64, -1);
    register_fds_[0] = wake_fd_;
    wake_fixed_fd_ = 0;

    absl::Time start = absl::Now();
    int res = io_uring_register_files(&ring_, register_fds_.data(), register_fds_.size());
    absl::Duration duration = absl::Now() - start;
    VLOG(1) << "io_uring_register_files took " << absl::ToInt64Microseconds(duration) << " usec";
    CHECK_EQ(0, res);
  }

  size_t sz = ring_.sq.ring_sz + params.sq_entries * sizeof(struct io_uring_sqe);
  LOG_FIRST_N(INFO, 1) << "IORing with " << params.sq_entries << " entries, allocated " << sz
                       << " bytes, cq_entries is " << *ring_.cq.kring_entries;
  CHECK_EQ(ring_size, params.sq_entries);  // Sanity.

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
      --pending_cb_cnt_;
      func(cqe.res, cqe.flags, payload);
      continue;
    }

    if (cqe.user_data == kIgnoreIndex)
      continue;

    if (cqe.user_data == kWakeIndex) {
      // We were woken up. Need to rearm wakeup poller.
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
    DVLOG(3) << "GetSubmitEntry: index: " << next_free_ce_ << ", payload: " << payload;

    next_free_ce_ = e.val;
    e.cb = std::move(cb);
    e.val = payload;
    ++pending_cb_cnt_;
  } else {
    res->user_data = kIgnoreIndex;
  }

  return SubmitEntry{res};
}

void Proactor::RegrowCentries() {
  size_t prev = centries_.size();
  VLOG(1) << "RegrowCentries from " << prev << " to " << prev * 2
          << " pending cb-cnt: " << pending_cb_cnt_;

  centries_.resize(prev * 2);  // grow by 2.
  next_free_ce_ = prev;
  for (; prev < centries_.size() - 1; ++prev)
    centries_[prev].val = prev + 1;
}

void Proactor::ArmWakeupEvent() {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  CHECK_NOTNULL(sqe);

  io_uring_prep_poll_add(sqe, wake_fixed_fd_, POLLIN);
  uint8_t flag = register_fd_ ? IOSQE_FIXED_FILE : 0;
  sqe->user_data = kIgnoreIndex;
  sqe->flags |= (flag | IOSQE_IO_LINK);
  sqe = io_uring_get_sqe(&ring_);

  // drain the signal.
  // we do not have to use threadlocal but we use it for performance reasons
  // to reduce cache thrashing.
  static thread_local uint64_t donot_care;
  io_uring_prep_read(sqe, wake_fixed_fd_, &donot_care, 8, 0);
  sqe->user_data = kWakeIndex;
  sqe->flags |= flag;
}

void Proactor::SchedulePeriodic(uint32_t id, PeriodicItem* item) {
  SubmitEntry se = GetSubmitEntry(
      [this, item](IoResult res, uint32_t flags, int64_t task_id) {
        DCHECK_GE(task_id, 0);
        this->PeriodicCb(res, task_id, std::move(item));
      },
      id);

  se.PrepTimeout(&item->period, false);
  DVLOG(2) << "Scheduling timer " << item << " userdata: " << se.sqe()->user_data;

  item->val1 = se.sqe()->user_data;
}

void Proactor::PeriodicCb(IoResult res, uint32 task_id, PeriodicItem* item) {
  if (!item->in_map) {  // has been removed from the map.
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

  DVLOG(1) << "Cancel timer " << val1 << ", cb userdata: " << se.sqe()->user_data;
  se.PrepTimeoutRemove(val1);  // cancel using userdata id sent to io-uring.
  me->suspend();
}

unsigned Proactor::RegisterFd(int source_fd) {
  DCHECK(register_fd_);

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
  DCHECK(register_fd_);

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
    // it's wake_fd_ and not wake_fixed_fd_ deliberately since we use plain write and not iouring.
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

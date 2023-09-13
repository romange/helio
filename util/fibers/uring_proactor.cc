// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/uring_proactor.h"

#include <absl/base/attributes.h>
#include <liburing.h>
#include <poll.h>
#include <string.h>
#include <sys/eventfd.h>

#include "base/flags.h"
#include "base/histogram.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "util/fibers/detail/scheduler.h"
#include "util/fibers/uring_socket.h"

ABSL_FLAG(bool, proactor_register_fd, false, "If true tries to register file descriptors");

#define URING_CHECK(x)                                                        \
  do {                                                                        \
    int __res_val = (x);                                                      \
    if (ABSL_PREDICT_FALSE(__res_val < 0)) {                                  \
      LOG(FATAL) << "Error " << (-__res_val)                                  \
                 << " evaluating '" #x "': " << SafeErrorMessage(-__res_val); \
    }                                                                         \
  } while (false)

#define VPRO(verbosity) VLOG(verbosity) << "PRO[" << tl_info_.proactor_index << "] "

using namespace std;

namespace util {

using detail::SafeErrorMessage;

namespace fb2 {

using detail::FiberInterface;

namespace {

void wait_for_cqe(io_uring* ring, unsigned wait_nr, __kernel_timespec* ts, sigset_t* sig = NULL) {
  struct io_uring_cqe* cqe_ptr = nullptr;

  int res = io_uring_wait_cqes(ring, &cqe_ptr, wait_nr, ts, sig);
  if (res < 0) {
    res = -res;
    LOG_IF(ERROR, res != EAGAIN && res != EINTR && res != ETIME) << SafeErrorMessage(res);
  }
}

constexpr uint64_t kIgnoreIndex = 0;
constexpr uint64_t kWakeIndex = 1;

constexpr uint64_t kUserDataCbIndex = 1024;
constexpr uint16_t kMsgRingSubmitTag = 1;
constexpr uint16_t kTimeoutSubmitTag = 2;

}  // namespace

UringProactor::UringProactor() : ProactorBase() {
}

UringProactor::~UringProactor() {
  CHECK(is_stopped_);
  if (thread_id_ != -1U) {
    io_uring_queue_exit(&ring_);
  }
  VLOG(1) << "Closing wake_fd " << wake_fd_ << " ring fd: " << ring_.ring_fd;
}

void UringProactor::Init(size_t ring_size, int wq_fd) {
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

  // If we setup flags that kernel does not recognize, it fails the setup call.
  if (kver.kernel > 5 || (kver.kernel == 5 && kver.major >= 19)) {
    params.flags |= IORING_SETUP_SUBMIT_ALL | IORING_SETUP_COOP_TASKRUN;
  }

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
               << SafeErrorMessage(init_res);
  }
  sqpoll_f_ = (params.flags & IORING_SETUP_SQPOLL) != 0;

  io_uring_probe* uring_probe = io_uring_get_probe_ring(&ring_);

  msgring_f_ = io_uring_opcode_supported(uring_probe, IORING_OP_MSG_RING);
  io_uring_free_probe(uring_probe);
  VLOG_IF(1, msgring_f_) << "msgring supported!";

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
    centries_[i].index = i + 1;
  }

  thread_id_ = pthread_self();
  tl_info_.owner = this;
}

void UringProactor::DispatchCqe(detail::FiberInterface* current, const io_uring_cqe& cqe) {
  uint32_t user_data = cqe.user_data & 0xFFFFFFFF;
  if (user_data >= kUserDataCbIndex) {  // our heap range surely starts higher than 1k.
    if (ABSL_PREDICT_FALSE(cqe.user_data == UINT64_MAX)) {
      base::sys::KernelVersion kver;
      base::sys::GetKernelVersion(&kver);

      LOG(ERROR) << "Fatal error that is most likely caused by a bug in kernel.";

      LOG(ERROR) << "Kernel version: " << kver.kernel << "." << kver.major << "." << kver.minor;
      LOG(ERROR) << "If you are running on WSL2 or using Docker Desktop, try upgrading it "
                    "to kernel 5.15 or later.";
      LOG(ERROR) << "If you are running dragonfly - you can workaround the bug "
                    "with `--force_epoll` flag. Exiting...";
      exit(1);
    }

    size_t index = user_data - kUserDataCbIndex;
    DCHECK_LT(index, centries_.size());
    auto& e = centries_[index];
    DCHECK(e.cb) << index;

    CbType func = std::move(e.cb);

    // Set e to be the head of free-list.
    e.index = next_free_ce_;
    next_free_ce_ = index;
    --pending_cb_cnt_;
    func(current, cqe.res, cqe.flags);
    return;
  }

  // We ignore ECANCELED because submissions with link_timeout that finish successfully generate
  // CQE with ECANCELED for the subsequent linked submission. See io_uring_enter(2) for more info.
  // ETIME is when a timer cqe fully completes.
  if (cqe.res < 0 && cqe.res != -ECANCELED && cqe.res != -ETIME) {
    LOG(WARNING) << "CQE error: " << -cqe.res << " cqe_type=" << (cqe.user_data >> 32);
  }

  if (user_data == kIgnoreIndex)
    return;

  if (user_data == kWakeIndex) {
    // We were woken up. Need to rearm wakeup poller.
    DCHECK_EQ(cqe.res, 8);
    DVLOG(2) << "PRO[" << tl_info_.proactor_index << "] Wakeup " << cqe.res << "/" << cqe.flags;

    // TODO: to move io_uring_get_sqe call from here to before we stall.
    ArmWakeupEvent();
    return;
  }
  LOG(ERROR) << "Unrecognized user_data " << cqe.user_data;
}

SubmitEntry UringProactor::GetSubmitEntry(CbType cb, uint16_t submit_tag) {
  io_uring_sqe* res = io_uring_get_sqe(&ring_);
  if (res == NULL) {
    ++get_entry_sq_full_;
    int submitted = io_uring_submit(&ring_);
    if (submitted > 0) {
      res = io_uring_get_sqe(&ring_);
    } else {
      ++get_entry_submit_fail_;
      LOG(FATAL) << "Fatal error submitting to iouring: " << -submitted;
    }
  }

  memset(res, 0, sizeof(io_uring_sqe));

  if (cb) {
    if (next_free_ce_ < 0) {
      RegrowCentries();
      DCHECK_GT(next_free_ce_, 0);
    }
    res->user_data = (next_free_ce_ + kUserDataCbIndex) | (uint64_t(submit_tag) << 32);
    DCHECK_LT(unsigned(next_free_ce_), centries_.size());

    auto& e = centries_[next_free_ce_];
    DCHECK(!e.cb);  // cb is undefined.
    DVLOG(3) << "GetSubmitEntry: index: " << next_free_ce_;

    next_free_ce_ = e.index;
    e.cb = std::move(cb);
    ++pending_cb_cnt_;
  } else {
    res->user_data = kIgnoreIndex | (uint64_t(submit_tag) << 32);
  }

  return SubmitEntry{res};
}

int UringProactor::RegisterBuffers() {
  unique_ptr<uint8_t[]> reg_buf(new uint8_t[1U << 16]);
  iovec vec[1];
  vec[0].iov_base = reg_buf.get();
  vec[0].iov_len = 1U << 16;
  int res = io_uring_register_buffers(&ring_, vec, 1);
  if (res < 0) {
    return -res;
  }

  free_req_buf_id_ = 0;
  for (int i = 0; i < 1024; ++i) {
    uint8_t* next = reg_buf.get() + i * 64;
    absl::little_endian::Store16(next, i + 1);
  }

  registered_buf_ = std::move(reg_buf);
  return 0;
}

uint8_t* UringProactor::ProvideRegisteredBuffer() {
  if (free_req_buf_id_ < 0 || free_req_buf_id_ >= 1024)
    return nullptr;

  int res = free_req_buf_id_;
  free_req_buf_id_ = absl::little_endian::Load16(&registered_buf_[free_req_buf_id_ * 64]);
  return registered_buf_.get() + res * 64;
}

void UringProactor::ReturnRegisteredBuffer(uint8_t* addr) {
  DCHECK(registered_buf_);
  intptr_t offs = addr - registered_buf_.get();
  DCHECK_GE(offs, 0);
  DCHECK_LT(offs, 1 << 16);

  unsigned buf_id = offs / 64;
  absl::little_endian::Store16(&registered_buf_[buf_id * 64], free_req_buf_id_);
  free_req_buf_id_ = buf_id;
}

UringProactor::EpollIndex UringProactor::EpollAdd(int fd, EpollCB cb, uint32_t event_mask) {
  CHECK_GT(event_mask, 0U);

  if (next_epoll_free_ == -1) {
    size_t prev = epoll_entries_.size();
    if (prev == 0) {
      epoll_entries_.resize(8);
    } else {
      epoll_entries_.resize(prev * 2);
    }
    next_epoll_free_ = prev;
    for (; prev < epoll_entries_.size() - 1; ++prev)
      epoll_entries_[prev].index = prev + 1;
  }

  auto& epoll = epoll_entries_[next_epoll_free_];
  unsigned id = next_epoll_free_;
  next_epoll_free_ = epoll.index;

  epoll.cb = std::move(cb);
  epoll.fd = fd;
  epoll.event_mask = event_mask;
  EpollAddInternal(id);

  return id;
}

void UringProactor::EpollDel(EpollIndex id) {
  CHECK_LT(id, epoll_entries_.size());
  auto& epoll = epoll_entries_[id];
  epoll.event_mask = 0;
  unsigned uid = epoll.index;

  FiberCall fc(this);
  fc->PrepPollRemove(uid);
  IoResult res = fc.Get();
  if (res == 0) {  // removed from iouring.
    EpollDelInternal(id);
  }
}

void UringProactor::RegrowCentries() {
  size_t prev = centries_.size();
  VLOG(1) << "RegrowCentries from " << prev << " to " << prev * 2
          << " pending cb-cnt: " << pending_cb_cnt_;

  centries_.resize(prev * 2);  // grow by 2.
  next_free_ce_ = prev;
  for (; prev < centries_.size() - 1; ++prev)
    centries_[prev].index = prev + 1;
}

void UringProactor::ArmWakeupEvent() {
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

void UringProactor::SchedulePeriodic(uint32_t id, PeriodicItem* item) {
  VPRO(2) << "SchedulePeriodic " << id;

  SubmitEntry se =
      GetSubmitEntry([this, id, item](detail::FiberInterface*, IoResult res, uint32_t flags) {
        this->PeriodicCb(res, id, std::move(item));
      });

  se.PrepTimeout(&item->period, false);
  DVLOG(2) << "Scheduling timer " << item << " userdata: " << se.sqe()->user_data;

  item->val1 = se.sqe()->user_data;
}

void UringProactor::PeriodicCb(IoResult res, uint32 task_id, PeriodicItem* item) {
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

void UringProactor::CancelPeriodicInternal(uint32_t val1, uint32_t val2) {
  auto* me = detail::FiberActive();
  auto cb = [me](detail::FiberInterface* current, IoResult res, uint32_t flags) {
    current->ActivateOther(me);
  };
  SubmitEntry se = GetSubmitEntry(std::move(cb));

  DVLOG(1) << "Cancel timer " << val1 << ", cb userdata: " << se.sqe()->user_data;
  se.PrepTimeoutRemove(val1);  // cancel using userdata id sent to io-uring.
  me->Suspend();
}

unsigned UringProactor::RegisterFd(int source_fd) {
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

void UringProactor::UnregisterFd(unsigned fixed_fd) {
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

LinuxSocketBase* UringProactor::CreateSocket(int fd) {
  return new UringSocket{fd, this};
}

void UringProactor::MainLoop(detail::Scheduler* scheduler) {
  constexpr size_t kBatchSize = 128;
  struct io_uring_cqe cqes[kBatchSize];
  static_assert(sizeof(cqes) == 2048);

  uint64_t num_stalls = 0, cqe_fetches = 0, loop_cnt = 0, num_submits = 0;
  uint32_t tq_seq = 0;
  uint32_t spin_loops = 0, num_task_runs = 0, task_interrupts = 0;
  uint32_t busy_sq_cnt = 0;
  Tasklet task;

  FiberInterface* dispatcher = detail::FiberActive();

  while (true) {
    ++loop_cnt;

    int num_submitted = io_uring_submit(&ring_);
    bool ring_busy = false;

    if (num_submitted >= 0) {
      num_submits += (num_submitted != 0);
      if (num_submitted) {
        DVLOG(3) << "Submitted " << num_submitted;
      }
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
    }

    uint32_t cqe_count = 0;
    unsigned ring_head;
    struct io_uring_cqe* cqe;
    io_uring_for_each_cqe(&ring_, ring_head, cqe) {
      ++cqe_count;
      DispatchCqe(dispatcher, *cqe);
    }

    if (cqe_count) {
      ++cqe_fetches;
      io_uring_cq_advance(&ring_, cqe_count);

      // In case some of the timer completions filled schedule_periodic_list_.
      for (auto& task_pair : schedule_periodic_list_) {
        SchedulePeriodic(task_pair.first, task_pair.second);
      }
      schedule_periodic_list_.clear();
      sqe_avail_.notifyAll();
    }

    scheduler->ProcessRemoteReady();

    if (scheduler->HasSleepingFibers()) {
      ProcessSleepFibers(scheduler);
    }

    // must be if and not while (or at most k iterations for while) because
    // otherwise fibers that yield won't allow dispatcher to grab i/o events since it will be
    // stuck here.
    if (scheduler->HasReady()) {
      cqe_count = 1;
      FiberInterface* fi = scheduler->PopReady();
      DCHECK(!fi->list_hook.is_linked());
      DCHECK(!fi->sleep_hook.is_linked());
      scheduler->AddReady(dispatcher);

      DVLOG(2) << "Switching to " << fi->name();

      fi->SwitchTo();
    }

    if (cqe_count) {
      continue;
    }

    bool should_spin = RunOnIdleTasks();
    if (should_spin) {
      // if on_idle_map_ is not empty we should not block on WAIT_SECTION_STATE.
      // Instead we use the cpu time on doing on_idle work.
      wait_for_cqe(&ring_, 0, nullptr);  // a dip into kernel to fetch more cqes.

      continue;  // continue spinning until on_idle_map_ is empty.
    }

    // Lets spin a bit to make a system a bit more responsive.
    // Important to spin a bit, otherwise we put too much pressure on  eventfd_write.
    // and we enter too often into kernel space.
    if (!ring_busy && spin_loops++ < 15) {
      DVLOG(3) << "spin_loops " << spin_loops;

      // We should not spin too much using sched_yield or it burns a fuckload of cpu.
      scheduler->DestroyTerminated();
      scheduler->RunDeferred();

      // Pause(spin_loops);
      continue;
    }

    spin_loops = 0;  // Reset the spinning.

    /**
     * If tq_seq_ has changed since it was cached into tq_seq, then
     * EmplaceTaskQueue succeeded and we might have more tasks to execute - lets
     * run the loop again. Otherwise, set tq_seq_ to WAIT_SECTION_STATE, hinting that
     * we are going to stall now. Other threads will need to wake-up the ring
     * (see WakeRing()) but only one will actually call the syscall.
     */
    if (tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_acquire)) {
      if (is_stopped_) {
        tq_seq_.store(0, std::memory_order_release);  // clear WAIT section
        break;
      }

      DCHECK(!scheduler->HasReady());

      if (task_queue_.empty()) {
        VPRO(2) << "wait_for_cqe " << loop_cnt;
        __kernel_timespec ts{0, 0};
        __kernel_timespec* ts_arg = nullptr;

        if (scheduler->HasSleepingFibers()) {
          constexpr uint64_t kNsFreq = 1000000000ULL;
          auto tp = scheduler->NextSleepPoint();
          auto now = chrono::steady_clock::now();
          if (now < tp) {
            auto ns = chrono::duration_cast<chrono::nanoseconds>(tp - now).count();
            ts.tv_sec = ns / kNsFreq;
            ts.tv_nsec = ns % kNsFreq;
          }
          ts_arg = &ts;
        }
        wait_for_cqe(&ring_, 1, ts_arg);
        VPRO(2) << "Woke up after wait_for_cqe ";

        ++num_stalls;
      }

      tq_seq = 0;
      tq_seq_.store(0, std::memory_order_release);
      goto spin_start;
    }
  }

  VPRO(1) << "total/stalls/cqe_fetches/num_submits: " << loop_cnt << "/" << num_stalls << "/"
          << cqe_fetches << "/" << num_submits;
  VPRO(1) << "Tasks/loop: " << double(num_task_runs) / loop_cnt;
  VPRO(1) << "tq_wakeups/tq_wakeup_saved/tq_full/tq_task_int: " << tq_wakeup_ev_.load() << "/"
          << tq_wakeup_save_ev_.load() << "/" << tq_full_ev_.load() << "/" << task_interrupts;
  VPRO(1) << "busy_sq/get_entry_sq_full/get_entry_sq_err/get_entry_awaits/pending_callbacks: "
          << busy_sq_cnt << "/" << get_entry_sq_full_ << "/" << get_entry_submit_fail_ << "/"
          << get_entry_await_ << "/" << pending_cb_cnt_;

  VPRO(1) << "centries size: " << centries_.size();
  centries_.clear();
}

const static uint64_t wake_val = 1;

void UringProactor::WakeRing() {
  tq_wakeup_ev_.fetch_add(1, std::memory_order_relaxed);

  UringProactor* caller = static_cast<UringProactor*>(ProactorBase::me());

  DCHECK(caller != this);

  if (caller && caller->msgring_f_) {
    SubmitEntry se = caller->GetSubmitEntry(nullptr, kMsgRingSubmitTag);
    se.PrepMsgRing(ring_.ring_fd, 0, 0);
  } else {
    // it's wake_fd_ and not wake_fixed_fd_ deliberately since we use plain write and not iouring.
    CHECK_EQ(8, write(wake_fd_, &wake_val, sizeof(wake_val)));
  }
}

void UringProactor::EpollAddInternal(EpollIndex id) {
  auto uring_cb = [id, this](detail::FiberInterface* p, IoResult res, uint32_t flags) {
    auto& epoll = epoll_entries_[id];
    if (res >= 0) {
      epoll.cb(res);

      if (epoll.event_mask)
        EpollAddInternal(id);
      else
        EpollDelInternal(id);
    } else {
      res = -res;
      LOG_IF(ERROR, res != ECANCELED) << "EpollAddInternal: unexpected error " << res;
    }
  };

  SubmitEntry se = GetSubmitEntry(std::move(uring_cb));
  auto& epoll = epoll_entries_[id];
  se.PrepPollAdd(epoll.fd, epoll.event_mask);
  epoll.index = se.sqe()->user_data;
}

void UringProactor::EpollDelInternal(EpollIndex id) {
  DVLOG(1) << "EpollDelInternal " << id;

  auto& epoll = epoll_entries_[id];
  epoll.index = next_epoll_free_;
  epoll.cb = nullptr;

  next_epoll_free_ = id;
}

FiberCall::FiberCall(UringProactor* proactor, uint32_t timeout_msec) : me_(detail::FiberActive()) {
  auto waker = [this](detail::FiberInterface* current, UringProactor::IoResult res,
                      uint32_t flags) {
    io_res_ = res;
    res_flags_ = flags;
    current->ActivateOther(me_);
  };

  if (timeout_msec != UINT32_MAX) {
    proactor->WaitTillAvailable(2);
  }
  se_ = proactor->GetSubmitEntry(std::move(waker));

  if (timeout_msec != UINT32_MAX) {
    se_.sqe()->flags |= IOSQE_IO_LINK;

    tm_ = proactor->GetSubmitEntry(nullptr, kTimeoutSubmitTag);

    // We must keep ts_ as member function so it could be accessed after this function scope.
    ts_.tv_sec = (timeout_msec / 1000);
    ts_.tv_nsec = (timeout_msec % 1000) * 1000000;
    tm_.PrepLinkTimeout(&ts_);  // relative timeout.
  }
}

FiberCall::~FiberCall() {
  CHECK(!me_) << "Get was not called!";
}

}  // namespace fb2
}  // namespace util

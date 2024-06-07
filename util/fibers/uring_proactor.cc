// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/uring_proactor.h"

#include <absl/base/attributes.h>
#include <liburing.h>
#include <poll.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>

#include "base/flags.h"
#include "base/histogram.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "util/fibers/detail/scheduler.h"
#include "util/fibers/uring_socket.h"

// TODO: we need to fix register_fds_ resize flow.
// Also we must ensure that there is no leakage of socket descriptors with enable_direct_fd enabled.
// See AcceptServerTest.Shutdown to trigger direct fd resize.
ABSL_FLAG(bool, enable_direct_fd, false, "If true tries to register file descriptors");

#define URING_CHECK(x)                                                        \
  do {                                                                        \
    int __res_val = (x);                                                      \
    if (ABSL_PREDICT_FALSE(__res_val < 0)) {                                  \
      LOG(FATAL) << "Error " << (-__res_val)                                  \
                 << " evaluating '" #x "': " << SafeErrorMessage(-__res_val); \
    }                                                                         \
  } while (false)

#define VPRO(verbosity) VLOG(verbosity) << "PRO[" << GetPoolIndex() << "] "

using namespace std;

namespace util {

using detail::SafeErrorMessage;

namespace fb2 {

static_assert(sizeof(FiberCall) == 48);

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
constexpr uint16_t kCqeBatchLen = 128;

constexpr size_t kBufRingEntriesCnt = 8192;
constexpr size_t kBufRingEntrySize = 64;  // TODO: should be configurable.

}  // namespace

UringProactor::UringProactor() : ProactorBase() {
}

UringProactor::~UringProactor() {
  CHECK(is_stopped_);
  if (thread_id_ != -1U) {
    if (buf_pool_.backing) {
      munmap(buf_pool_.backing, buf_pool_.segments.Size() * UringBuf::kAlign);
      io_uring_unregister_buffers(&ring_);
    }

    for (size_t i = 0; i < bufring_groups_.size(); ++i) {
      const auto& group = bufring_groups_[i];
      if (group.ring != nullptr) {
        io_uring_free_buf_ring(&ring_, group.ring, kBufRingEntriesCnt, i);
        delete group.buf;
      }
    }

    if (direct_fd_) {
      io_uring_unregister_files(&ring_);
    }
    io_uring_queue_exit(&ring_);
  }
  VLOG(1) << "Closing wake_fd " << wake_fd_ << " ring fd: " << ring_.ring_fd;
}

void UringProactor::Init(unsigned pool_index, size_t ring_size, int wq_fd) {
  CHECK_EQ(0U, ring_size & (ring_size - 1));
  CHECK_GE(ring_size, 8U);
  CHECK_EQ(0U, thread_id_) << "Init was already called";

  pool_index_ = pool_index;

  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  CHECK(kver.kernel > 5 || (kver.kernel == 5 && kver.major >= 8))
      << "Versions 5.8 or higher are supported";

  io_uring_params params;
  memset(&params, 0, sizeof(params));

  msgring_f_ = 0;
  poll_first_ = 0;
  direct_fd_ = 0;
  buf_ring_f_ = 0;

  if (kver.kernel > 5 || (kver.kernel == 5 && kver.major >= 15)) {
    direct_fd_ = absl::GetFlag(FLAGS_enable_direct_fd);  // failswitch to disable direct fds.
  }

  // If we setup flags that kernel does not recognize, it fails the setup call.
  if (kver.kernel > 5 || (kver.kernel == 5 && kver.major >= 19)) {
    params.flags |= IORING_SETUP_SUBMIT_ALL;
    // we can notify kernel that it can skip send/receive operations and do polling first.
    poll_first_ = 1;

    // io_uring_register_buf_ring is supported since 5.19.
    buf_ring_f_ = 1;
  }

  if (kver.kernel >= 6 && kver.major >= 1) {
    // This has a positive effect on CPU usage, latency and throughput.
    params.flags |=
        (IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_TASKRUN_FLAG | IORING_SETUP_SINGLE_ISSUER);
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

  io_uring_probe* uring_probe = io_uring_get_probe_ring(&ring_);

  msgring_f_ = io_uring_opcode_supported(uring_probe, IORING_OP_MSG_RING);
  io_uring_free_probe(uring_probe);
  VLOG_IF(1, msgring_f_) << "msgring supported!";

  unsigned req_feats = IORING_FEAT_SINGLE_MMAP | IORING_FEAT_FAST_POLL | IORING_FEAT_NODROP;
  CHECK_EQ(req_feats, params.features & req_feats)
      << "required feature feature is not present in the kernel";

  int res = io_uring_register_ring_fd(&ring_);
  VLOG_IF(1, res < 0) << "io_uring_register_ring_fd failed: " << -res;

  if (direct_fd_) {
    register_fds_.resize(512, -1);
    int res = io_uring_register_files(&ring_, register_fds_.data(), register_fds_.size());
    CHECK_EQ(0, res);
  }

  size_t sz = ring_.sq.ring_sz + params.sq_entries * sizeof(struct io_uring_sqe);
  LOG_FIRST_N(INFO, 1) << "IORing with " << params.sq_entries << " entries, allocated " << sz
                       << " bytes, cq_entries is " << *ring_.cq.kring_entries;

  ArmWakeupEvent();
  centries_.resize(params.sq_entries);  // .val = -1
  next_free_ce_ = 0;
  for (size_t i = 0; i < centries_.size() - 1; ++i) {
    centries_[i].index = i + 1;
  }

  thread_id_ = pthread_self();
  sys_thread_id_ = syscall(SYS_gettid);

  tl_info_.owner = this;
}

void UringProactor::ProcessCqeBatch(unsigned count, io_uring_cqe** cqes,
                                    detail::FiberInterface* current) {
  for (unsigned i = 0; i < count; ++i) {
    // copy cqe (16 bytes) because it helps when debugging, gdb can not access memory in kernel
    // space.
    io_uring_cqe cqe = *cqes[i];

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

      if (cqe.flags & IORING_CQE_F_MORE) {
        // multishot operation. we keep the callback intact.
        e.cb(current, cqe.res, cqe.flags);
      } else {
        CbType func = std::move(e.cb);

        // Set e to be the head of free-list.
        e.index = next_free_ce_;
        next_free_ce_ = index;
        --pending_cb_cnt_;
        func(current, cqe.res, cqe.flags);
      }
      continue;
    }

    // We ignore ECANCELED because submissions with link_timeout that finish successfully generate
    // CQE with ECANCELED for the subsequent linked submission. See io_uring_enter(2) for more info.
    // ETIME is when a timer cqe fully completes.
    if (cqe.res < 0 && cqe.res != -ECANCELED && cqe.res != -ETIME) {
      LOG(WARNING) << "CQE error: " << -cqe.res << " cqe_type=" << (cqe.user_data >> 32);
    }

    if (user_data == kIgnoreIndex)
      continue;

    if (user_data == kWakeIndex) {
      // Path relevant only for older kernels. For new kernels we use MSG_RING.
      // We were woken up. Need to rearm wakeup poller.
      DCHECK_EQ(cqe.res, 8);
      DVLOG(2) << "PRO[" << GetPoolIndex() << "] Wakeup " << cqe.res << "/" << cqe.flags;

      ArmWakeupEvent();
      continue;
    }
    LOG(ERROR) << "Unrecognized user_data " << cqe.user_data;
  }
}

void UringProactor::ReapCompletions(unsigned init_count, io_uring_cqe** cqes,
                                    detail::FiberInterface* current) {
  unsigned batch_count = init_count;
  while (batch_count > 0) {
    ProcessCqeBatch(batch_count, cqes, current);
    io_uring_cq_advance(&ring_, batch_count);
    reaped_cqe_cnt_ += batch_count;
    if (batch_count < kCqeBatchLen)
      break;
    batch_count = io_uring_peek_batch_cqe(&ring_, cqes, kCqeBatchLen);
  }

  // In case some of the timer completions filled schedule_periodic_list_.
  for (auto& task_pair : schedule_periodic_list_) {
    SchedulePeriodic(task_pair.first, task_pair.second);
  }
  schedule_periodic_list_.clear();
  sqe_avail_.notifyAll();
}

SubmitEntry UringProactor::GetSubmitEntry(CbType cb, uint16_t submit_tag) {
  io_uring_sqe* res = io_uring_get_sqe(&ring_);
  if (res == NULL) {
    ++get_entry_sq_full_;
    int submitted = io_uring_submit(&ring_);
    if (submitted > 0) {
      res = io_uring_get_sqe(&ring_);
    } else {
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

int UringProactor::RegisterBuffers(size_t size) {
  size = (size + UringBuf::kAlign - 1) / UringBuf::kAlign * UringBuf::kAlign;

  // Use mmap to create the backing
  void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED)
    return -errno;

  buf_pool_.backing = reinterpret_cast<uint8_t*>(ptr);
  buf_pool_.segments.Grow(size / UringBuf::kAlign);

  iovec vec{buf_pool_.backing, size};
  return io_uring_register_buffers(&ring_, &vec, 1);
}

std::optional<UringBuf> UringProactor::RequestBuffer(size_t size) {
  DCHECK(buf_pool_.backing);
  // We keep track not of bytes, but 4kb segments and round up
  size_t segments = (size + UringBuf::kAlign - 1) / UringBuf::kAlign;
  if (auto offset = buf_pool_.segments.Request(segments)) {
    uint8_t* ptr = buf_pool_.backing + *offset * UringBuf::kAlign;
    return UringBuf{{ptr, segments * UringBuf::kAlign}, 0};
  }
  return std::nullopt;
}

void UringProactor::ReturnBuffer(UringBuf buf) {
  DCHECK(buf.buf_idx);

  size_t segments = (buf.bytes.data() - buf_pool_.backing) / UringBuf::kAlign;
  buf_pool_.segments.Return(segments);
}

int UringProactor::RegisterBufferRing(unsigned group_id) {
  if (buf_ring_f_ == 0)
    return EOPNOTSUPP;

  if (bufring_groups_.size() <= group_id) {
    bufring_groups_.resize(group_id + 1);
  }

  auto& ring_group = bufring_groups_[group_id];
  CHECK(ring_group.ring == nullptr);

  int err = 0;

  ring_group.ring = io_uring_setup_buf_ring(&ring_, kBufRingEntriesCnt, group_id, 0, &err);
  if (ring_group.ring == nullptr) {
    return -err;  // err is negative.
  }

  unsigned mask = kBufRingEntriesCnt - 1;
  ring_group.buf = new uint8_t[kBufRingEntriesCnt * kBufRingEntrySize];
  uint8_t* next = ring_group.buf;
  for (unsigned i = 0; i < kBufRingEntriesCnt; ++i) {
    io_uring_buf_ring_add(ring_group.ring, next, kBufRingEntrySize, i, mask, i);
    next += 64;
  }
  io_uring_buf_ring_advance(ring_group.ring, kBufRingEntriesCnt);

  return 0;
}

uint8_t* UringProactor::GetBufRingPtr(unsigned group_id, unsigned bufid) {
  DCHECK_LT(group_id, bufring_groups_.size());
  DCHECK_LT(bufid, kBufRingEntriesCnt);
  DCHECK(bufring_groups_[group_id].buf);
  return bufring_groups_[group_id].buf + bufid * kBufRingEntrySize;
}

void UringProactor::ConsumeBufRing(unsigned group_id, unsigned len) {
  DCHECK_LT(group_id, bufring_groups_.size());
  DCHECK_LE(len, kBufRingEntriesCnt);
  DCHECK(bufring_groups_[group_id].ring);

  io_uring_buf_ring_advance(bufring_groups_[group_id].ring, len);
}

int UringProactor::CancelRequests(int fd, unsigned flags) {
  io_uring_sync_cancel_reg reg_arg;
  memset(&reg_arg, 0, sizeof(reg_arg));
  reg_arg.timeout.tv_nsec = -1;
  reg_arg.timeout.tv_sec = -1;
  reg_arg.flags = flags;
  reg_arg.fd = fd;

  return io_uring_register_sync_cancel(&ring_, &reg_arg);
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

  io_uring_prep_poll_add(sqe, wake_fd_, POLLIN);
  uint8_t flag = 0;
  sqe->user_data = kIgnoreIndex;
  sqe->flags |= (flag | IOSQE_IO_LINK);
  sqe = io_uring_get_sqe(&ring_);

  // drain the signal.
  // we do not have to use threadlocal but we use it for performance reasons
  // to reduce cache thrashing.
  static thread_local uint64_t donot_care;
  io_uring_prep_read(sqe, wake_fd_, &donot_care, 8, 0);
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
  item->ref_cnt = 2;  // one for the map and one for the callback.
  item->val1 = se.sqe()->user_data;
}

void UringProactor::PeriodicCb(IoResult res, uint32 task_id, PeriodicItem* item) {
  if (item->ref_cnt <= 1) {  // has been removed from the map.
    delete item;
    return;
  }

  // -ECANCELED can happen only if in_map is false and it's handled above.
  CHECK_EQ(res, -ETIME);

  DCHECK(periodic_map_.find(task_id) != periodic_map_.end());
  DCHECK(item->task);
  item->task();

  schedule_periodic_list_.emplace_back(task_id, item);
}

void UringProactor::CancelPeriodicInternal(PeriodicItem* item) {
  auto* me = detail::FiberActive();
  auto cb = [me](detail::FiberInterface* current, IoResult res, uint32_t flags) {
    ActivateSameThread(current, me);
  };
  uint32_t val1 = item->val1;
  SubmitEntry se = GetSubmitEntry(std::move(cb));

  DVLOG(1) << "Cancel timer " << val1 << ", cb userdata: " << se.sqe()->user_data;
  se.PrepTimeoutRemove(val1);  // cancel using userdata id sent to io-uring.
  me->Suspend();
}

unsigned UringProactor::RegisterFd(int source_fd) {
  if (!direct_fd_)
    return kInvalidDirectFd;

  // TODO: to create a linked list from free fds.
  auto next = std::find(register_fds_.begin() + next_free_index_, register_fds_.end(), -1);
  if (next == register_fds_.end()) {
    size_t prev_sz = register_fds_.size();
    DCHECK_GT(prev_sz, 0u);

    // enlarge direct fds table.
    register_fds_.resize(prev_sz * 2, -1);
    register_fds_[prev_sz] = source_fd;  // source fd will map to prev_sz index.
    next_free_index_ = prev_sz + 1;

    // TODO: this does not work because it seems we need to unregister first
    // to be able re-register. See
    int res = io_uring_register_files(&ring_, register_fds_.data(), register_fds_.size());
    if (res < 0) {
      LOG(ERROR) << "Error registering files: " << -res << " " << SafeErrorMessage(-res) << " "
                 << prev_sz;
      register_fds_.resize(prev_sz);
      next_free_index_ = prev_sz;
      return kInvalidDirectFd;
    }
    ++direct_fds_cnt_;

    return prev_sz;
  }

  *next = source_fd;
  next_free_index_ = next - register_fds_.begin();
  int res = io_uring_register_files_update(&ring_, next_free_index_, &source_fd, 1);
  if (res < 0) {
    LOG(ERROR) << "Error updating direct fds: " << -res << " " << SafeErrorMessage(-res);
    return kInvalidDirectFd;
  }
  ++next_free_index_;
  ++direct_fds_cnt_;

  return next_free_index_ - 1;
}

int UringProactor::TranslateDirectFd(unsigned fixed_fd) const {
  DCHECK(direct_fd_);
  DCHECK_LT(fixed_fd, register_fds_.size());
  DCHECK_GE(register_fds_[fixed_fd], 0);

  return register_fds_[fixed_fd];
}

int UringProactor::UnregisterFd(unsigned fixed_fd) {
  DCHECK(direct_fd_);

  if (!direct_fd_)
    return -1;

  DCHECK_LT(fixed_fd, register_fds_.size());

  int fd = register_fds_[fixed_fd];
  DCHECK_GE(fd, 0);

  register_fds_[fixed_fd] = -1;

  int res = io_uring_register_files_update(&ring_, fixed_fd, &register_fds_[fixed_fd], 1);
  if (res <= 0) {
    LOG(FATAL) << "Error updating direct fds: " << -res << " " << SafeErrorMessage(-res);
  }
  --direct_fds_cnt_;
  if (fixed_fd < next_free_index_) {
    next_free_index_ = fixed_fd;
  }
  return fd;
}

LinuxSocketBase* UringProactor::CreateSocket() {
  return new UringSocket{-1, this};
}

void UringProactor::MainLoop(detail::Scheduler* scheduler) {
  struct io_uring_cqe* cqes[kCqeBatchLen];

  uint32_t tq_seq = 0;
  uint32_t spin_loops = 0;
  uint32_t busy_sq_cnt = 0;
  Tasklet task;

  FiberInterface* dispatcher = detail::FiberActive();

  // The loop must follow these rules:
  // 1. if we task-queue is not empty or if we have ready fibers, then we should
  //    not stall in wait_for_cqe(.., 1, ...).
  //
  //
  // 2. Specifically, yielding fibers will cause that scheduler->HasReady() will return true.
  //    We still should not stall in wait_for_cqe if we have yielding fibers but we also
  //    should give a chance to reap completions.
  // 3. we should batch reaping of cqes if possible to improve performance of the IRQ handling.
  // 4. ProcessSleep does not have to be called every loop cycle since it does not really
  //    expect usec precision.
  // 6. ProcessSleep and ProcessRemoteReady may introduce ready fibers.

  while (true) {
    ++stats_.loop_cnt;
    bool has_cpu_work = false;

    // io_uring_submit should be more performant in some case than io_uring_submit_and_get_events
    // because when there no sqes to flush io_submit may save
    // a syscall, while io_uring_submit_and_get_events will always do a syscall.
    // Unfortunately I did not see the impact of it.
    // Another observation:
    // AvgCqe/ReapCall goes up if we call here io_uring_submit_and_get_events.
    int num_submitted = io_uring_submit_and_get_events(&ring_);
    bool ring_busy = false;

    if (num_submitted >= 0) {
      stats_.uring_submit_calls += (num_submitted != 0);
      if (num_submitted) {
        DVLOG(3) << "Submitted " << num_submitted;
      }
    } else if (num_submitted == -EBUSY) {
      VLOG(1) << "EBUSY " << io_uring_sq_ready(&ring_);
      ring_busy = true;
      num_submitted = 0;
      ++busy_sq_cnt;
    } else {
      URING_CHECK(num_submitted);
    }

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
        ++stats_.num_task_runs;
        ++cnt;
        tl_info_.monotonic_time = GetClockNanos();
        if (task_start + 500000 < tl_info_.monotonic_time) {  // Break after 500usec
          ++stats_.task_interrupts;
          has_cpu_work = true;
          break;
        }
      } while (task_queue_.try_dequeue(task));
      stats_.num_task_runs += cnt;
      DVLOG(2) << "Tasks runs " << stats_.num_task_runs << "/" << spin_loops;

      // We notify second time to avoid deadlocks.
      // Without it ProactorTest.AsyncCall blocks.
      task_queue_avail_.notifyAll();
    }

    scheduler->ProcessRemoteReady(nullptr);

    // Traverses one or more fibers because a worker fiber does not necessarily returns
    // straight back to the dispatcher. Instead it chooses the next ready worker fiber
    // from the ready queue.
    //
    // We can not iterate in while loop here because fibers that yield will make the loop
    // never ending.
    if (scheduler->HasReady()) {
      FiberInterface* fi = scheduler->PopReady();
      DCHECK(!fi->list_hook.is_linked());
      DCHECK(!fi->sleep_hook.is_linked());
      scheduler->AddReady(dispatcher);

      DVLOG(2) << "Switching to " << fi->name();
      tl_info_.monotonic_time = GetClockNanos();
      fi->SwitchTo();

      if (scheduler->HasReady()) {
        has_cpu_work = true;
      } else {
        // all our ready fibers have been processed. Lets try to submit more sqes.
        continue;
      }
    }

    uint32_t cqe_count = io_uring_peek_batch_cqe(&ring_, cqes, kCqeBatchLen);
    if (cqe_count) {
      ++stats_.completions_fetches;

      // cqe tail (ring->cq.ktail) can be updated asynchronously by the kernel even if we
      // do now execute any syscalls. Therefore we count how many completions we handled
      // and reap the same amount.
      ReapCompletions(cqe_count, cqes, dispatcher);
      continue;
    }

    if (has_cpu_work || io_uring_sq_ready(&ring_) > 0) {
      continue;
    }

    ///
    /// End of the tight loop that processes tasks, ready fibers, and submits sqes.
    ///
    if (scheduler->HasSleepingFibers()) {
      unsigned activated = ProcessSleepFibers(scheduler);
      if (activated > 0) {  // If we have ready fibers - restart the loop.
        continue;
      }
    }

    DCHECK(!scheduler->HasReady());
    DCHECK_EQ(io_uring_sq_ready(&ring_), 0u);

    // DCHECK_EQ(io_uring_cq_ready(&ring_), 0u) does not hold because completions
    // can be updated asynchronously by the kernel (unless IORING_SETUP_DEFER_TASKRUN is set).

    bool should_spin = RunOnIdleTasks();
    if (should_spin) {
      continue;  // continue spinning until on_idle_map_ is empty.
    }

    // Lets spin a bit to make a system a bit more responsive.
    // Important to spin a bit, otherwise we put too much pressure on  eventfd_write.
    // and we enter too often into kernel space.
    if (!ring_busy && spin_loops++ < 10) {
      DVLOG(3) << "spin_loops " << spin_loops;

      // We should not spin too much using sched_yield or it burns a fuckload of cpu.
      scheduler->DestroyTerminated();

      continue;
    }

    spin_loops = 0;  // Reset the spinning.

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

    /**
     * If tq_seq_ has changed since it was cached into tq_seq, then
     * EmplaceTaskQueue succeeded and we might have more tasks to execute - lets
     * run the loop again. Otherwise, set tq_seq_ to WAIT_SECTION_STATE, hinting that
     * we are going to stall now. Other threads will need to wake-up the ring
     * (see WakeRing()) but only one will actually call the syscall.
     */
    if (task_queue_.empty() &&
        tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_acquire)) {
      if (is_stopped_) {
        tq_seq_.store(0, std::memory_order_release);  // clear WAIT section
        break;
      }

      DCHECK(!scheduler->HasReady());

      VPRO(2) << "wait_for_cqe " << stats_.loop_cnt;

      wait_for_cqe(&ring_, 1, ts_arg);
      VPRO(2) << "Woke up after wait_for_cqe ";

      ++stats_.num_stalls;
      tq_seq = 0;
      tq_seq_.store(0, std::memory_order_release);
    }
  }

  VPRO(1) << "total/stalls/cqe_fetches/num_submits: " << stats_.loop_cnt << "/" << stats_.num_stalls
          << "/" << stats_.completions_fetches << "/" << stats_.uring_submit_calls;
  if (stats_.completions_fetches > 0)
    VPRO(1) << "AvgCqe/ReapCall: " << double(reaped_cqe_cnt_) / stats_.completions_fetches;
  VPRO(1) << "tq_wakeups/tq_wakeup_saved/tq_full/tq_task_int: " << tq_wakeup_ev_.load() << "/"
          << tq_wakeup_skipped_ev_.load() << "/" << tq_full_ev_.load() << "/"
          << stats_.task_interrupts;
  VPRO(1) << "busy_sq/get_entry_sq_full/get_entry_sq_err/get_entry_awaits/pending_callbacks: "
          << busy_sq_cnt << "/" << get_entry_sq_full_ << "/" << get_entry_await_ << "/"
          << pending_cb_cnt_;

  VPRO(1) << "centries size: " << centries_.size();
  centries_.clear();
  DCHECK_EQ(0u, direct_fds_cnt_);
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
    was_run_ = true;
    DCHECK(me_) << io_res_ << " " << res_flags_;
    ActivateSameThread(current, me_);
  };

  if (timeout_msec != UINT32_MAX) {
    proactor->WaitTillAvailable(2);
  }
  se_ = proactor->GetSubmitEntry(std::move(waker));

  if (timeout_msec != UINT32_MAX) {
    se_.sqe()->flags |= IOSQE_IO_LINK;

    SubmitEntry tm = proactor->GetSubmitEntry(nullptr, kTimeoutSubmitTag);

    // We must keep ts_ as member function so it could be accessed after this function scope.
    ts_.tv_sec = (timeout_msec / 1000);
    ts_.tv_nsec = (timeout_msec % 1000) * 1000000;
    tm.PrepLinkTimeout(&ts_);  // relative timeout.
  }
}

FiberCall::~FiberCall() {
  CHECK(!me_) << "Get was not called!";
}

auto FiberCall::Get() -> IoResult {
  // In most cases our fibers wait on exactly one io_uring event, specifically the one that
  // was issued by this fiber call. However, it is possible and in fact in some cases we do that
  // that we issue asynchronously an io_uring request A (could be any call supported by io_uring)
  // and then issue another request B via FiberCall. Once completion A is being fullfilled, it may
  // wake this suspended fiber via its completion callback. In that case this fiber will wake up
  // even though the waker of this call was not run yet.
  // To avoid this, we suspend until we make sure our own waker runs.
  do {
    me_->Suspend();
    LOG_IF(DFATAL, !was_run_) << "Woken up by the wrong notifier";
  } while (!was_run_);
  me_ = nullptr;

  return io_res_;
}

}  // namespace fb2
}  // namespace util

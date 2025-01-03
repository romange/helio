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

#include <bit>

#include "base/flags.h"
#include "base/histogram.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "util/fibers/detail/scheduler.h"
#include "util/fibers/uring_socket.h"

// We must ensure that there is no leakage of socket descriptors with enable_direct_fd enabled.
// See AcceptServerTest.Shutdown to trigger direct fd resize.
ABSL_FLAG(uint32_t, uring_direct_table_len, 0, "If positive create direct fd table of this length");

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
        io_uring_free_buf_ring(&ring_, group.ring, 1U << group.nentries_exp, i);
        delete[] group.buf;
        delete[] group.multishot_arr;
      }
    }

    if (!register_fds_.empty()) {
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
  buf_ring_f_ = 0;
  bundle_f_ = 0;

  // If we setup flags that kernel does not recognize, it fails the setup call.
  if (kver.kernel > 5 || (kver.kernel == 5 && kver.major >= 19)) {
    params.flags |= IORING_SETUP_SUBMIT_ALL;
    // we can notify kernel that it can skip send/receive operations and do polling first.
    poll_first_ = 1;

    // io_uring_register_buf_ring is supported since 5.19.
    buf_ring_f_ = 1;

    // FLAGS_uring_direct_table_len is a failswitch to disable direct fds.
    register_fds_.resize(absl::GetFlag(FLAGS_uring_direct_table_len), -1);
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

#ifdef IORING_FEAT_RECVSEND_BUNDLE
  if (params.features & IORING_FEAT_RECVSEND_BUNDLE) {
    bundle_f_ = 1;
  }
#endif

  int res = io_uring_register_ring_fd(&ring_);
  VLOG_IF(1, res < 0) << "io_uring_register_ring_fd failed: " << -res;

  if (!register_fds_.empty()) {
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
    uint32_t user_tag = cqe.user_data >> 32;
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
        e.cb(current, cqe.res, cqe.flags, user_tag);
      } else {
        CbType func = std::move(e.cb);

        // Set e to be the head of free-list.
        e.index = next_free_ce_;
        next_free_ce_ = index;
        --pending_cb_cnt_;
        func(current, cqe.res, cqe.flags, user_tag);
      }
      continue;
    }

    // We ignore ECANCELED because submissions with link_timeout that finish successfully generate
    // CQE with ECANCELED for the subsequent linked submission. See io_uring_enter(2) for more info.
    // ETIME is when a timer cqe fully completes.
    if (cqe.res < 0 && cqe.res != -ECANCELED && cqe.res != -ETIME) {
      LOG(WARNING) << "CQE error: " << -cqe.res << " cqe_type=" << user_tag;
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

SubmitEntry UringProactor::GetSubmitEntry(CbType cb, uint32_t submit_tag) {
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

unsigned UringProactor::RegisterBuffers(size_t size) {
  size = (size + UringBuf::kAlign - 1) / UringBuf::kAlign * UringBuf::kAlign;

  // Use mmap to create the backing
  void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) {
    LOG(ERROR) << "Could not mmap " << size << " bytes";
    return errno;
  }

  iovec vec{ptr, size};
  int res = io_uring_register_buffers(&ring_, &vec, 1);
  if (res < 0) {
    LOG(ERROR) << "Error calling io_uring_register_buffers: " << SafeErrorMessage(-res);
    munmap(ptr, size);
    return -res;
  }

  buf_pool_.backing = reinterpret_cast<uint8_t*>(ptr);
  buf_pool_.segments.Grow(size / UringBuf::kAlign);

  return 0;
}

std::optional<UringBuf> UringProactor::RequestBuffer(size_t size) {
  if (buf_pool_.backing) {
    // We keep track not of bytes, but 4kb segments and round up
    size_t segment_cnt = (size + UringBuf::kAlign - 1) / UringBuf::kAlign;
    if (auto offset = buf_pool_.segments.Request(segment_cnt)) {
      uint8_t* ptr = buf_pool_.backing + *offset * UringBuf::kAlign;
      return UringBuf{{ptr, segment_cnt * UringBuf::kAlign}, 0};
    }
  }
  return std::nullopt;
}

void UringProactor::ReturnBuffer(UringBuf buf) {
  DCHECK(buf.buf_idx);

  size_t segments = (buf.bytes.data() - buf_pool_.backing) / UringBuf::kAlign;
  buf_pool_.segments.Return(segments);
}

int UringProactor::RegisterBufferRing(uint16_t group_id, uint16_t nentries, unsigned esize) {
  CHECK_LT(nentries, 32768u);
  CHECK_EQ(0, nentries & (nentries - 1));  // power of 2.
  DCHECK(InMyThread());

  if (buf_ring_f_ == 0)
    return EOPNOTSUPP;

  if (bufring_groups_.size() <= group_id) {
    bufring_groups_.resize(group_id + 1);
  }

  auto& buf_group = bufring_groups_[group_id];
  CHECK(buf_group.ring == nullptr);

  int err = 0;

  buf_group.ring = io_uring_setup_buf_ring(&ring_, nentries, group_id, 0, &err);

  if (buf_group.ring == nullptr) {
    return -err;  // err is negative.
  }

  unsigned mask = io_uring_buf_ring_mask(nentries);
  buf_group.buf = new uint8_t[size_t(nentries) * esize];

  buf_group.nentries_exp = absl::bit_width(nentries) - 1;
  buf_group.entry_size = esize;
  uint8_t* next = buf_group.buf;

  // buffers are ordered nicely at first, in sequential order inside a single range
  // but when we return them back to bufring, then will be reordered because
  // CQEs complete in arbitrary order, moreover the ownership over buffers is passed back
  // to bufring in arbitrary order inside ConsumeBufRing.
  for (unsigned i = 0; i < nentries; ++i) {
    io_uring_buf_ring_add(buf_group.ring, next, esize, i, mask, i);
    next += esize;
  }

  // return the ownership to the ring.
  io_uring_buf_ring_advance(buf_group.ring, nentries);

  return 0;
}

uint8_t* UringProactor::GetBufRingPtr(uint16_t group_id, uint16_t bufid) {
  DCHECK_LT(group_id, bufring_groups_.size());
  auto& buf_group = bufring_groups_[group_id];

  DCHECK_LT(bufid, 1 << buf_group.nentries_exp);
  DCHECK(bufring_groups_[group_id].buf);
  return bufring_groups_[group_id].buf + size_t(bufid) * buf_group.entry_size;
}

void UringProactor::ReplenishBuffers(uint16_t group_id, io::Bytes slice) {
  DCHECK_LT(group_id, bufring_groups_.size());
  DCHECK(!slice.empty());

  auto& buf_group = bufring_groups_[group_id];
  unsigned nentries = 1U << buf_group.nentries_exp;
  size_t total_len = size_t(nentries) * size_t(buf_group.entry_size);
  DCHECK(slice.end() <= buf_group.buf + total_len);
  off_t offs = slice.data() - buf_group.buf;
  DCHECK_GE(offs, 0);
  DCHECK(offs % buf_group.entry_size == 0);

  // Add 1 or more buffers back to the ring. ReplenishBuffers calls can come OOO, therefore
  // we expect to see reshuffling of buffers within the ring.
  unsigned bid = offs / buf_group.entry_size;
  size_t replenished = 0;
  uint8_t* cur_buf = buf_group.buf + bid * buf_group.entry_size;
  unsigned mask = io_uring_buf_ring_mask(nentries);
  unsigned offset = 0;
  while (replenished < slice.size()) {
    io_uring_buf_ring_add(buf_group.ring, cur_buf, buf_group.entry_size, bid, mask, offset++);
    replenished += buf_group.entry_size;
  }

  io_uring_buf_ring_advance(bufring_groups_[group_id].ring, offset);
}

int UringProactor::BufRingAvailable(unsigned group_id) const {
  DCHECK_LT(group_id, bufring_groups_.size());
  auto& buf_group = bufring_groups_[group_id];

  int res = io_uring_buf_ring_available(const_cast<io_uring*>(&ring_), buf_group.ring, group_id);
  return res;
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

void UringProactor::EnableMultiShot(uint16_t group_id) {
  DCHECK_LT(group_id, bufring_groups_.size());
  auto& buf_group = bufring_groups_[group_id];
  DCHECK(!buf_group.multishot_arr);
  buf_group.multishot_exp = buf_group.nentries_exp;

  size_t shot_len = 1U << buf_group.multishot_exp;
  buf_group.multishot_arr = new MultiShot[shot_len];
  buf_group.free_multi_shot_id = 0;

  for (uint16_t i = 0; i < shot_len; ++i) {
    buf_group.multishot_arr[i].next = i + 1;
  }
  buf_group.multishot_arr[shot_len - 1].next = kMultiShotUndef;
}

void UringProactor::EnqueueMultishotCompletion(uint16_t group_id, IoResult res, uint32_t flags,
                                               uint16_t* tail) {
  DCHECK_LT(group_id, bufring_groups_.size());
  DCHECK(flags & IORING_CQE_F_BUFFER) << res;
  CHECK_GT(res, 0) << flags;

  auto& buf_group = bufring_groups_[group_id];
  if (!buf_group.multishot_arr) {
    EnableMultiShot(group_id);
  }

  auto next = buf_group.free_multi_shot_id;
  DCHECK_LT(next, 1 << buf_group.nentries_exp) << flags;

  auto& entry = buf_group.multishot_arr[next];
  buf_group.free_multi_shot_id = entry.next;

  entry.bid = flags >> IORING_CQE_BUFFER_SHIFT;
  entry.res = res;
  uint16_t tail_id = *tail;

  // Add it to the tail of the ring buffer.
  if (tail_id == kMultiShotUndef) {
    entry.next = next;  // circular list with one element.
    entry.prev = next;
  } else {
    auto& tail_entry = buf_group.multishot_arr[tail_id];
    uint16_t head_id = tail_entry.next;
    entry.next = head_id;
    entry.prev = tail_id;

    // update the head.
    tail_entry.next = next;
    buf_group.multishot_arr[head_id].prev = next;
  }

  *tail = next;
}

auto UringProactor::PullMultiShotCompletion(uint16_t group_id, uint16_t* tail) -> MultiShotResult {
  DCHECK_LT(group_id, bufring_groups_.size());
  auto& buf_group = bufring_groups_[group_id];

  uint16_t tail_id = *tail;
  DCHECK_NE(tail_id, kMultiShotUndef);

  auto& tail_entry = buf_group.multishot_arr[tail_id];

  // We remove head from the ring buffer.
  uint16_t head_id = tail_entry.next;

  auto& head_entry = buf_group.multishot_arr[head_id];
  if (head_id == tail_id) {
    *tail = kMultiShotUndef;
  } else {
    tail_entry.next = head_entry.next;
    buf_group.multishot_arr[head_entry.next].prev = tail_id;
  }

  // link the entry to the free list.
  head_entry.next = buf_group.free_multi_shot_id;
  buf_group.free_multi_shot_id = head_id;
  CHECK_GT(head_entry.res, 0);
  uint8_t* buf = buf_group.buf + head_entry.bid * buf_group.entry_size;
  return MultiShotResult{buf, size_t(head_entry.res)};
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
      GetSubmitEntry([this, id, item](detail::FiberInterface*, IoResult res, uint32_t flags,
                                      uint32_t) { this->PeriodicCb(res, id, std::move(item)); });

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
  auto cb = [me](detail::FiberInterface* current, IoResult res, uint32_t flags, uint32_t) {
    ActivateSameThread(current, me);
  };
  uint32_t val1 = item->val1;
  SubmitEntry se = GetSubmitEntry(std::move(cb));

  DVLOG(1) << "Cancel timer " << val1 << ", cb userdata: " << se.sqe()->user_data;
  se.PrepTimeoutRemove(val1);  // cancel using userdata id sent to io-uring.
  me->Suspend();
}

unsigned UringProactor::RegisterFd(int source_fd) {
  if (register_fds_.empty())
    return kInvalidDirectFd;

  // TODO: to create a linked list from free fds.
  auto next = std::find(register_fds_.begin() + next_free_index_, register_fds_.end(), -1);
  if (next == register_fds_.end())  // it is not possible to resize this table.
    return kInvalidDirectFd;

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
  DCHECK_LT(fixed_fd, register_fds_.size());
  DCHECK_GE(register_fds_[fixed_fd], 0);

  return register_fds_[fixed_fd];
}

int UringProactor::UnregisterFd(unsigned fixed_fd) {
  DCHECK(!register_fds_.empty());

  if (register_fds_.empty())
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
  enum {
    JUMP_FROM_INIT,
    JUMP_FROM_READY,
    JUMP_FROM_L2,
    JUMP_FROM_SPIN,
    JUMP_FROM_TOTAL
  } jump_from = JUMP_FROM_INIT;
  unsigned jump_counts[JUMP_FROM_TOTAL] = {0};

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

    if (num_submitted > 0) {
      ++stats_.uring_submit_calls;
      DVLOG(3) << "Submitted " << num_submitted;
    } else if (num_submitted == -EBUSY) {
      VLOG(1) << "EBUSY " << io_uring_sq_ready(&ring_);
      ring_busy = true;
      num_submitted = 0;
      ++busy_sq_cnt;
    } else if (num_submitted == 0) {
      jump_counts[jump_from]++;
    } else if (num_submitted != -ETIME) {
      LOG(DFATAL) << "Error submitting to iouring: " << -num_submitted;
      continue;
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

    uint32_t cqe_count = io_uring_peek_batch_cqe(&ring_, cqes, kCqeBatchLen);
    if (cqe_count) {
      ++stats_.completions_fetches;

      // cqe tail (ring->cq.ktail) can be updated asynchronously by the kernel even if we
      // do now execute any syscalls. Therefore we count how many completions we handled
      // and reap the same amount.
      ReapCompletions(cqe_count, cqes, dispatcher);

      if (ShouldPollL2Tasks()) {
        RunL2Tasks(scheduler);
      }
    }

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
        // all our ready fibers have been processed. Lets try to submit more sqes.
        jump_from = JUMP_FROM_READY;
        continue;
      }
    }

    if (has_cpu_work || io_uring_sq_ready(&ring_) > 0) {
      jump_from = JUMP_FROM_READY;
      continue;
    }

    ///
    /// End of the tight loop that processes tasks, ready fibers, and submits sqes.
    ///
    bool activated = RunL2Tasks(scheduler);
    if (activated) {  // If we have ready fibers - restart the loop.
      jump_from = JUMP_FROM_L2;
      continue;
    }

    DCHECK(!has_cpu_work && !scheduler->HasReady());
    DCHECK_EQ(io_uring_sq_ready(&ring_), 0u);

    if (io_uring_sq_ready(&ring_) > 0) {
      jump_from = JUMP_FROM_READY;
      continue;
    }

    // DCHECK_EQ(io_uring_cq_ready(&ring_), 0u) does not hold because completions
    // can be updated asynchronously by the kernel (unless IORING_SETUP_DEFER_TASKRUN is set).

    bool should_spin = RunOnIdleTasks();
    if (should_spin) {
      jump_from = JUMP_FROM_SPIN;
      continue;  // continue spinning until on_idle_map_ is empty.
    }

    // Lets spin a bit to make a system a bit more responsive.
    // Important to spin a bit, otherwise we put too much pressure on  eventfd_write.
    // and we enter too often into kernel space.
    if (!ring_busy && spin_loops++ < 10) {
      DVLOG(3) << "spin_loops " << spin_loops;

      // We should not spin too much using sched_yield or it burns a fuckload of cpu.
      scheduler->DestroyTerminated();
      jump_from = JUMP_FROM_SPIN;
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
     * run the loop again. Otherwise, set tq_seq_ to WAIT_SECTION_STATE, marking that
     * we are going to stall now. Other threads will have to wake up the ring
     * (see WakeRing()) but only one will actually call WakeRing.
     * Important:
     *
     * 1. the task_queue_.empty() check is an optimization to avoid unnecessary stalls.
     * 2. It's possible that we may use weaker ordering than memory_order_acq_rel,
     *    but it's not worth the risk. We had a bug with missed notifications before, see
     *    WakeupIfNeeded() for details.
     * 3. In case compare fails, we do not care about the ordering because we reset tq_seq again
     *    at the beginning of the loop.
     */
    if (task_queue_.empty() &&
        tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, memory_order_acq_rel,
                                      memory_order_relaxed)) {
      if (is_stopped_) {
        tq_seq_.store(0, memory_order_release);  // clear WAIT section
        break;
      }

      DCHECK(!scheduler->HasReady());

      VPRO(2) << "wait_for_cqe " << stats_.loop_cnt;
      uint64_t start_cycle = GetCPUCycleCount();
      wait_for_cqe(&ring_, 1, ts_arg);
      IdleEnd(start_cycle);
      VPRO(2) << "Woke up after wait_for_cqe, tq_seq_: " << tq_seq_.load(memory_order_relaxed)
              << " tasks:" << stats_.num_task_runs << " " << stats_.loop_cnt;

      ++stats_.num_stalls;
      tq_seq = 0;
      tq_seq_.store(0, std::memory_order_release);
    }
  }

  VPRO(1) << "total/stalls/cqe_fetches/num_submits: " << stats_.loop_cnt << "/" << stats_.num_stalls
          << "/" << stats_.completions_fetches << "/" << stats_.uring_submit_calls;
  VPRO(1) << "jump_counts: ";
  for (unsigned i = 0; i < JUMP_FROM_TOTAL; ++i) {
    VPRO(1) << i << ": " << jump_counts[i];
  }
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
  auto uring_cb = [id, this](detail::FiberInterface* p, IoResult res, uint32_t flags, uint32_t) {
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
  auto waker = [this](detail::FiberInterface* current, UringProactor::IoResult res, uint32_t flags,
                      uint32_t) {
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

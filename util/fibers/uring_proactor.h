// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <liburing.h>
#include <pthread.h>

#include "util/fibers/proactor_base.h"
#include "util/uring/submit_entry.h"

namespace util {
namespace fb2 {

#ifndef USE_FB2
using uring::SubmitEntry;
#endif

namespace detail {
class Scheduler;
}

class UringProactor : public ProactorBase {
  UringProactor(const UringProactor&) = delete;
  void operator=(const UringProactor&) = delete;

 public:
  UringProactor();
  ~UringProactor();

  void Init(size_t ring_size, int wq_fd = -1);

  using IoResult = int;

  // detail::FiberInterface* is the current fiber.
  // IoResult is the I/O result of the completion event.
  // uint32_t - epoll flags.
  // int64_t is the payload supplied during event submission. See GetSubmitEntry below.
  // using CbType = std::function<void(IoResult, uint32_t)>;
  using CbType =
      fu2::function_base<true /*owns*/, false /*non-copyable*/, fu2::capacity_fixed<16, 8>,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/,
                         void(detail::FiberInterface*, IoResult, uint32_t)>;
  /**
   * @brief Get the Submit Entry object in order to issue I/O request.
   *
   * @param cb - completion callback.
   * @param submit_type - user tag to be supplied to the completion callback. This is useful to
   *                      distinguish between different CQEs when debugging problems.
   *                      tags 0-255 are reserved for internal use.
   * @return SubmitEntry with initialized userdata.
   *
   */
  SubmitEntry GetSubmitEntry(CbType cb, uint16_t submit_tag = 0);

  // Returns number of entries available for submitting to io_uring.
  uint32_t GetSubmitRingAvailability() const {
    return io_uring_sq_space_left(&ring_);
  }

  // Waits till specified number of entries are available for submitting.
  // Returns true if preemption ocurred, false if io_uring has enough space from the beginning.
  bool WaitTillAvailable(uint32_t threshold) {
    return sqe_avail_.await([&] { return GetSubmitRingAvailability() >= threshold; });
  }

  bool HasSqPoll() const {
    return sqpoll_f_;
  }

  bool HasRegisterFd() const {
    return register_fd_;
  }

  int ring_fd() const {
    return ring_.ring_fd;
  }

  unsigned RegisterFd(int source_fd);

  int TranslateFixedFd(int fixed_fd) const {
    return register_fd_ && fixed_fd >= 0 ? register_fds_[fixed_fd] : fixed_fd;
  }

  void UnregisterFd(unsigned fixed_fd);
  LinuxSocketBase* CreateSocket(int fd = -1) final;

  Kind GetKind() const final {
    return IOURING;
  }

  // Currently only a single bin of 1024 buffers of size 64 bytes (total 64K) is supported.
  // Returns 0 on success, errno on failure.
  int RegisterBuffers();

  bool HasRegisteredBuffers() const {
    return bool(registered_buf_);
  }

  // Returns a buffer of size 64 or null if no buffers are found.
  uint8_t* ProvideRegisteredBuffer();
  void ReturnRegisteredBuffer(uint8_t* addr);

  using EpollCB = std::function<void(uint32_t)>;
  using EpollIndex = unsigned;
  EpollIndex EpollAdd(int fd, EpollCB cb, uint32_t event_mask);
  void EpollDel(EpollIndex id);

 private:
  void DispatchCqe(detail::FiberInterface* current, const io_uring_cqe& cqe);

  void RegrowCentries();
  void ArmWakeupEvent();
  void SchedulePeriodic(uint32_t id, PeriodicItem* item) final;
  void CancelPeriodicInternal(uint32_t val1, uint32_t val2) final;

  void PeriodicCb(IoResult res, uint32_t task_id, PeriodicItem* item);

  void MainLoop(detail::Scheduler* sched) final;
  void WakeRing() final;
  void EpollAddInternal(EpollIndex id);
  void EpollDelInternal(EpollIndex id);

  io_uring ring_;

  int wake_fixed_fd_;

  uint8_t sqpoll_f_ : 1;
  uint8_t register_fd_ : 1;
  uint8_t msgring_f_ : 1;
  uint8_t reserved_f_ : 5;

  EventCount sqe_avail_;

  struct CompletionEntry {
    CbType cb;

    // serves for linked list management.
    int64_t index = -1;
  };
  static_assert(sizeof(CompletionEntry) == 40, "");

  std::vector<CompletionEntry> centries_;
  std::vector<int> register_fds_;

  // we keep this vector only for iouring because its timers are one shot.
  // For epoll, periodic timers are refreshed automatically.
  std::vector<std::pair<uint32_t, PeriodicItem*>> schedule_periodic_list_;

  int32_t next_free_ce_ = -1;
  uint32_t pending_cb_cnt_ = 0;
  uint32_t next_free_fd_ = 0;  // next available fd for register files.
  uint32_t get_entry_sq_full_ = 0, get_entry_submit_fail_ = 0, get_entry_await_ = 0;

  int32_t free_req_buf_id_ = -1;
  std::unique_ptr<uint8_t[]> registered_buf_;

  struct EpollEntry {
    EpollCB cb;
    int fd = -1;
    uint32_t event_mask = 0;
    int64_t index = -1;
  };
  std::vector<EpollEntry> epoll_entries_;
  int32_t next_epoll_free_ = -1;
};

class FiberCall {
  FiberCall(const FiberCall&) = delete;
  void operator=(const FiberCall&) = delete;

 public:
  using IoResult = UringProactor::IoResult;

  explicit FiberCall(UringProactor* proactor, uint32_t timeout_msec = UINT32_MAX);

  ~FiberCall();

  SubmitEntry* operator->() {
    return &se_;
  }

  IoResult Get() {
    me_->Suspend();
    me_ = nullptr;

    return io_res_;
  }

  uint32_t flags() const {
    return res_flags_;
  }

 private:
  SubmitEntry se_;
  SubmitEntry tm_;

  detail::FiberInterface* me_;
  UringProactor::IoResult io_res_ = 0;
  timespec ts_;             // in case of timeout.
  uint32_t res_flags_ = 0;  // set by waker upon completion.
};

}  // namespace fb2
}  // namespace util

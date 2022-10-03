// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <liburing.h>
#include <pthread.h>

#include "util/proactor_base.h"
#include "util/uring/submit_entry.h"

namespace util {
namespace uring {

class UringFiberAlgo;

class Proactor : public ProactorBase {
  Proactor(const Proactor&) = delete;
  void operator=(const Proactor&) = delete;

 public:
  Proactor();
  ~Proactor();

  void Init(size_t ring_size, int wq_fd = -1);

  // Runs the poll-loop. Stalls the calling thread which will become the "Proactor" thread.
  void Run() final;

  using IoResult = int;

  // IoResult is the I/O result of the completion event.
  // uint32_t - epoll flags.
  // int64_t is the payload supplied during event submission. See GetSubmitEntry below.
  // using CbType = std::function<void(IoResult, uint32_t, int64_t)>;
  using CbType =
      fu2::function_base<true /*owns*/, false /*non-copyable*/, fu2::capacity_fixed<16, 8>,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/,
                         void(IoResult, uint32_t, int64_t)>;
  /**
   * @brief Get the Submit Entry object in order to issue I/O request.
   *
   * @param cb - completion callback.
   * @param payload - an argument to the completion callback that is further passed as the second
   *                  argument to CbType(). Can be nullptr if no notification is required.
   * @return SubmitEntry with initialized userdata.
   *
   * This method might block the calling fiber therefore it should not be called within proactor
   * context. In other words it can not be called from  *Brief([]...) calls to Proactor.
   * In addition, this method can not be used for introducing IOSQE_IO_LINK chains since they
   * require atomic SQE allocation.
   * @todo We should add GetSubmitEntries that can allocate multiple SQEs atomically.
   *       In that case we will need RegisterCallback function that takes an unregistered SQE
   *       and assigns a callback to it. GetSubmitEntry will be implemented using those functions.
   */
  SubmitEntry GetSubmitEntry(CbType cb, int64_t payload);

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

  ProactorKind GetKind() const final {
    return IOURING;
  }

 private:
  void DispatchCompletions(io_uring_cqe* cqes, unsigned count);

  void RegrowCentries();
  void ArmWakeupEvent();
  void SchedulePeriodic(uint32_t id, PeriodicItem* item) final;
  void CancelPeriodicInternal(uint32_t val1, uint32_t val2) final;

  void PeriodicCb(IoResult res, uint32_t task_id, PeriodicItem* item);

  void WakeRing() final;

  io_uring ring_;

  int wake_fixed_fd_;

  uint8_t sqpoll_f_ : 1;
  uint8_t register_fd_ : 1;
  uint8_t reserved_f_ : 6;

  EventCount sqe_avail_;
  ::boost::fibers::context* main_loop_ctx_ = nullptr;

  friend class UringFiberAlgo;

  struct CompletionEntry {
    CbType cb;

    // serves for linked list management when unused. Also can store an additional payload
    // field when in flight.
    int64_t val = -1;
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
};

class FiberCall {
  FiberCall(const FiberCall&) = delete;
  void operator=(const FiberCall&) = delete;

 public:
  using IoResult = Proactor::IoResult;

  explicit FiberCall(Proactor* proactor, uint32_t timeout_msec = UINT32_MAX);

  ~FiberCall();

  SubmitEntry* operator->() {
    return &se_;
  }

  IoResult Get() {
    me_->suspend();
    me_ = nullptr;

    return io_res_;
  }

  uint32_t flags() const {
    return res_flags_;
  }

 private:
  SubmitEntry se_;
  SubmitEntry tm_;

  ::boost::fibers::context* me_;
  Proactor::IoResult io_res_ = 0;
  timespec ts_;             // in case of timeout.
  uint32_t res_flags_ = 0;  // set by waker upon completion.
};

}  // namespace uring
}  // namespace util

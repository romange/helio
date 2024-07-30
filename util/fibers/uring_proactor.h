// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <liburing.h>
#include <pthread.h>

#include "util/fibers/proactor_base.h"
#include "util/fibers/submit_entry.h"
#include "base/segment_pool.h"

namespace util {
namespace fb2 {

namespace detail {
class Scheduler;
}

// Aligned buffer that is optionally part of registered buffer (io_uring_register_buffers)
struct UringBuf {
  static constexpr size_t kAlign = 4096;

  io::MutableBytes bytes;           // buf, nbytes
  std::optional<unsigned> buf_idx;  // buf_idx
};

class UringProactor : public ProactorBase {
  UringProactor(const UringProactor&) = delete;
  void operator=(const UringProactor&) = delete;

 public:
  static constexpr unsigned kInvalidDirectFd = -1;

  UringProactor();
  ~UringProactor();

  void Init(unsigned pool_index, size_t ring_size, int wq_fd = -1);

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

  bool HasPollFirst() const {
    return poll_first_;
  }

  bool HasDirectFD() const {
    return !register_fds_.empty();
  }

  int ring_fd() const {
    return ring_.ring_fd;
  }

  // Returns direct fd or kInvalidDirectFd if it did not succeed.
  unsigned RegisterFd(int source_fd);

  // Returns a linux file descriptor given a valid fixed fd.
  int TranslateDirectFd(unsigned fixed_fd) const;

  // Unregisters a fixed fd. Returns linux fd or -1 if it did not succeed.
  int UnregisterFd(unsigned fixed_fd);

  LinuxSocketBase* CreateSocket() final;

  Kind GetKind() const final {
    return IOURING;
  }

  // Register buffer with given size and allocate backing, calls io_uring_register_buffers
  int RegisterBuffers(size_t size);

  // Request buffer of given size, returns none if there's no space left in the backing.
  // Must be returned with ReturnBuffer
  std::optional<UringBuf> RequestBuffer(size_t size);
  void ReturnBuffer(UringBuf buf);

  // Wrapper interface around io_uring_(un)register_buffers.
  // Returns 0 on success, -errno on failure.
  int RegisterBuffers(const struct iovec* iovecs, unsigned nr_vecs);
  int UnregisterBuffers();

  // Experimental. should not be called in production.
  // Registers an iouring buffer ring (see io_uring_register_buf_ring(3)).
  // Registers a predefined 16K buffer ring with specified buffer group_id.
  // Returns 0 on success, errno on failure.
  int RegisterBufferRing(unsigned group_id);
  uint8_t* GetBufRingPtr(unsigned group_id, unsigned bufid);
  void ConsumeBufRing(unsigned group_id, unsigned len);

  // Returns 0 on success, errno on failure.
  // See io_uring_prep_cancel(3) for flags.
  int CancelRequests(int fd, unsigned flags);

  using EpollCB = std::function<void(uint32_t)>;
  using EpollIndex = unsigned;
  EpollIndex EpollAdd(int fd, EpollCB cb, uint32_t event_mask);
  void EpollDel(EpollIndex id);

 private:
  void ProcessCqeBatch(unsigned count, io_uring_cqe** cqes, detail::FiberInterface* current);
  void ReapCompletions(unsigned count, io_uring_cqe** cqes, detail::FiberInterface* current);

  void RegrowCentries();

  // Used with older kernels with msgring_f_ == 0.
  void ArmWakeupEvent();
  void SchedulePeriodic(uint32_t id, PeriodicItem* item) final;
  void CancelPeriodicInternal(PeriodicItem* item) final;

  void PeriodicCb(IoResult res, uint32_t task_id, PeriodicItem* item);

  void MainLoop(detail::Scheduler* sched) final;
  void WakeRing() final;
  void EpollAddInternal(EpollIndex id);
  void EpollDelInternal(EpollIndex id);

  io_uring ring_;

  uint8_t msgring_f_ : 1;
  uint8_t poll_first_ : 1;
  uint8_t buf_ring_f_ : 1;
  uint8_t : 5;

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
  // TODO: start using IORING_TIMEOUT_MULTISHOT (see io_uring_prep_timeout(3)).
  std::vector<std::pair<uint32_t, PeriodicItem*>> schedule_periodic_list_;

  struct BufRingGroup {
    io_uring_buf_ring* ring = nullptr;
    uint8_t* buf = nullptr;
  };

  std::vector<BufRingGroup> bufring_groups_;

  // Keeps track of requested buffers
  struct {
    uint8_t* backing = nullptr;
    base::SegmentPool segments{};
  } buf_pool_{};

  int32_t next_free_ce_ = -1;
  uint32_t pending_cb_cnt_ = 0;
  uint32_t next_free_index_ = 0;  // next available fd for register files.
  uint32_t direct_fds_cnt_ = 0;
  uint32_t get_entry_sq_full_ = 0, get_entry_await_ = 0;
  uint64_t reaped_cqe_cnt_ = 0;

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

  IoResult Get();

  uint32_t flags() const {
    return res_flags_;
  }

 private:
  SubmitEntry se_;

  detail::FiberInterface* me_;
  UringProactor::IoResult io_res_ = 0;
  uint32_t res_flags_ = 0;  // set by waker upon completion.
  bool was_run_ = false;
  timespec ts_;             // in case of timeout.
};

}  // namespace fb2
}  // namespace util

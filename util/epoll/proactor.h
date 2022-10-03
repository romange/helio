// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/proactor_base.h"

struct epoll_event;

namespace util {
namespace epoll {

class EpollFiberAlgo;
class EvPool;

class EpollProactor : public ProactorBase {
 public:
  EpollProactor();
  ~EpollProactor();

  // should be called from the thread that owns this EpollProactor before calling Run.
  void Init();

  // Runs the poll-loop. Stalls the calling thread which will become the "EpollProactor" thread.
  void Run() final;

  using IoResult = int;

  // event_mask passed from epoll_event.events.
  using CbType = std::function<void(uint32_t event_mask, EpollProactor*)>;

  // Returns the handler id for the armed event.
  unsigned Arm(int fd, CbType cb, uint32_t event_mask);
  void UpdateCb(unsigned arm_index, CbType cb);
  void Disarm(int fd, unsigned arm_index);

  int ev_loop_fd() const {
    return epoll_fd_;
  }

  ProactorKind GetKind() const final {
    return EPOLL;
  }

 private:
  void DispatchCompletions(struct epoll_event* cevents, unsigned count);

  LinuxSocketBase* CreateSocket(int fd = -1) final;
  void SchedulePeriodic(uint32_t id, PeriodicItem* item) final;
  void CancelPeriodicInternal(uint32_t val1, uint32_t val2) final;
  void PeriodicCb(PeriodicItem* item);

  void RegrowCentries();
  void ArmWakeupEvent();

  int epoll_fd_ = -1;

  ::boost::fibers::context* main_loop_ctx_ = nullptr;

  // friend class EpollFiberAlgo;
  struct CompletionEntry {
    CbType cb;

    // serves for linked list management when unused. Also can store an additional payload
    // field when in flight.
    int32_t index = -1;
    int32_t unused = -1;
  };
  static_assert(sizeof(CompletionEntry) == 40, "");

  std::vector<CompletionEntry> centries_;
  int32_t next_free_ce_ = -1;
};

}  // namespace epoll
}  // namespace util

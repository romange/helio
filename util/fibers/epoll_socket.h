// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/fiber_socket_base.h"
#include "util/fibers/epoll_proactor.h"

namespace util {
namespace fb2 {

class EpollSocket : public LinuxSocketBase {
 public:
  using FiberSocketBase::AsyncWriteCb;

  template <typename T> using Result = io::Result<T>;

  EpollSocket(int fd = -1);

  virtual ~EpollSocket();

  ABSL_MUST_USE_RESULT AcceptResult Accept() final;

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep) final;
  ABSL_MUST_USE_RESULT error_code Close() final;

  // Really need here expected.
  Result<size_t> WriteSome(const iovec* ptr, uint32_t len) override;
  void AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;
  Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  //! Subsribes to one-shot poll. event_mask is a mask of POLLXXX values.
  //! When and an event occurs, the cb will be called with the mask of actual events
  //! that trigerred it.
  //! Returns: handle id that can be used to cancel the poll request (see CancelPoll below).
  uint32_t PollEvent(uint32_t event_mask, std::function<void(uint32_t)> cb) final;

  //! Cancels the poll event. id must be the id returned by PollEvent function.
  //! Returns 0 if cancellation ocurred, or ENOENT, EALREADY if poll has not been found or
  //! in process of completing.
  uint32_t CancelPoll(uint32_t id) final;

  using FiberSocketBase::IsConnClosed;

 private:
  EpollProactor* GetProactor() {
    return static_cast<EpollProactor*>(proactor());
  }
  void OnSetProactor() final;
  void OnResetProactor() final;

  // returns true if the operation has completed.
  bool SuspendMyself(detail::FiberInterface* cntx, std::error_code* ec);

  void Wakey(uint32_t mask, EpollProactor* cntr);

  detail::FiberInterface* write_context_ = nullptr;
  detail::FiberInterface* read_context_ = nullptr;
  int32_t arm_index_ = -1;
  uint32_t epoll_mask_ = 0;
};

constexpr size_t kSizeofEpollSocket = sizeof(EpollSocket);


}  // namespace fb2
}  // namespace util

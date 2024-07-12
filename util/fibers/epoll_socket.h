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
  template <typename T> using Result = io::Result<T>;

  EpollSocket(int fd = -1);

  virtual ~EpollSocket();

  ABSL_MUST_USE_RESULT AcceptResult Accept() final;

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep,
                                          std::function<void(int)> on_pre_connect) final;
  ABSL_MUST_USE_RESULT error_code Close() final;

  // Really need here expected.
  Result<size_t> WriteSome(const iovec* ptr, uint32_t len) override;
  void AsyncWriteSome(const iovec* v, uint32_t len, AsyncProgressCb cb) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;
  Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  error_code Shutdown(int how) override;

  void RegisterOnErrorCb(std::function<void(uint32_t)> cb) final;
  void CancelOnErrorCb() final;

  using FiberSocketBase::IsConnClosed;

 private:
  EpollProactor* GetProactor() {
    return static_cast<EpollProactor*>(proactor());
  }
  void OnSetProactor() final;
  void OnResetProactor() final;

  // returns true if the operation has completed.
  bool SuspendMyself(detail::FiberInterface* cntx, std::error_code* ec);

  // kevent pass error code together with completion event.
  void Wakey(uint32_t event_flags, int error, EpollProactor* cntr);

  detail::FiberInterface* write_context_ = nullptr;
  detail::FiberInterface* read_context_ = nullptr;
  int32_t arm_index_ = -1;
  uint16_t epoll_mask_ = 0;
  uint16_t kev_error_ = 0;

  std::function<void(uint32_t)> error_cb_;
};

constexpr size_t kSizeofEpollSocket = sizeof(EpollSocket);

}  // namespace fb2
}  // namespace util

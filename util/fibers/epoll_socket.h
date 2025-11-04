// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <utility>

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

  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override;
  void AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;
  Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  error_code Shutdown(int how) override;

  unsigned RecvProvided(unsigned buf_len, ProvidedBuffer* dest) final;
  void ReturnProvided(const ProvidedBuffer& pbuf) final;

  void RegisterOnErrorCb(std::function<void(uint32_t)> cb) final;
  void CancelOnErrorCb() final;

  using FiberSocketBase::IsConnClosed;

 private:
  class PendingReq;

  struct AsyncReq {
    uint32_t len;
    iovec* vec;
    io::AsyncProgressCb cb;

    AsyncReq(iovec* v, uint32_t l, io::AsyncProgressCb _cb) : len(l), vec(v), cb(std::move(_cb)) {
    }

    // Caller is responsible for *calling* cb.
    // Returns true if the callback has run.
    std::pair<bool, Result<size_t>> Run(int fd, bool is_send);
  };

  EpollProactor* GetProactor() {
    return static_cast<EpollProactor*>(proactor());
  }
  void OnSetProactor() final;
  void OnResetProactor() final;

  // kevent pass error code together with completion event.
  void Wakey(uint32_t event_flags, int error, EpollProactor* cntr);

  void HandleAsyncRequest(error_code ec, bool is_send);

  union {
    PendingReq* write_req_;
    AsyncReq* async_write_req_;
  };

  union {
    PendingReq* read_req_;
    AsyncReq* async_read_req_;
  };

  int32_t arm_index_ = -1;

  static constexpr uint32_t kMaxBufSize = 1 << 16;
  static constexpr uint32_t kMinBufSize = 1 << 4;
  uint32_t bufreq_sz_ = kMinBufSize;
  union {
    struct {
      uint8_t async_write_pending_ : 1;
      uint8_t async_read_pending_ : 1;
    };
    uint8_t flags_;
  };
  std::function<void(uint32_t)> error_cb_;
};

constexpr size_t kSizeofEpollSocket = sizeof(EpollSocket);

}  // namespace fb2
}  // namespace util

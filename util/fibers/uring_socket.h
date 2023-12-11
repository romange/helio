// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <liburing.h>

#include "util/fiber_socket_base.h"
#include "util/fibers/uring_proactor.h"

namespace util {

namespace fb2 {

class UringSocket : public LinuxSocketBase {
 public:
  using Proactor = UringProactor;
  using FiberSocketBase::AsyncWriteCb;

  template <typename T> using Result = io::Result<T>;

  UringSocket(int fd, Proactor* p);

  UringSocket(Proactor* p = nullptr) : UringSocket(-1, p) {
  }

  virtual ~UringSocket();

  ABSL_MUST_USE_RESULT AcceptResult Accept() final;

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep) final;
  ABSL_MUST_USE_RESULT error_code Close() final;

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override;
  void AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;
  Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  using FiberSocketBase::IsConnClosed;

  void RegisterOnErrorCb(std::function<void(uint32_t)> cb) final;
  void CancelOnErrorCb() final;

 private:
  UringProactor* GetProactor() {
    return static_cast<Proactor*>(proactor());
  }

  const UringProactor* GetProactor() const {
    return static_cast<const UringProactor*>(proactor());
  }

  uint8_t register_flag() const {
    return false ? IOSQE_FIXED_FILE : 0;
  }

  uint32_t error_cb_id_ = UINT32_MAX;
};

}  // namespace fb2
}  // namespace util

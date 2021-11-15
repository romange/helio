// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing/io_uring.h>

#include "util/fiber_socket_base.h"
#include "util/uring/proactor.h"

namespace util {
namespace uring {

class UringSocket : public LinuxSocketBase {
 public:
  template<typename T> using Result = io::Result<T>;

  UringSocket(int fd, Proactor* p) : LinuxSocketBase(fd, p) {
  }

  UringSocket(Proactor* p = nullptr) : UringSocket(-1, p) {
  }

  virtual ~UringSocket();

  ABSL_MUST_USE_RESULT accept_result Accept() final;

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep) final;
  ABSL_MUST_USE_RESULT error_code Close() final;

  using FiberSocketBase::Send;

  // Really need here expected.
  Result<size_t> Send(const iovec* ptr, size_t len) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;

  using FiberSocketBase::IsConnClosed;

 private:
  Proactor* GetProactor() { return static_cast<Proactor*>(proactor()); }
};

}  // namespace uring
}  // namespace util

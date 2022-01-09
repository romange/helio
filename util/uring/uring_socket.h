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
  template <typename T> using Result = io::Result<T>;

  UringSocket(int fd, Proactor* p) : LinuxSocketBase(fd, p) {
  }

  UringSocket(Proactor* p = nullptr) : UringSocket(-1, p) {
  }

  virtual ~UringSocket();

  ABSL_MUST_USE_RESULT AcceptResult Accept() final;

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep) final;
  ABSL_MUST_USE_RESULT error_code Close() final;

  using FiberSocketBase::Send;

  // Really need here expected.
  Result<size_t> Send(const iovec* ptr, size_t len) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;

  using FiberSocketBase::IsConnClosed;

  //! Subsribes to one-shot poll. event_mask is a mask of POLLXXX values.
  //! When and an event occurs, the cb will be called with the mask of actual events
  //! that trigerred it.
  //! Returns: handle id that can be used to cancel the poll request (see CancelPoll below).
  uint32_t PollEvent(uint32_t event_mask, std::function<void(uint32_t)> cb);

  //! Cancels the poll event. id must be the id returned by PollEvent function.
  //! Returns 0 if cancellation ocurred, or ENOENT, EALREADY if poll has not been found or
  //! in process of completing.
  uint32_t CancelPoll(uint32_t id);

 private:
  Proactor* GetProactor() {
    return static_cast<Proactor*>(proactor());
  }
};

}  // namespace uring
}  // namespace util

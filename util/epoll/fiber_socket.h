// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/fiber_socket_base.h"
#include "util/epoll/ev_controller.h"

namespace util {
namespace epoll {

class FiberSocket : public LinuxSocketBase {
 public:
  template<typename T> using Result = io::Result<T>;

  FiberSocket(int fd = -1);

  virtual ~FiberSocket();

  ABSL_MUST_USE_RESULT AcceptResult Accept() final;

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep) final;
  ABSL_MUST_USE_RESULT error_code Close() final;

  // Really need here expected.
  Result<size_t> WriteSome(const iovec* ptr, uint32_t len) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;

  using FiberSocketBase::IsConnClosed;

 private:
  EvController* GetEv() { return static_cast<EvController*>(proactor()); }
  void OnSetProactor() final;

  void Wakey(uint32_t mask, EvController* cntr);

  ::boost::fibers::context* current_context_ = nullptr;
  int arm_index_ = -1;
};

}  // namespace epoll
}  // namespace util

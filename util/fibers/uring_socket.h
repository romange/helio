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

  template <typename T> using Result = io::Result<T>;

  UringSocket(int fd, Proactor* p);

  UringSocket(Proactor* p = nullptr) : UringSocket(-1, p) {
  }

  virtual ~UringSocket();

  // Creates a socket. Prerequisite: the socket has not been opened before
  // using Connect or created via Accept.
  error_code Create(unsigned short protocol_family = 2) final;

  ABSL_MUST_USE_RESULT AcceptResult Accept() final;

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep,
                                          std::function<void(int)> on_pre_connect) final;
  ABSL_MUST_USE_RESULT error_code Close() final;

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override;
  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override;
  void AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;
  Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  using FiberSocketBase::IsConnClosed;

  void RegisterOnErrorCb(std::function<void(uint32_t)> cb) final;
  void CancelOnErrorCb() final;
  void RegisterOnRecv(OnRecvCb cb) final;
  void ResetOnRecvHook() final;

  // Returns the native linux fd even for direct-fd iouring mode.
  native_handle_type native_handle() const final;

  bool HasRecvData() const {
    return has_recv_data_;
  }

  void EnableRecvMultishot() {
    enable_multi_shot_ = 1;
  }

  void set_bufring_id(uint16_t id) {
    bufring_id_ = id;
  }

 private:
  UringProactor* GetProactor() {
    return static_cast<Proactor*>(proactor());
  }

  const UringProactor* GetProactor() const {
    return static_cast<const UringProactor*>(proactor());
  }

  void OnSetProactor() final;
  void OnResetProactor() final;

  uint8_t register_flag() const {
    return is_direct_fd_ ? IOSQE_FIXED_FILE : 0;
  }

  void UpdateDfVal(unsigned val) {
    fd_ = (val << kFdShift) | (fd_ & ((1 << kFdShift) - 1));
  }

  struct ErrorCbRefWrapper {
    uint32_t error_cb_id = 0;
    uint32_t ref_count = 2;  // one for the socket reference, one for the completion lambda.
    std::function<void(uint32_t)> cb;

    static ErrorCbRefWrapper* New(std::function<void(uint32_t)> cb) {
      return new ErrorCbRefWrapper(std::move(cb));
    }

    static void Destroy(ErrorCbRefWrapper* ptr) {
      ptr->cb = {};
      if (--ptr->ref_count == 0)
        delete ptr;
    }

   private:
    ErrorCbRefWrapper(std::function<void(uint32_t)> _cb) : cb(std::move(_cb)) {
    }
  };

  ErrorCbRefWrapper* error_cb_wrapper_ = nullptr;
  UringProactor::EpollIndex recv_poll_id_ = 0;

  union {
    uint16_t flags_;
    struct {
      uint16_t has_pollfirst_ : 1;
      uint16_t has_recv_data_ : 1;
      uint16_t is_direct_fd_ : 1;
      uint16_t enable_multi_shot_ : 1;
      uint16_t register_recv_multishot_ : 1;
    };
  };
  uint16_t bufring_id_ = 0;
};

}  // namespace fb2
}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <liburing.h>

#include <queue>

#include "util/fiber_socket_base.h"
#include "util/fibers/uring_proactor.h"

namespace util {

namespace fb2 {

class UringSocket;

class MultiShotReceiver {

public:
  MultiShotReceiver() = default;

  // Blocks until one or more slices are available.
  // Returns number slices (strictly positive) copied if succeeded,
  // or -errno on error, or 0 if multishot state has concluded.
  // Must be followed up by Consume when finished accessing the slices.
  int Next(iovec* dest, unsigned len);

  // Follows up on the previous Next call.
  // Returns number of slices copied to dest.
  // Never blocks.
  void Consume(unsigned len);

  bool Armed() const {
    return proactor_ != nullptr;
  }
private:
  friend class UringSocket;

  struct Slice {
    int32_t len;  // positive len, negative errno.
    uint16_t index;

    Slice(int32_t l, uint16_t i) : len(l), index(i) {}
  } __attribute__((packed));

  std::queue<Slice> slices_;
  UringProactor* proactor_;
  detail::FiberInterface* waiter_ = nullptr;
};

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
  void AsyncWriteSome(const iovec* v, uint32_t len, AsyncProgressCb cb) override;

  Result<size_t> RecvMsg(const msghdr& msg, int flags) override;
  Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  using FiberSocketBase::IsConnClosed;

  void RegisterOnErrorCb(std::function<void(uint32_t)> cb) final;
  void CancelOnErrorCb() final;

  // Returns the native linux fd even for direct-fd iouring mode.
  native_handle_type native_handle() const final;

  bool HasRecvData() const {
    return has_recv_data_;
  }

  void SetupReceiveMultiShot(MultiShotReceiver* receiver);
  void CancelRequests();

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

  union {
    uint32_t flags_;
    struct {
      uint32_t has_pollfirst_ : 1;
      uint32_t has_recv_data_ : 1;
      uint32_t is_direct_fd_ : 1;
    };
  };
};

}  // namespace fb2
}  // namespace util

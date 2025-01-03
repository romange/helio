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

  // Returns the native linux fd even for direct-fd iouring mode.
  native_handle_type native_handle() const final;

  bool HasRecvData() const {
    return has_recv_data_;
  }

  // ProvidedBuffer is a struct that contains a pointer to the buffer and its size or an error code.
  // When called in Multishot mode, if an error is filled, the multishot mode is
  // disabled automatically.
  // If no provided buffers are available, the function will fill in the ENOBUFS error.
  unsigned RecvProvided(unsigned buf_len, ProvidedBuffer* dest) final;
  void ReturnProvided(const ProvidedBuffer& pbuf) final;

  void EnableRecvMultishot();
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

  void CancelMultiShot();

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

  struct MultiShot {
    detail::FiberInterface* poll_pending = nullptr;

    uint16_t tail = UringProactor::kMultiShotUndef;
    uint16_t err_no = 0;

    union {
      struct {
        uint8_t refcnt : 4; // range [0-2]: socket + callback or deleted.
        uint8_t error_raised : 1;
      };
      uint8_t flags_;
    };

    // Returns true if this object was deleted.
    bool DecRef();

    void Activate(int fd, uint16_t bufring_id, uint8_t flags, UringProactor* proactor);

    MultiShot() : flags_(0) { refcnt = 1; }

    bool HasBuffers() const {
      return tail != UringProactor::kMultiShotUndef;
    }
  };

  static_assert(sizeof(MultiShot) == 16, "");

  MultiShot* multishot_ = nullptr;

  union {
    uint16_t flags_;
    struct {
      uint16_t has_pollfirst_ : 1;
      uint16_t has_recv_data_ : 1;
      uint16_t is_direct_fd_ : 1;
    };
  };
  uint16_t bufring_id_ = 0;
};

}  // namespace fb2
}  // namespace util

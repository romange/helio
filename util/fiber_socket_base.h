// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/attributes.h>

// for tcp::endpoint. Consider introducing our own.
#include <boost/asio/ip/tcp.hpp>
#include <functional>

#include "io/io.h"

namespace util {

namespace fb2 {
class ProactorBase;
}  // namespace fb2

class FiberSocketBase : public io::Sink,
                        public io::AsyncSink,
                        public io::Source,
                        public io::AsyncSource {
  FiberSocketBase(const FiberSocketBase&) = delete;
  void operator=(const FiberSocketBase&) = delete;
  FiberSocketBase(FiberSocketBase&& other) = delete;
  FiberSocketBase& operator=(FiberSocketBase&& other) = delete;

 protected:
  explicit FiberSocketBase(fb2::ProactorBase* pb) : proactor_(pb) {
  }

 public:
  using endpoint_type = ::boost::asio::ip::tcp::endpoint;
  using error_code = std::error_code;
  using AcceptResult = ::io::Result<FiberSocketBase*>;
  using ProactorBase = fb2::ProactorBase;

  ABSL_MUST_USE_RESULT virtual error_code Shutdown(int how) = 0;

  ABSL_MUST_USE_RESULT virtual AcceptResult Accept() = 0;

  ABSL_MUST_USE_RESULT virtual error_code Connect(const endpoint_type& ep,
                                                  std::function<void(int)> on_pre_connect = {}) = 0;

  ABSL_MUST_USE_RESULT virtual error_code Close() = 0;

  virtual bool IsOpen() const = 0;

  ::io::Result<size_t> virtual RecvMsg(const msghdr& msg, int flags) = 0;

  ::io::Result<size_t> Recv(const iovec* ptr, size_t len);

  // to satisfy io::Source concept.
  ::io::Result<size_t> ReadSome(const iovec* v, uint32_t len) final {
    return len > 1 ? Recv(v, len)
                   : Recv(io::MutableBytes{reinterpret_cast<uint8_t*>(v->iov_base), v->iov_len}, 0);
  }

  virtual ::io::Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) = 0;

  struct ProvidedBuffer {
    union {
      uint8_t* start;
      uint16_t bid;
    };

    int res_len;   // positive len, negative errno.
    uint32_t allocated;
    uint8_t cookie;   // Used by the socket to identify the buffer source.

    void SetError(uint16_t err) {
      res_len = -int(err);
      allocated = 0;
      start = nullptr;
    }
  };

  // Unlike Recv/ReadSome, this method returns buffers managed by the socket.
  // They should be returned back to the socket after the data is read.
  // Returns - number of buffers filled.
  virtual unsigned RecvProvided(unsigned buf_len, ProvidedBuffer* dest) = 0;

  virtual void ReturnProvided(const ProvidedBuffer& pbuf) = 0;

  static bool IsConnClosed(const error_code& ec) {
    return (ec == std::errc::connection_aborted) || (ec == std::errc::connection_reset) ||
           (ec == std::errc::broken_pipe);
  }

  virtual void SetProactor(ProactorBase* p);

  ProactorBase* proactor() {
    return proactor_;
  }

  const ProactorBase* proactor() const {
    return proactor_;
  }

  // UINT32_MAX to disable timeout.
  virtual void set_timeout(uint32_t msec) = 0;
  virtual uint32_t timeout() const = 0;

  using AsyncSink::AsyncWrite;
  using AsyncSink::AsyncWriteSome;
  using AsyncSource::AsyncRead;
  using AsyncSource::AsyncReadSome;

  virtual endpoint_type LocalEndpoint() const = 0;
  virtual endpoint_type RemoteEndpoint() const = 0;

  //! Registers a callback that will be called if the socket is closed or has an error.
  //! Should not be called if a callback is already registered.
  virtual void RegisterOnErrorCb(std::function<void(uint32_t)> cb) = 0;

  //! Cancels a callback that was registered with RegisterOnErrorCb. Must be reentrant.
  virtual void CancelOnErrorCb() = 0;

  virtual bool IsUDS() const = 0;

  using native_handle_type = int;
  virtual native_handle_type native_handle() const = 0;

  /// Creates a socket. By default with AF_INET family (2).
  virtual error_code Create(unsigned short protocol_family = 2) = 0;

  virtual ABSL_MUST_USE_RESULT error_code Bind(const struct sockaddr* bind_addr,
                                               unsigned addr_len) = 0;
  virtual ABSL_MUST_USE_RESULT error_code Listen(unsigned backlog) = 0;

  // Listens on all interfaces. If port is 0 then a random available port is chosen
  // by the OS.
  virtual ABSL_MUST_USE_RESULT error_code Listen(uint16_t port, unsigned backlog) = 0;

  // Listen on UDS socket. Must be created with Create(AF_UNIX) first.
  virtual ABSL_MUST_USE_RESULT error_code ListenUDS(const char* path, mode_t permissions,
                                                    unsigned backlog) = 0;

 protected:
  virtual void OnSetProactor() {
  }

  virtual void OnResetProactor() {
  }

 private:
  // We must reference proactor in each socket so that we could support write_some/read_some
  // with predefined interface and be compliant with SyncWriteStream/SyncReadStream concepts.
  ProactorBase* proactor_;
};

class LinuxSocketBase : public FiberSocketBase {
 public:
  using FiberSocketBase::native_handle_type;

  virtual ~LinuxSocketBase();

  native_handle_type native_handle() const override {
    static_assert(int32_t(-1) >> kFdShift == -1);

    return ShiftedFd();
  }

  /// Creates a socket. By default with AF_INET family (2).
  error_code Create(unsigned short protocol_family = 2) override;

  ABSL_MUST_USE_RESULT error_code Bind(const struct sockaddr* bind_addr,
                                       unsigned addr_len) override;
  ABSL_MUST_USE_RESULT error_code Listen(unsigned backlog) override;

  // Listens on all interfaces. If port is 0 then a random available port is chosen
  // by the OS.
  ABSL_MUST_USE_RESULT error_code Listen(uint16_t port, unsigned backlog) override;

  // Listen on UDS socket. Must be created with Create(AF_UNIX) first.
  ABSL_MUST_USE_RESULT error_code ListenUDS(const char* path, mode_t permissions,
                                            unsigned backlog) override;

  error_code Shutdown(int how) override;

  // UINT32_MAX to disable timeout.
  void set_timeout(uint32_t msec) final override {
    timeout_ = msec;
  }

  uint32_t timeout() const final override {
    return timeout_;
  }

  //! Removes the ownership over file descriptor. Use with caution.
  void Detach() {
    fd_ = -1;
  }

  //! IsOpen does not promise that the socket is TCP connected or live,
  // just that the file descriptor is valid and its state is open.
  bool IsOpen() const final {
    return (fd_ & IS_SHUTDOWN) == 0;
  }

  endpoint_type LocalEndpoint() const override;
  endpoint_type RemoteEndpoint() const override;

  bool IsUDS() const override {
    return fd_ & IS_UDS;
  }

 protected:
  constexpr static unsigned kFdShift = 4;

  LinuxSocketBase(int fd, ProactorBase* pb)
      : FiberSocketBase(pb), fd_(fd > 0 ? fd << kFdShift : fd) {
  }

  int ShiftedFd() const {
    return fd_ >> kFdShift;
  }

  enum {
    IS_SHUTDOWN = 0x1,
    IS_UDS = 0x2,
  };

  // Flags which are passed on to peers produced by Accept()
  const static int32_t kInheritedFlags = IS_UDS;

  // kFdShift low bits are used for masking the state of fd.
  // gives me 256M descriptors.
  int32_t fd_;

 private:
  uint32_t timeout_ = UINT32_MAX;
};

void SetNonBlocking(int fd);

void SetCloexec(int fd);

}  // namespace util

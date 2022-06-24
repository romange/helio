// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

// for tcp::endpoint. Consider introducing our own.
#include <boost/asio/ip/tcp.hpp>

#include "absl/base/attributes.h"
#include "io/io.h"

namespace util {

class ProactorBase;

class FiberSocketBase : public io::Sink {
  FiberSocketBase(const FiberSocketBase&) = delete;
  void operator=(const FiberSocketBase&) = delete;
  FiberSocketBase(FiberSocketBase&& other) = delete;
  FiberSocketBase& operator=(FiberSocketBase&& other) = delete;

 protected:
  explicit FiberSocketBase(ProactorBase* pb) : proactor_(pb) {
  }

 public:
  using endpoint_type = ::boost::asio::ip::tcp::endpoint;
  using error_code = std::error_code;
  using AcceptResult = ::io::Result<FiberSocketBase*>;

  ABSL_MUST_USE_RESULT virtual error_code Shutdown(int how) = 0;

  ABSL_MUST_USE_RESULT virtual AcceptResult Accept() = 0;

  ABSL_MUST_USE_RESULT virtual error_code Connect(const endpoint_type& ep) = 0;

  ABSL_MUST_USE_RESULT virtual error_code Close() = 0;

  virtual bool IsOpen() const = 0;

  ::io::Result<size_t> virtual RecvMsg(const msghdr& msg, int flags) = 0;

  ::io::Result<size_t> Recv(const iovec* ptr, size_t len);

  ::io::Result<size_t> Recv(const io::MutableBytes& mb) {
    iovec v{mb.data(), mb.size()};
    return Recv(&v, 1);
  }

  static bool IsConnClosed(const error_code& ec) {
    return (ec == std::errc::connection_aborted) || (ec == std::errc::connection_reset);
  }

  void SetProactor(ProactorBase* p);

  ProactorBase* proactor() {
    return proactor_;
  }

  // UINT32_MAX to disable timeout.
  void set_timeout(uint32_t msec) {
    timeout_ = msec;
  }
  uint32_t timeout() const {
    return timeout_;
  }

 protected:
  virtual void OnSetProactor() {
  }

  virtual void OnResetProactor() {
  }

 private:
  // We must reference proactor in each socket so that we could support write_some/read_some
  // with predefined interface and be compliant with SyncWriteStream/SyncReadStream concepts.
  ProactorBase* proactor_;
  uint32_t timeout_ = UINT32_MAX;
};

class LinuxSocketBase : public FiberSocketBase {
 public:
  using native_handle_type = int;

  virtual ~LinuxSocketBase();

  native_handle_type native_handle() const {
    static_assert(int32_t(-1) >> 3 == -1);

    return fd_ >> 3;
  }

  // Creates a socket
  error_code Create();

  ABSL_MUST_USE_RESULT error_code Listen(const struct sockaddr* bind_addr, unsigned addr_len,
                                         unsigned backlog);

  // Listens on all interfaces. If port is 0 then a random available port is chosen
  // by the OS.
  ABSL_MUST_USE_RESULT error_code Listen(unsigned port, unsigned backlog);

  error_code Shutdown(int how) override;

  //! Removes the ownership over file descriptor. Use with caution.
  void Detach() {
    fd_ = -1;
  }

  //! IsOpen does not promise that the socket is TCP connected or live,
  // just that the file descriptor is valid and its state is open.
  bool IsOpen() const final {
    return (fd_ & IS_SHUTDOWN) == 0;
  }

  endpoint_type LocalEndpoint() const;
  endpoint_type RemoteEndpoint() const;

 protected:
  LinuxSocketBase(int fd, ProactorBase* pb) : FiberSocketBase(pb), fd_(fd > 0 ? fd << 3 : fd) {
  }

  enum { IS_SHUTDOWN = 0x1 };

  // 3 low bits are used for masking the state of fd.
  // gives me 512M descriptors.
  int32_t fd_;
};

class SocketSource : public io::Source {
 public:
  SocketSource(FiberSocketBase* sock) : sock_(sock) {
  }

  io::Result<size_t> ReadSome(const iovec* v, uint32_t len) final {
    return sock_->Recv(v, len);
  }

 private:
  FiberSocketBase* sock_;
};

}  // namespace util

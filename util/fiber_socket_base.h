// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// for tcp::endpoint. Consider introducing our own.
#include <absl/base/attributes.h>

#include <boost/asio/ip/tcp.hpp>

#include "io/io.h"

namespace util {

namespace fb2 {
class ProactorBase;
}  // namespace fb2

class FiberSocketBase : public io::Sink, public io::AsyncSink, public io::Source {
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
  using io::AsyncSink::AsyncWriteCb;
  using ProactorBase = fb2::ProactorBase;

  ABSL_MUST_USE_RESULT virtual error_code Shutdown(int how) = 0;

  ABSL_MUST_USE_RESULT virtual AcceptResult Accept() = 0;

  ABSL_MUST_USE_RESULT virtual error_code Connect(const endpoint_type& ep) = 0;

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

  using AsyncSink::AsyncWrite;
  using AsyncSink::AsyncWriteSome;

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
  constexpr static unsigned kFdShift = 4;

  virtual ~LinuxSocketBase();

  native_handle_type native_handle() const {
    static_assert(int32_t(-1) >> kFdShift == -1);

    return fd_ >> kFdShift;
  }

  /// Creates a socket. By default with AF_INET family (2).
  error_code Create(unsigned short protocol_family = 2);

  ABSL_MUST_USE_RESULT error_code Bind(const struct sockaddr* bind_addr, unsigned addr_len);
  ABSL_MUST_USE_RESULT error_code Chmod(const char* path, mode_t permissions);
  ABSL_MUST_USE_RESULT error_code Listen(unsigned backlog);

  // Listens on all interfaces. If port is 0 then a random available port is chosen
  // by the OS.
  ABSL_MUST_USE_RESULT error_code Listen(uint16_t port, unsigned backlog);

  // Listen on UDS socket. Must be created with Create(AF_UNIX) first.
  ABSL_MUST_USE_RESULT error_code ListenUDS(const char* path, mode_t permissions, unsigned backlog);

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

  //! Subsribes to one-shot poll. event_mask is a mask of POLLXXX values.
  //! When and an event occurs, the cb will be called with the mask of actual events
  //! that trigerred it.
  //! Returns: handle id that can be used to cancel the poll request (see CancelPoll below).
  virtual uint32_t PollEvent(uint32_t event_mask, std::function<void(uint32_t)> cb) = 0;

  //! Cancels the poll event. id must be the id returned by PollEvent function.
  //! Returns 0 if cancellation ocurred, or ENOENT, EALREADY if poll has not been found or
  //! in process of completing.
  virtual uint32_t CancelPoll(uint32_t id) = 0;

  bool IsUDS() const {
    return fd_ & IS_UDS;
  }

  // Whether it was registered with io_uring engine.
  bool IsDirect() const {
    return fd_ & REGISTER_FD;
  }

 protected:
  LinuxSocketBase(int fd, ProactorBase* pb)
      : FiberSocketBase(pb), fd_(fd > 0 ? fd << kFdShift : fd) {
  }

  enum {
    IS_SHUTDOWN = 0x1,
    IS_UDS = 0x2,
    REGISTER_FD = 0x4,
  };

  // kFdShift low bits are used for masking the state of fd.
  // gives me 256M descriptors.
  int32_t fd_;
};

#if 0
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
#endif

}  // namespace util

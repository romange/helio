// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fiber_socket_base.h"

#include <netinet/in.h>
#include <poll.h>

#include <boost/fiber/context.hpp>

#include "base/logging.h"
#include "base/stl_util.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {

using namespace std;
using io::Result;

namespace {

inline ssize_t posix_err_wrap(ssize_t res, FiberSocketBase::error_code* ec) {
  if (res == -1) {
    *ec = FiberSocketBase::error_code(errno, std::system_category());
  } else if (res < 0) {
    LOG(WARNING) << "Bad posix error " << res;
  }
  return res;
}

}  // namespace

void FiberSocketBase::SetProactor(ProactorBase* p) {
  if (p == proactor_)
    return;

  if (proactor_) {  // migration path
    OnResetProactor();
    proactor_ = nullptr;
  }
  proactor_ = p;

  OnSetProactor();
}

Result<size_t> FiberSocketBase::Recv(const iovec* ptr, size_t len) {
  CHECK_GT(len, 0U);

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;

  return RecvMsg(msg, 0);
}

LinuxSocketBase::~LinuxSocketBase() {
  int fd = native_handle();

  if (fd > -1) {
    LOG(WARNING) << "Socket was not closed properly, closing file descriptor";
    int res = close(fd);
    LOG_IF(WARNING, res == -1) << "Error closing socket " << strerror(errno);
  }
}

error_code LinuxSocketBase::Create(unsigned short pfamily) {
  DCHECK_EQ(fd_, -1);

  error_code ec;

  int fd = socket(pfamily, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (posix_err_wrap(fd, &ec) < 0)
    return ec;

  fd_ = fd << 3;
  if (pfamily == AF_UNIX) {
    fd_ |= IS_UDS;
  }
  return ec;
}

error_code LinuxSocketBase::Listen(uint16_t port, unsigned backlog) {
  sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  return Listen((struct sockaddr*)&server_addr, sizeof(server_addr), backlog);
}

error_code LinuxSocketBase::ListenUDS(const char* path, unsigned backlog) {
  DCHECK(fd_ & IS_UDS);

  struct sockaddr_un addr;
  size_t len = strlen(path);

  if (len + 1 >= sizeof(addr.sun_path))
    return make_error_code(errc::filename_too_long);

  addr.sun_family = AF_UNIX;
  memcpy(addr.sun_path, path, len);
  addr.sun_path[len] = 0;

  return Listen((struct sockaddr*)&addr, sizeof(addr), backlog);
}

error_code LinuxSocketBase::Listen(const struct sockaddr* bind_addr, unsigned addr_len,
                                   unsigned backlog) {
  error_code ec;
  if (fd_ == -1) {
    ec = Create();
    if (ec)
      return ec;
  }

  int fd = native_handle();

  int res = bind(fd, bind_addr, addr_len);
  if (posix_err_wrap(res, &ec) < 0) {
    close(fd);
    fd_ = -1;
    return ec;
  }

  VSOCK(1) << "Listening";

  res = posix_err_wrap(listen(fd, backlog), &ec);
  if (posix_err_wrap(res, &ec) < 0) {
    close(fd);
    fd_ = -1;
    return ec;
  }

  OnSetProactor();
  return ec;
}

auto LinuxSocketBase::Shutdown(int how) -> error_code {
  CHECK_GE(fd_, 0);

  // If we shutdown and then try to Send/Recv - the call will stall since no data
  // is sent/received. Therefore we remember the state to allow consistent API experience.
  error_code ec;
  if (fd_ & IS_SHUTDOWN)
    return ec;

  int fd = native_handle();

  posix_err_wrap(::shutdown(fd, how), &ec);
  fd_ |= IS_SHUTDOWN;  // Enter shutdown state unrelated to the success of the call.

  return ec;
}

auto LinuxSocketBase::LocalEndpoint() const -> endpoint_type {
  endpoint_type endpoint;

  if (fd_ < 0)
    return endpoint;

  DCHECK_EQ(0, fd_ & IS_UDS);
  DCHECK_EQ(0, fd_ & REGISTER_FD);

  socklen_t addr_len = endpoint.capacity();
  error_code ec;

  posix_err_wrap(::getsockname(native_handle(), (sockaddr*)endpoint.data(), &addr_len), &ec);
  CHECK(!ec) << ec << "/" << ec.message() << " while running getsockname";

  endpoint.resize(addr_len);

  return endpoint;
}

auto LinuxSocketBase::RemoteEndpoint() const -> endpoint_type {
  endpoint_type endpoint;
  DCHECK_GE(fd_, 0);
  DCHECK_EQ(0, fd_ & IS_UDS);
  DCHECK_EQ(0, fd_ & REGISTER_FD);

  socklen_t addr_len = endpoint.capacity();
  error_code ec;

  if (getpeername(native_handle(), (sockaddr*)endpoint.data(), &addr_len) == 0)
    endpoint.resize(addr_len);

  return endpoint;
}

}  // namespace util

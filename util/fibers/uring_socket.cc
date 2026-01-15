// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/uring_socket.h"

#include <netinet/in.h>
#include <poll.h>

#include "base/logging.h"
#include "base/stl_util.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {
namespace fb2 {

using namespace std;
using IoResult = UringProactor::IoResult;
using nonstd::make_unexpected;

namespace {

// Disable direct fd for sockets due to https://github.com/axboe/liburing/issues/1192
constexpr bool kEnableDirect = false;

}  // namespace

UringSocket::UringSocket(int fd, Proactor* p) : LinuxSocketBase(fd, p), flags_(0) {
  if (p) {
    // This flag has a clear positive impact of the CPU usage for server side sockets.
    // It also has a negative impact on client side sockets - not sure what the reason is but
    // it was consistently reproducible with echo_server.
    has_pollfirst_ = p->HasPollFirst();
  }
}

UringSocket::~UringSocket() {
  DCHECK_LT(fd_, 0) << "Socket must have been closed explicitly.";
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

error_code UringSocket::Create(unsigned short protocol_family) {
  error_code ec = LinuxSocketBase::Create(protocol_family);
  if (ec) {
    return ec;
  }

  UringProactor* proactor = GetProactor();
  CHECK(proactor && is_direct_fd_ == 0);
  DCHECK(proactor->InMyThread());

  if (kEnableDirect && proactor->HasDirectFD()) {
    int source_fd = ShiftedFd();  // linux fd.
    unsigned direct_fd = proactor->RegisterFd(source_fd);
    if (direct_fd != UringProactor::kInvalidDirectFd) {
      // encode back the id we got.
      UpdateDfVal(direct_fd);
      is_direct_fd_ = 1;
    }
  }
  return ec;
}

auto UringSocket::Close() -> error_code {
  if (fd_ < 0)
    return {};

  // We may close a socket that was never registered with a proactor.
  DCHECK(!proactor() || proactor()->InMyThread());
  DVSOCK(1) << "Closing socket";

  if (error_cb_wrapper_) {
    LOG(DFATAL) << "Error callback still registered in Close";
    ErrorCbRefWrapper::Destroy(error_cb_wrapper_);
    error_cb_wrapper_ = nullptr;
  }

  ResetOnRecvHook();

  int fd;
  if (is_direct_fd_) {
    unsigned direct_fd = ShiftedFd();
    UringProactor* proactor = GetProactor();

    fd = proactor->UnregisterFd(direct_fd);
    if (fd < 0) {
      LOG(WARNING) << "Error unregistering fd " << direct_fd;
      return {};
    }
    is_direct_fd_ = 0;
  } else {
    fd = native_handle();
  }

  error_code ec;
  posix_err_wrap(::close(fd), &ec);
  fd_ = -1;

  return ec;
}

auto UringSocket::Accept() -> AcceptResult {
  CHECK(proactor());

  error_code ec;

  int fd = native_handle();
  VSOCK(2) << "Accept";

  int res = -1;
  while (true) {
    res = accept4(fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      break;
    }

    DCHECK_EQ(-1, res);

    if (errno == EAGAIN) {
      // TODO: to add support for iouring direct file descriptors.
      FiberCall fc(GetProactor());
      fc->PrepPollAdd(ShiftedFd(), POLLIN);
      fc->sqe()->flags |= register_flag();
      IoResult io_res = fc.Get();

      // tcp sockets set POLLERR but UDS set POLLHUP.
      if ((io_res & (POLLERR | POLLHUP)) != 0) {
        return MakeUnexpected(errc::connection_aborted);
      }

      continue;
    }

    posix_err_wrap(res, &ec);
    return make_unexpected(ec);
  }

  UringSocket* fs = new UringSocket{nullptr};
  fs->fd_ = (res << kFdShift) | (fd_ & kInheritedFlags);
  fs->has_pollfirst_ = has_pollfirst_;

  return fs;
}

error_code UringSocket::Connect(const endpoint_type& ep, std::function<void(int)> on_pre_connect) {
  CHECK_EQ(fd_, -1);
  CHECK(proactor() && proactor()->InMyThread());

  error_code ec;

  int fd = socket(ep.protocol().family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (posix_err_wrap(fd, &ec))
    return ec;

  VSOCK(1) << "Connect [" << fd << "] " << ep.address().to_string() << ":" << ep.port();

  UringProactor* proactor = GetProactor();

  // TODO: support direct descriptors. For now client sockets always use regular linux fds.
  fd_ = fd << kFdShift;

  if (on_pre_connect) {
    on_pre_connect(fd);
  }

  FiberCall fc(proactor, timeout());
  fc->PrepConnect(fd, (const sockaddr*)ep.data(), ep.size());
  IoResult io_res = fc.Get();

  if (io_res < 0) {  // In that case connect returns -errno.
    ec = error_code(-io_res, system_category());
  }
  return ec;
}

auto UringSocket::WriteSome(const iovec* ptr, uint32_t len) -> Result<size_t> {
  DCHECK_GT(len, 0U);
  DCHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return MakeUnexpected(errc::connection_aborted);
  }

  int fd = ShiftedFd();
  Proactor* proactor = GetProactor();
  DCHECK(ProactorBase::me() == proactor);

  ssize_t res = 0;
  VSOCK(2) << "WriteSome [" << fd << "] " << len;

  if (len == 1) {
    while (true) {
      FiberCall fc(proactor, timeout());
      fc->PrepSend(fd, ptr->iov_base, ptr->iov_len, MSG_NOSIGNAL);
      fc->sqe()->flags |= register_flag();
      res = fc.Get();  // Interrupt point
      if (res >= 0) {
        return res;  // Fastpath
      }

      DVSOCK(2) << "Got " << res;
      res = -res;
      if (res == EAGAIN)  // EAGAIN can happen in case of CQ overflow.
        continue;

      break;
    }
  } else {  // len > 1
    msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = const_cast<iovec*>(ptr);
    msg.msg_iovlen = len;

    while (true) {
      FiberCall fc(proactor, timeout());
      fc->PrepSendMsg(fd, &msg, MSG_NOSIGNAL);
      fc->sqe()->flags |= register_flag();
      res = fc.Get();  // Interrupt point
      if (res >= 0) {
        return res;  // Fastpath
      }

      DVSOCK(2) << "Got " << res;
      res = -res;
      if (res == EAGAIN)  // EAGAIN can happen in case of CQ overflow.
        continue;

      break;
    };
  }

  error_code ec(res, system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return make_unexpected(std::move(ec));
}

void UringSocket::AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  if (fd_ & IS_SHUTDOWN) {
    cb(MakeUnexpected(errc::connection_aborted));
    return;
  }

  // this time we can not store it on stack.
  //
  msghdr* msg = new msghdr;
  memset(msg, 0, sizeof(msghdr));
  msg->msg_iov = const_cast<iovec*>(v);
  msg->msg_iovlen = len;

  int fd = native_handle();
  Proactor* proactor = GetProactor();
  DCHECK(ProactorBase::me() == proactor);

  auto mycb = [msg, cb = std::move(cb)](detail::FiberInterface*, Proactor::IoResult res,
                                        uint32_t flags, uint32_t) {
    delete msg;

    if (res >= 0) {
      cb(res);
      return;
    }

    cb(make_unexpected(error_code{-res, generic_category()}));
  };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(mycb));
  se.PrepSendMsg(fd, msg, MSG_NOSIGNAL);
  se.sqe()->flags |= register_flag();
}

void UringSocket::AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  if (fd_ & IS_SHUTDOWN) {
    cb(MakeUnexpected(errc::connection_aborted));
    return;
  }

  msghdr* msg = new msghdr;
  memset(msg, 0, sizeof(msghdr));
  msg->msg_iov = const_cast<iovec*>(v);
  msg->msg_iovlen = len;

  int fd = ShiftedFd();
  Proactor* proactor = GetProactor();
  DCHECK(ProactorBase::me() == proactor);

  auto mycb = [msg, cb = std::move(cb)](detail::FiberInterface*, Proactor::IoResult res,
                                        uint32_t flags, uint32_t) {
    delete msg;

    if (res >= 0) {
      cb(res);
      return;
    }

    cb(make_unexpected(error_code{-res, generic_category()}));
  };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(mycb));
  se.PrepRecvMsg(fd, msg, 0);
  se.sqe()->flags |= register_flag();
}

auto UringSocket::RecvMsg(const msghdr& msg, int flags) -> Result<size_t> {
  DCHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return MakeUnexpected(errc::connection_aborted);
  }
  int fd = ShiftedFd();
  Proactor* p = GetProactor();
  DCHECK(ProactorBase::me() == p);

  ssize_t res;
  VSOCK(2) << "RecvMsg [" << fd << "]";

  while (true) {
    FiberCall fc(p, timeout());
    fc->PrepRecvMsg(fd, &msg, flags);
    fc->sqe()->flags |= register_flag();

    // As described in "io_uring_prep_recv(3)"
    if (has_pollfirst_ && !has_recv_data_) {
      fc->sqe()->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }

    res = fc.Get();

    if (res >= 0) {
      return res;
    }
    DVSOCK(2) << "Got " << res;

    res = -res;
    // EAGAIN can happen in case of CQ overflow.
    if (res == EAGAIN && (flags & MSG_DONTWAIT) == 0) {
      continue;
    }

    break;
  }

  error_code ec(res, system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return make_unexpected(std::move(ec));
}

io::Result<size_t> UringSocket::Recv(const io::MutableBytes& mb, int flags) {
  int fd = ShiftedFd();
  Proactor* p = GetProactor();
  DCHECK(ProactorBase::me() == p);

  VSOCK(2) << "Recv [" << fd << "], flags: " << flags;
  ssize_t res;
  while (true) {
    FiberCall fc(p, timeout());
    fc->PrepRecv(fd, mb.data(), mb.size(), flags);
    fc->sqe()->flags |= register_flag();
    if (has_pollfirst_ && !has_recv_data_) {
      fc->sqe()->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }
    res = fc.Get();

    if (res >= 0) {
      has_recv_data_ = (fc.flags() & IORING_CQE_F_SOCK_NONEMPTY) ? 1 : 0;
      DVSOCK(2) << "Received " << res << " bytes " << ", has_more " << has_recv_data_;
      return res;
    }
    DVSOCK(2) << "Got " << res;

    res = -res;
    // EAGAIN can happen in case of CQ overflow.
    if (res == EAGAIN && (flags & MSG_DONTWAIT) == 0) {
      continue;
    }

    break;
  }

  error_code ec(res, system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return make_unexpected(std::move(ec));
}

void UringSocket::RegisterOnErrorCb(std::function<void(uint32_t)> cb) {
  CHECK(!error_cb_wrapper_);
  DCHECK(IsOpen());

  uint32_t event_mask = POLLERR | POLLHUP;

  Proactor* p = GetProactor();
  error_cb_wrapper_ = ErrorCbRefWrapper::New(std::move(cb));
  auto se_cb = [data = error_cb_wrapper_](detail::FiberInterface*, Proactor::IoResult res,
                                          uint32_t flags, uint32_t) {
    auto cb = std::move(data->cb);
    ErrorCbRefWrapper::Destroy(data);
    if (res < 0) {
      res = -res;
      LOG_IF(WARNING, res != ECANCELED) << "Unexpected error result " << res;
    } else if (cb) {
      cb(res);
    }
  };

  SubmitEntry se = p->GetSubmitEntry(std::move(se_cb));
  se.PrepPollAdd(ShiftedFd(), event_mask);
  se.sqe()->flags |= register_flag();
  error_cb_wrapper_->error_cb_id = se.sqe()->user_data;
}

void UringSocket::CancelOnErrorCb() {
  if (!error_cb_wrapper_)
    return;

  FiberCall fc(GetProactor());
  fc->PrepPollRemove(error_cb_wrapper_->error_cb_id);

  ErrorCbRefWrapper::Destroy(error_cb_wrapper_);
  error_cb_wrapper_ = nullptr;

  IoResult io_res = fc.Get();
  if (io_res < 0) {
    io_res = -io_res;

    // The callback could have been already called or being in process of calling.
    LOG_IF(WARNING, io_res != ENOENT && io_res != EALREADY)
        << "Error canceling error cb " << io_res;
  }
}

void UringSocket::RegisterOnRecv(OnRecvCb cb) {
  DCHECK(IsOpen());
  CHECK_EQ(recv_poll_id_, 0u);
  CHECK(!register_recv_multishot_);

  Proactor* proactor = GetProactor();

  int fd = ShiftedFd();
  if (enable_multi_shot_) {
    auto recv_cb = [this, cb = std::move(cb)](detail::FiberInterface*, UringProactor::IoResult res,
                                              uint32_t flags, uint32_t bufring_id) {
      UringProactor* up = static_cast<UringProactor*>(ProactorBase::me());

      if (flags & IORING_CQE_F_BUFFER) {
        CHECK_GT(res, 0);

        uint16_t bufid = UringProactor::BufRingIdFromFlags(flags);
        bool incremental = (flags & IORING_CQE_F_BUF_MORE) != 0;
        DVLOG(2) << "Received " << bufid << " " << res << " " << incremental;

        while (res > 0) {
          auto [buf_ptr, segment_len] = up->BufRingGetBufPtr(bufring_id, bufid, res);
          DVLOG(2) << "Notifying " << bufid << " " << res << " " << segment_len;
          cb(RecvNotification{io::MutableBytes(buf_ptr, segment_len)});

          // Returns next bufid that comprises the bundle.
          bufid = up->BufRingTrackRecvCompletion(bufring_id, segment_len, bufid, incremental);
          res -= segment_len;
          incremental = true;  // after the first segment in a bundle we are always incremental.
        }

        if ((flags & IORING_CQE_F_MORE) == 0) {
          // Kernel stopped multishot recv for some reason. Most likely due to lack of buffers.
          // We have two options here:
          // a) to reregister the recv using provided bufring_id.
          // b) to fallback to epoll registration.
          // For now we go with a).
          this->RegisterOnRecv(std::move(cb));  // re-register for next notifications.
        }
      } else {
        CHECK_LE(res, 0);
        res = -res;
        DCHECK_EQ(0u, flags & IORING_CQE_F_MORE);

        if (res == ENOBUFS) {
          // TODO: we should reregister the recv here, there is no need to for the
          // app to know about it. There are two options:
          // a) to fallback to epoll or
          // b) to reissue recv with buffer select.
          LOG(FATAL) << "TBD";
        }

        // When we get an error, the multishot stops automatically.
        // we align by resetting register_recv_multishot_ and propagate the error.
        this->register_recv_multishot_ = 0;
        if (res == 0) {
          // connection closed.
          cb(RecvNotification{false});
        } else {
          cb(RecvNotification{error_code(-res, system_category())});
        }
      }
    };

    fb2::SubmitEntry entry = proactor->GetSubmitEntry(std::move(recv_cb), bufring_id_);
    entry.PrepRecv(fd, nullptr, 0, 0);
    auto& sqe = *entry.sqe();

    sqe.flags |= (register_flag() | IOSQE_BUFFER_SELECT);
    sqe.buf_group = bufring_id_;
    uint16_t prio_flags = (IORING_RECV_MULTISHOT | IORING_RECVSEND_POLL_FIRST);

    if (proactor->HasBundleSupport()) {
      prio_flags |= IORING_RECVSEND_BUNDLE;
    }
    sqe.ioprio |= prio_flags;
  } else {
    auto epoll_cb = [cb = std::move(cb)](uint32_t mask) {
      bool is_valid = (mask & (POLLERR | POLLHUP)) == 0;
      cb(RecvNotification{is_valid});
    };

    recv_poll_id_ = proactor->EpollAdd(fd, std::move(epoll_cb), POLLIN);
  }
}

void UringSocket::ResetOnRecvHook() {
  if (recv_poll_id_) {
    Proactor* p = GetProactor();
    p->EpollDel(recv_poll_id_);
    recv_poll_id_ = 0;
  } else if (register_recv_multishot_) {
    register_recv_multishot_ = 0;
    int flags = is_direct_fd_ ? IORING_ASYNC_CANCEL_FD_FIXED : IORING_ASYNC_CANCEL_FD;

    int res = GetProactor()->CancelRequests(ShiftedFd(), flags);
    LOG_IF(WARNING, res < 0) << "Error canceling multishot " << -res;
  }
}

auto UringSocket::native_handle() const -> native_handle_type {
  int fd = ShiftedFd();

  if (is_direct_fd_) {
    fd = GetProactor()->TranslateDirectFd(fd);
  }
  return fd;
}

void UringSocket::OnSetProactor() {
  UringProactor* proactor = GetProactor();

  if (kEnableDirect && proactor->HasDirectFD() && is_direct_fd_ == 0 && fd_ >= 0) {
    // Using direct descriptors has consistent positive impact on CPU usage of the server.
    // Checked with echo_server with server side sockets.
    int source_fd = ShiftedFd();  // linux fd.
    unsigned direct_fd = proactor->RegisterFd(source_fd);
    if (direct_fd != UringProactor::kInvalidDirectFd) {
      // encode back the id we got.
      UpdateDfVal(direct_fd);
      is_direct_fd_ = 1;
    }
  }
}

void UringSocket::OnResetProactor() {
  DCHECK(proactor()->InMyThread());
  if (is_direct_fd_) {
    UringProactor* proactor = GetProactor();
    unsigned direct_fd = ShiftedFd();
    int fd = proactor->UnregisterFd(direct_fd);
    if (fd < 0) {
      LOG(WARNING) << "Error unregistering fd " << direct_fd;
    }
    UpdateDfVal(fd);
    is_direct_fd_ = 0;
  }
}

}  // namespace fb2
}  // namespace util

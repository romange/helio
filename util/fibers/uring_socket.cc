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

inline ssize_t posix_err_wrap(ssize_t res, UringSocket::error_code* ec) {
  if (res == -1) {
    *ec = UringSocket::error_code(errno, system_category());
  } else if (res < 0) {
    LOG(WARNING) << "Bad posix error " << res;
  }
  return res;
}

auto Unexpected(std::errc e) {
  return make_unexpected(make_error_code(e));
}

constexpr uint8_t kHeapType = 1;
constexpr uint8_t kBufRingType = 2;

}  // namespace

bool UringSocket::MultiShot::DecRef() {
  if (--refcnt > 0)
    return false;

  delete this;
  return true;
}

void UringSocket::MultiShot::Activate(int fd, uint16_t bufring_id, uint8_t flags,
                                      UringProactor* proactor) {
  if (refcnt > 1)
    return;

  auto cb = [this](detail::FiberInterface* current, IoResult res, uint32_t flags,
                   uint32_t bufring_id) {
    DVLOG(2) << "Multishot completion " << res << " flags: " << flags;
    UringProactor* proactor = static_cast<UringProactor*>(ProactorBase::me());

    if ((flags & IORING_CQE_F_MORE) == 0) {
      if (DecRef())  // Last reference.
        return;

      // Assumption.
      CHECK_EQ(flags & IORING_CQE_F_BUFFER, 0u);
      CHECK_LE(res, 0);
      err_no = -res;
      error_raised = 1;
    } else {
      CHECK(flags & IORING_CQE_F_BUFFER);
      CHECK_GT(res, 0);

      proactor->EnqueueMultishotCompletion(bufring_id, res, flags, &tail);
      DVLOG(1) << "Multishot tail " << tail << " " << flags;

      DCHECK_NE(tail, UringProactor::kMultiShotUndef);
    }

    if (poll_pending) {
      ActivateSameThread(current, poll_pending);
      poll_pending = nullptr;
    }
  };

  fb2::SubmitEntry entry = proactor->GetSubmitEntry(std::move(cb), bufring_id);
  entry.PrepRecv(fd, nullptr, 0, 0);
  auto& sqe = *entry.sqe();

  sqe.flags |= (flags | IOSQE_BUFFER_SELECT);
  sqe.buf_group = bufring_id;
  sqe.ioprio |= (IORING_RECV_MULTISHOT | IORING_RECVSEND_POLL_FIRST);

  ++refcnt;
}

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

  DCHECK(proactor());
  DCHECK(proactor()->InMyThread());
  DVSOCK(1) << "Closing socket";
  CancelMultiShot();

  int fd;
  if (is_direct_fd_) {
    UringProactor* proactor = GetProactor();
    unsigned direct_fd = ShiftedFd();
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
        return Unexpected(errc::connection_aborted);
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

auto UringSocket::Connect(const endpoint_type& ep, std::function<void(int)> on_pre_connect)
    -> error_code {
  CHECK_EQ(fd_, -1);
  CHECK(proactor() && proactor()->InMyThread());

  error_code ec;

  int fd = socket(ep.protocol().family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (posix_err_wrap(fd, &ec) < 0)
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
  CHECK(proactor());
  CHECK_GT(len, 0U);
  CHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return Unexpected(errc::connection_aborted);
  }

  int fd = ShiftedFd();
  Proactor* p = GetProactor();
  ssize_t res = 0;
  VSOCK(2) << "WriteSome [" << fd << "] " << len;

  if (len == 1) {
    while (true) {
      FiberCall fc(p, timeout());
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
      FiberCall fc(p, timeout());
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
    cb(Unexpected(errc::connection_aborted));
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
    cb(Unexpected(errc::connection_aborted));
    return;
  }

  msghdr* msg = new msghdr;
  memset(msg, 0, sizeof(msghdr));
  msg->msg_iov = const_cast<iovec*>(v);
  msg->msg_iovlen = len;

  int fd = ShiftedFd();
  Proactor* proactor = GetProactor();
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
  CHECK(proactor());
  CHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return Unexpected(errc::connection_aborted);
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

    if (res > 0) {
      return res;
    }
    DVSOCK(2) << "Got " << res;

    res = -res;
    // EAGAIN can happen in case of CQ overflow.
    if (res == EAGAIN && (flags & MSG_DONTWAIT) == 0) {
      continue;
    }

    if (res == 0)
      res = ECONNABORTED;
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

    if (res > 0) {
      has_recv_data_ = (fc.flags() & IORING_CQE_F_SOCK_NONEMPTY) ? 1 : 0;
      DVSOCK(2) << "Received " << res << " bytes";
      return res;
    }
    DVSOCK(2) << "Got " << res;

    res = -res;
    // EAGAIN can happen in case of CQ overflow.
    if (res == EAGAIN && (flags & MSG_DONTWAIT) == 0) {
      continue;
    }

    if (res == 0)
      res = ECONNABORTED;
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

auto UringSocket::native_handle() const -> native_handle_type {
  int fd = ShiftedFd();

  if (is_direct_fd_) {
    fd = GetProactor()->TranslateDirectFd(fd);
  }
  return fd;
}

unsigned UringSocket::RecvProvided(unsigned buf_len, ProvidedBuffer* dest) {
  DCHECK_GT(buf_len, 0u);

  int fd = ShiftedFd();
  Proactor* p = GetProactor();
  DCHECK(ProactorBase::me() == p);

  if (multishot_) {
    // Multishot mode.
    unsigned res = 0;
    if (!multishot_->error_raised && !multishot_->HasBuffers()) {
      DCHECK(multishot_->poll_pending == nullptr);
      DCHECK_GT(multishot_->refcnt, 1);
      multishot_->poll_pending = detail::FiberActive();
      multishot_->poll_pending->Suspend();
    }

    while (multishot_->HasBuffers()) {
      UringProactor::MultiShotResult result =
          GetProactor()->PullMultiShotCompletion(bufring_id_, &multishot_->tail);
      auto& pbuf = dest[res++];
      pbuf.buffer = result;
      pbuf.allocated = 0;
      pbuf.err_no = 0;
      pbuf.cookie = kBufRingType;
      if (res == buf_len) {
        return res;
      }
    };
    if (res > 0)
      return res;

    CHECK(multishot_->error_raised);

    dest[0].SetError(multishot_->err_no == 0 ? ECONNABORTED : multishot_->err_no);
    CHECK(multishot_->DecRef());  // Last reference, we disable multishot.
    multishot_ = nullptr;
    return 1;
  }

  // Non-multishot mode.
  FiberCall fc(p, timeout());

  fc->PrepRecv(fd, nullptr, 0, 0);
  fc->sqe()->flags |= (register_flag() | IOSQE_BUFFER_SELECT);
  fc->sqe()->buf_group = bufring_id_;
  if (has_pollfirst_ && !has_recv_data_) {
    fc->sqe()->ioprio |= IORING_RECVSEND_POLL_FIRST;
  }
  ssize_t res = fc.Get();

  dest[0].cookie = kBufRingType;
  dest[0].allocated = 0;

  if (res > 0) {
    uint32_t flags = fc.flags();
    DCHECK(IORING_CQE_F_BUFFER & flags);

    has_recv_data_ = flags & IORING_CQE_F_SOCK_NONEMPTY ? 1 : 0;
    DVSOCK(2) << "Received " << res << " bytes";
    uint8_t* start = p->GetBufRingPtr(bufring_id_, flags >> IORING_CQE_BUFFER_SHIFT);
    dest[0].buffer = io::MutableBytes{start, static_cast<size_t>(res)};
    dest[0].err_no = 0;

    if (multishot_) {
      multishot_->Activate(fd, bufring_id_, register_flag(), p);
    }
    return 1;
  }
  res = -res;

  if (res == 0)
    res = ECONNABORTED;
  else if (res == ENOBUFS)
    has_recv_data_ = 1;
  dest[0].SetError(res);

  return 1;
}

void UringSocket::ReturnProvided(const ProvidedBuffer& pbuf) {
  DCHECK(!pbuf.buffer.empty());
  Proactor* p = GetProactor();
  if (pbuf.cookie == kBufRingType) {
    p->ReplenishBuffers(bufring_id_, pbuf.buffer);
  } else {
    DCHECK_EQ(pbuf.cookie, kHeapType);
    p->DeallocateBuffer({const_cast<uint8_t*>(pbuf.buffer.data()), pbuf.allocated});
  }
}

void UringSocket::SendProvided(uint16_t buf_gid, io::AsyncProgressCb cb) {
  Proactor* p = GetProactor();
  auto io_cb = [cb = std::move(cb)](detail::FiberInterface* current, UringProactor::IoResult res,
                                    uint32_t flags, uint32_t ) {
    DVLOG(2) << "SendProvided completion " << res << " flags: " << flags;
    if (res >= 0) {
      cb(res);
    } else {
      cb(make_unexpected(error_code{-res, generic_category()}));
    }
  };

  int fd = ShiftedFd();
  SubmitEntry se = p->GetSubmitEntry(io_cb);
  se.PrepSend(fd, nullptr, 0, MSG_NOSIGNAL);
  se.sqe()->flags |= (register_flag() | IOSQE_BUFFER_SELECT);
  se.sqe()->buf_group = buf_gid;
}

void UringSocket::EnableRecvMultishot() {
  if (!multishot_) {
    multishot_ = new MultiShot();
  }

  // noop if already activated.
  multishot_->Activate(ShiftedFd(), bufring_id_, register_flag(), GetProactor());
}

void UringSocket::CancelMultiShot() {
  if (!multishot_)
    return;

  if (!multishot_->DecRef()) {  // Still has references because of the armed callback.
    int flags = is_direct_fd_ ? IORING_ASYNC_CANCEL_FD_FIXED : IORING_ASYNC_CANCEL_FD;
    GetProactor()->CancelRequests(ShiftedFd(), flags);
  }
  multishot_ = nullptr;
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

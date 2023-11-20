// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/tls/tls_socket.h"

#include <openssl/err.h>

#include <algorithm>

#include "base/logging.h"
#include "util/tls/tls_engine.h"

namespace util {
namespace tls {

using namespace std;
using nonstd::make_unexpected;

namespace {

class error_category : public std::error_category {
 public:
  const char* name() const noexcept final {
    return "async.tls";
  }

  string message(int ev) const final;

  error_condition default_error_condition(int ev) const noexcept final;

  bool equivalent(int ev, const error_condition& condition) const noexcept final {
    return condition.value() == ev && &condition.category() == this;
  }

  bool equivalent(const error_code& error, int ev) const noexcept final {
    return error.value() == ev && &error.category() == this;
  }
};

string error_category::message(int ev) const {
  char buf[256];
  ERR_error_string_n(ev, buf, sizeof(buf));
  return buf;
}

error_condition error_category::default_error_condition(int ev) const noexcept {
  return error_condition{ev, *this};
}

error_category tls_category;

inline error_code SSL2Error(unsigned long err) {
  CHECK_LT(err, unsigned(INT_MAX));

  return error_code{int(err), tls_category};
}

}  // namespace

TlsSocket::TlsSocket(std::unique_ptr<FiberSocketBase> next)
    : FiberSocketBase(next ? next->proactor() : nullptr), next_sock_(std::move(next)) {
}

TlsSocket::TlsSocket(FiberSocketBase* next) : TlsSocket(std::unique_ptr<FiberSocketBase>(next)) {
}

TlsSocket::~TlsSocket() {
}

void TlsSocket::InitSSL(SSL_CTX* context) {
  CHECK(!engine_);
  engine_.reset(new Engine{context});
}

auto TlsSocket::Shutdown(int how) -> error_code {
  DCHECK(engine_);
  return next_sock_->Shutdown(how);
}

auto TlsSocket::Accept() -> AcceptResult {
  DCHECK(engine_);

  while (true) {
    Engine::OpResult op_result = engine_->Handshake(Engine::SERVER);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    error_code ec = MaybeSendOutput();
    if (ec) {
      return make_unexpected(ec);
    }

    int op_val = *op_result;

    if (op_val >= 0) {  // Shutdown or empty read/write may return 0.
      break;
    }
    if (op_val == Engine::EOF_STREAM) {
      return make_unexpected(make_error_code(errc::connection_reset));
    }
    if (op_val == Engine::NEED_READ_AND_MAYBE_WRITE) {
      ec = HandleRead();
      if (ec)
        return make_unexpected(ec);
    }
  }

  return nullptr;
}

auto TlsSocket::Connect(const endpoint_type& endpoint) -> error_code {
  DCHECK(engine_);
  auto io_result = engine_->Handshake(Engine::HandshakeType::CLIENT);
  if (!io_result.has_value()) {
    return std::error_code(io_result.error(), std::system_category());
  }

  // If the socket is already open, we should not call connect on it
  if (IsOpen()) {
    return {};
  }

  return next_sock_->Connect(endpoint);
}

auto TlsSocket::Close() -> error_code {
  DCHECK(engine_);
  next_sock_->Close();

  return error_code{};
}

io::Result<size_t> TlsSocket::RecvMsg(const msghdr& msg, int flags) {
  DCHECK(engine_);
  DCHECK_GT(size_t(msg.msg_iovlen), 0u);

  DLOG_IF(INFO, flags) << "Flags argument is not supported " << flags;

  auto* io = msg.msg_iov;
  size_t io_len = msg.msg_iovlen;

  Engine::MutableBuffer dest{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
  size_t read_total = 0;

  while (true) {
    DCHECK(!dest.empty());

    size_t read_len = std::min(dest.size(), size_t(INT_MAX));

    Engine::OpResult op_result = engine_->Read(dest.data(), read_len);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    error_code ec = MaybeSendOutput();
    if (ec) {
      return make_unexpected(ec);
    }

    int op_val = *op_result;

    if (op_val > 0) {
      read_total += op_val;

      if (size_t(op_val) == read_len) {
        if (size_t(op_val) < dest.size()) {
          dest.remove_prefix(op_val);
        } else {
          ++io;
          --io_len;
          if (io_len == 0)
            break;
          dest = Engine::MutableBuffer{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
        }
        continue;  // We read everything we asked for - lets retry.
      }
      break;
    }

    if (read_total)  // if we read something lets return it before we handle other states.
      break;

    if (op_val == Engine::EOF_STREAM) {
      return make_unexpected(make_error_code(errc::connection_reset));
    }

    if (op_val == Engine::NEED_READ_AND_MAYBE_WRITE) {
      ec = HandleRead();
      if (ec)
        return make_unexpected(ec);
    }
  }
  return read_total;
}

io::Result<size_t> TlsSocket::Recv(const io::MutableBytes& mb, int flags) {
  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  iovec vec[1];

  msg.msg_iov = vec;
  msg.msg_iovlen = 1;
  vec[0].iov_base = mb.data();
  vec[0].iov_len = mb.size();
  return RecvMsg(msg, flags);
}

io::Result<size_t> TlsSocket::WriteSome(const iovec* ptr, uint32_t len) {
  // Chosen to be sufficiently smaller than the usual MTU (1500) and a multiple of 16.
  // IP - max 24 bytes. TCP - max 60 bytes. TLS - max 21 bytes.
  constexpr size_t kBufferSize = 1392;
  io::Result<size_t> ec;
  size_t total_sent = 0;

  while (len) {
    if (ptr->iov_len > kBufferSize || len == 1) {
      ec = SendBuffer(Engine::Buffer{reinterpret_cast<uint8_t*>(ptr->iov_base), ptr->iov_len});
      ptr++;
      len--;
    } else {
      alignas(64) uint8_t scratch[kBufferSize];
      size_t buffered_size = 0;
      while (len && (buffered_size + ptr->iov_len) <= kBufferSize) {
        std::memcpy(scratch + buffered_size, ptr->iov_base, ptr->iov_len);
        buffered_size += ptr->iov_len;
        ptr++;
        len--;
      }
      ec = SendBuffer({scratch, buffered_size});
    }
    if (!ec.has_value()) {
      return ec;
    } else {
      total_sent += ec.value();
    }
  }
  return total_sent;
}

io::Result<size_t> TlsSocket::SendBuffer(Engine::Buffer buf) {
  DCHECK(engine_);
  DCHECK_GT(buf.size(), 0u);

  size_t send_total = 0;

  while (true) {
    Engine::OpResult op_result = engine_->Write(buf);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    error_code ec = MaybeSendOutput();
    if (ec) {
      return make_unexpected(ec);
    }

    int op_val = *op_result;

    if (op_val > 0) {
      send_total += op_val;

      if (size_t(op_val) == buf.size()) {
        break;
      } else {
        buf.remove_prefix(op_val);
      }
    }

    if (op_val == Engine::EOF_STREAM) {
      return make_unexpected(make_error_code(errc::connection_reset));
    }

    if (op_val == Engine::NEED_READ_AND_MAYBE_WRITE) {
      ec = HandleRead();
      if (ec)
        return make_unexpected(ec);
    }
  }

  return send_total;
}

// TODO: to implement async functionality.
void TlsSocket::AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) {
  io::Result<size_t> res = WriteSome(v, len);
  cb(res);
}

SSL* TlsSocket::ssl_handle() {
  return engine_ ? engine_->native_handle() : nullptr;
}

auto TlsSocket::MaybeSendOutput() -> error_code {
  // this function is present in both read and write paths.
  // meaning that both of them can be called concurrently from differrent fibers and then
  // race over flushing the output buffer. We use state_ to prevent that.
  if (state_ & WRITE_IN_PROGRESS) {
    return error_code{};
  }

  auto buf_result = engine_->PeekOutputBuf();
  CHECK(buf_result);

  if (!buf_result->empty()) {
    // we do not allow concurrent writes from multiple fibers.
    state_ |= WRITE_IN_PROGRESS;
    io::Result<size_t> write_result = next_sock_->WriteSome(*buf_result);

    // Safe to clear here since the code below is atomic fiber-wise.
    state_ &= ~WRITE_IN_PROGRESS;

    if (!write_result) {
      return write_result.error();
    }
    CHECK_GT(*write_result, 0u);
    engine_->ConsumeOutputBuf(*write_result);
  }

  return error_code{};
}

auto TlsSocket::HandleRead() -> error_code {
  if (state_ & READ_IN_PROGRESS) {
    return error_code{};
  }

  auto mut_buf = engine_->PeekInputBuf();
  state_ |= READ_IN_PROGRESS;
  io::Result<size_t> esz = next_sock_->Recv(mut_buf, 0);
  state_ &= ~READ_IN_PROGRESS;
  if (!esz) {
    return esz.error();
  }

  engine_->CommitInput(*esz);

  return error_code{};
}

TlsSocket::endpoint_type TlsSocket::LocalEndpoint() const {
  return next_sock_->LocalEndpoint();
}

TlsSocket::endpoint_type TlsSocket::RemoteEndpoint() const {
  return next_sock_->RemoteEndpoint();
}

void TlsSocket::RegisterOnErrorCb(std::function<void(uint32_t)> cb) {
  return next_sock_->RegisterOnErrorCb(std::move(cb));
}

void TlsSocket::CancelOnErrorCb() {
  return next_sock_->CancelOnErrorCb();
}


bool TlsSocket::IsUDS() const {
  return next_sock_->IsUDS();
}

TlsSocket::native_handle_type TlsSocket::native_handle() const {
  return next_sock_->native_handle();
}

bool TlsSocket::IsDirect() const {
  return next_sock_->IsDirect();
}

error_code TlsSocket::Create(unsigned short protocol_family) {
  return next_sock_->Create(protocol_family);
}

error_code TlsSocket::Bind(const struct sockaddr* bind_addr, unsigned addr_len) {
  return next_sock_->Bind(bind_addr, addr_len);
}

error_code TlsSocket::Listen(unsigned backlog) {
  return next_sock_->Listen(backlog);
}

error_code TlsSocket::Listen(uint16_t port, unsigned backlog) {
  return next_sock_->Listen(port, backlog);
}

error_code TlsSocket::ListenUDS(const char* path, mode_t permissions, unsigned backlog) {
  return next_sock_->ListenUDS(path, permissions, backlog);
}

void TlsSocket::SetProactor(ProactorBase* p) {
  next_sock_->SetProactor(p);
  FiberSocketBase::SetProactor(p);
}

}  // namespace tls
}  // namespace util

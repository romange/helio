// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/tls/tls_socket.h"

#include <openssl/err.h>

#include <algorithm>

#include "base/logging.h"
#include "util/fibers/fibers.h"
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
  // sanity check that all pending ops are done.
  DCHECK_EQ(state_ & (WRITE_IN_PROGRESS | READ_IN_PROGRESS | SHUTDOWN_IN_PROGRESS), 0);
}

void TlsSocket::InitSSL(SSL_CTX* context, Buffer prefix) {
  CHECK(!engine_);
  engine_.reset(new Engine{context});
  if (!prefix.empty()) {
    Engine::OpResult op_result = engine_->WriteBuf(prefix);
    CHECK(op_result);
    CHECK_EQ(unsigned(*op_result), prefix.size());
  }
}

auto TlsSocket::Shutdown(int how) -> error_code {
  DCHECK(engine_);
  if (state_ & (SHUTDOWN_DONE | SHUTDOWN_IN_PROGRESS)) {
    return {};
  }

  state_ |= SHUTDOWN_IN_PROGRESS;
  Engine::OpResult op_result = engine_->Shutdown();
  if (op_result) {
    // engine_ could send notification messages to the peer.
    std::ignore = MaybeSendOutput();
  }

  // In any case we should also shutdown the underlying TCP socket without relying on the
  // the peer. It could be that when we are in the middle of MaybeSendOutput, and
  // the other fiber calls Close() on this socket. In this case next_sock_ will be closed
  // by the time we reach this line, so we omit calling Shutdown().
  // It's not the best behavior, but it's also not disastrous either, because
  // such interaction happens only during the server shutdown.
  error_code res;
  if (next_sock_->IsOpen()) {
    res = next_sock_->Shutdown(how);
  }
  state_ |= SHUTDOWN_DONE;
  state_ &= ~SHUTDOWN_IN_PROGRESS;

  return res;
}

auto TlsSocket::Accept() -> AcceptResult {
  DCHECK(engine_);

  while (true) {
    Engine::OpResult op_result = engine_->Handshake(Engine::SERVER);

    // it is important to send output (protocol errors) before we return from this function.
    error_code ec = MaybeSendOutput();
    if (ec) {
      return make_unexpected(ec);
    }

    // now check the result of the handshake.
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    int op_val = *op_result;

    if (op_val >= 0) {  // Shutdown or empty read/write may return 0.
      break;
    }

    ec = HandleOp(op_val);
    if (ec)
      return make_unexpected(ec);
  }

  return nullptr;
}

error_code TlsSocket::Connect(const endpoint_type& endpoint,
                              std::function<void(int)> on_pre_connect) {
  DCHECK(engine_);
  Engine::OpResult op_result = engine_->Handshake(Engine::HandshakeType::CLIENT);
  if (!op_result) {
    return std::error_code(op_result.error(), std::system_category());
  }

  // If the socket is already open, we should not call connect on it
  if (!IsOpen()) {
    error_code ec = next_sock_->Connect(endpoint, std::move(on_pre_connect));
    if (ec)
      return ec;
  }

  // Flush the ssl data to the socket and run the loop that ensures handshaking converges.
  int op_val = *op_result;

  // it should guide us to write and then read.
  DCHECK_EQ(op_val, Engine::NEED_READ_AND_MAYBE_WRITE);
  while (op_val < 0) {
    error_code ec = HandleOp(op_val);
    if (ec)
      return ec;

    op_result = engine_->Handshake(Engine::HandshakeType::CLIENT);
    if (!op_result) {
      return std::error_code(op_result.error(), std::system_category());
    }
    op_val = *op_result;
  }

  const auto* cipher = SSL_get_current_cipher(engine_->native_handle());
  VLOG(1) << "SSL handshake success, chosen " << SSL_CIPHER_get_name(cipher) << "/"
          << SSL_CIPHER_get_version(cipher);

  return {};
}

auto TlsSocket::Close() -> error_code {
  DCHECK(engine_);
  return next_sock_->Close();
}

class SpinCounter {
 public:
  explicit SpinCounter(size_t limit) : limit_(limit) {
  }

  bool Check(bool condition) {
    current_it_ = condition ? current_it_ + 1 : 0;
    return current_it_ >= limit_;
  }

  size_t Limit() const {
    return limit_;
  }

  size_t Spins() const {
    return current_it_;
  }

 private:
  const size_t limit_;
  size_t current_it_{0};
};

io::Result<size_t> TlsSocket::RecvMsg(const msghdr& msg, int flags) {
  DCHECK(engine_);
  DCHECK_GT(size_t(msg.msg_iovlen), 0u);

  DLOG_IF(INFO, flags) << "Flags argument is not supported " << flags;

  auto* io = msg.msg_iov;
  size_t io_len = msg.msg_iovlen;

  Engine::MutableBuffer dest{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
  size_t read_total = 0;
  SpinCounter spin_count(20);

  while (true) {
    DCHECK(!dest.empty());

    size_t read_len = std::min(dest.size(), size_t(INT_MAX));

    Engine::OpResult op_result = engine_->Read(dest.data(), read_len);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    int op_val = *op_result;
    if (spin_count.Check(op_val <= 0)) {
      // Once every 30 seconds.
      LOG_EVERY_T(WARNING, 30) << "IO loop spin limit reached. Limit: " << spin_count.Limit()
                               << " Spin: " << spin_count.Spins();
    }

    if (op_val > 0) {
      read_total += op_val;

      if (size_t(op_val) == read_len) {
        if (size_t(op_val) < dest.size()) {
          dest.remove_prefix(op_val);
        } else {
          ++io;
          --io_len;
          if (io_len == 0)
            break;  // Finished reading everything.
          dest = Engine::MutableBuffer{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
        }
        // We read everything we asked for but there are still buffers left to fill.
        continue;
      }
      break;
    }

    error_code ec = HandleOp(op_val);
    if (ec)
      return make_unexpected(ec);
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
  io::Result<size_t> res;
  size_t total_sent = 0;

  while (len) {
    if (ptr->iov_len > kBufferSize || len == 1) {
      res = SendBuffer(Engine::Buffer{reinterpret_cast<uint8_t*>(ptr->iov_base), ptr->iov_len});
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
      res = SendBuffer({scratch, buffered_size});
    }
    if (!res) {
      return res;
    }
    total_sent += *res;
  }
  return total_sent;
}

io::Result<size_t> TlsSocket::SendBuffer(Engine::Buffer buf) {
  // Sending buffer into ssl.
  DCHECK(engine_);
  DCHECK_GT(buf.size(), 0u);

  size_t send_total = 0;
  SpinCounter spin_count(20);

  while (true) {
    Engine::OpResult op_result = engine_->Write(buf);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
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

    if (spin_count.Check(op_val <= 0)) {
      // Once every 30 seconds.
      LOG_EVERY_T(WARNING, 30) << "IO loop spin limit reached. Limit: " << spin_count.Limit()
                               << " Spins: " << spin_count.Spins();
    }

    error_code ec = HandleOp(op_val);
    if (ec)
      return make_unexpected(ec);
  }

  return send_total;
}

// TODO: to implement async functionality.
void TlsSocket::AsyncWriteSome(const iovec* v, uint32_t len, AsyncProgressCb cb) {
  io::Result<size_t> res = WriteSome(v, len);
  cb(res);
}

SSL* TlsSocket::ssl_handle() {
  return engine_ ? engine_->native_handle() : nullptr;
}

auto TlsSocket::MaybeSendOutput() -> error_code {
  if (engine_->OutputPending() == 0)
    return {};

  // This function is present in both read and write paths.
  // meaning that both of them can be called concurrently from differrent fibers and then
  // race over flushing the output buffer. We use state_ to prevent that.
  if (state_ & WRITE_IN_PROGRESS) {
    // Do not remove the Yield from here because it can enter an infinite loop
    // which can be described by the following chore:
    // Fiber1 (Connection fiber):
    // Reads SSL renegotiation request
    // Sets WRITE_IN_PROGRESS in state_
    // Tries to write renegotiation response to socket
    // BIO (SSL buffer) gets filled up
    // Fiber2 (runs command):
    // Tries to write response to socket
    // Can't write to socket because WRITE_IN_PROGRESS is set
    // Live blocks indefinitely
    // Fiber2 is in infinite loop, Fiber1 can't progress
    ThisFiber::Yield();
    return error_code{};
  }

  return HandleSocketWrite();
}

auto TlsSocket::HandleSocketRead() -> error_code {
  error_code ec = MaybeSendOutput();
  if (ec)
    return ec;

  if (state_ & READ_IN_PROGRESS) {
    // We need to Yield because otherwise we might end up in an infinite loop.
    // See also comments in MaybeSendOutput.
    ThisFiber::Yield();
    return error_code{};
  }

  auto mut_buf = engine_->PeekInputBuf();
  state_ |= READ_IN_PROGRESS;
  io::Result<size_t> esz = next_sock_->Recv(mut_buf, 0);
  state_ &= ~READ_IN_PROGRESS;
  if (!esz) {
    return esz.error();
  }

  DVLOG(1) << "TlsSocket:Read " << *esz << " bytes";

  engine_->CommitInput(*esz);

  return error_code{};
}

error_code TlsSocket::HandleSocketWrite() {
  Engine::Buffer buffer = engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());

  if (buffer.empty())
    return {};

  // we do not allow concurrent writes from multiple fibers.
  state_ |= WRITE_IN_PROGRESS;
  while (!buffer.empty()) {
    io::Result<size_t> write_result = next_sock_->WriteSome(buffer);

    DCHECK(engine_);
    if (!write_result) {
      state_ &= ~WRITE_IN_PROGRESS;

      return write_result.error();
    }
    CHECK_GT(*write_result, 0u);
    engine_->ConsumeOutputBuf(*write_result);
    buffer.remove_prefix(*write_result);
  }
  DCHECK_EQ(engine_->OutputPending(), 0u);

  state_ &= ~WRITE_IN_PROGRESS;

  return error_code{};
}

error_code TlsSocket::HandleOp(int op_val) {
  switch (op_val) {
    case Engine::EOF_STREAM:
      return make_error_code(errc::connection_reset);
    case Engine::NEED_READ_AND_MAYBE_WRITE:
      return HandleSocketRead();
    default:
      LOG(DFATAL) << "Unsupported " << op_val;
  }
  return {};
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

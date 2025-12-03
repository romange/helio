// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/tls/tls_socket.h"

#include <absl/strings/escaping.h>
#include <openssl/err.h>

#include <algorithm>

#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"
#include "util/tls/tls_engine.h"

#define VSOCK(verbosity)                                                            \
  VLOG(verbosity) << "sock[" << native_handle() << "], state " << state_.to_ulong() \
                  << ", write_total:" << upstream_write_ << " "                     \
                  << " pending output: " << engine_->OutputPending() << " "

#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {
namespace tls {

using namespace std;
using nonstd::make_unexpected;

#define RETURN_ON_ERROR(x) \
  do {                     \
    auto ec = (x);         \
    if (ec) {              \
      return ec;           \
    }                      \
  } while (false)

TlsSocket::TlsSocket(std::unique_ptr<FiberSocketBase> next)
    : FiberSocketBase(next ? next->proactor() : nullptr), next_sock_(std::move(next)) {
}

TlsSocket::TlsSocket(FiberSocketBase* next) : TlsSocket(std::unique_ptr<FiberSocketBase>(next)) {
}

TlsSocket::~TlsSocket() {
  // sanity check that all pending ops are done.
  DCHECK_EQ((state_[WRITE_IN_PROGRESS] || state_[READ_IN_PROGRESS] || state_[SHUTDOWN_IN_PROGRESS]),
            false);
}

void TlsSocket::InitSSL(SSL_CTX* context, Buffer prefix) {
  CHECK(!engine_);
  engine_.reset(new Engine{context});
  if (!prefix.empty()) {
    auto input_buf = engine_->PeekInputBuf();
    CHECK_GE(input_buf.size(), prefix.size());
    std::memcpy(input_buf.data(), prefix.data(), prefix.size());
    engine_->CommitInput(prefix.size());
  }
}

auto TlsSocket::Shutdown(int how) -> error_code {
  DCHECK(engine_);
  if (state_[SHUTDOWN_DONE] || state_[SHUTDOWN_IN_PROGRESS]) {
    return {};
  }

  state_.set(SHUTDOWN_IN_PROGRESS);
  Engine::OpResult op_result = engine_->Shutdown();

  // TODO: this flow is hacky and should be reworked.
  // 1. If we are blocked on writes, then MaybeSendOutput() will block as well and shutdown
  // might deadlock. but if we do not call MaybeSendOutput, then the peer will not get
  // the close_notify message.
  // Furthermore, the call `next_sock_->Shutdown` below can race with sending close_notify
  // message.
  // For now we just try to call MaybeSendOutput only if there is no ongoing write.
  if (op_result && !state_[WRITE_IN_PROGRESS]) {
    // engine_ could send notification messages to the peer.
    // We try to send it without blocking, best effort.
    Engine::Buffer buffer = engine_->PeekOutputBuf();
    if (!buffer.empty()) {
      auto res = next_sock_->TrySend(buffer);
      if (res && (*res > 0)) {
        engine_->ConsumeOutputBuf(*res);
      }
    }
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
  state_.set(SHUTDOWN_DONE);
  state_.reset(SHUTDOWN_IN_PROGRESS);

  return res;
}

auto TlsSocket::Accept() -> AcceptResult {
  DCHECK(engine_);

  while (true) {
    Engine::OpResult op_result = engine_->Handshake(Engine::SERVER);

    if (op_result == Engine::EOF_STREAM) {
      return make_unexpected(make_error_code(errc::connection_aborted));
    }

    // it is important to send output (protocol errors) before we return from this function.
    error_code ec = MaybeSendOutput();
    if (ec) {
      VSOCK(1) << "MaybeSendOutput failed " << ec;
      return make_unexpected(ec);
    }

    if (op_result == 1) {  // Success.
      if (VLOG_IS_ON(1)) {
        const SSL_CIPHER* cipher = SSL_get_current_cipher(engine_->native_handle());
        string_view proto_version = SSL_get_version(engine_->native_handle());

        // IANA mapping https://testssl.sh/openssl-iana.mapping.html
        uint16_t protocol_id = SSL_CIPHER_get_protocol_id(cipher);

        LOG(INFO) << "sock[" << native_handle() << "] SSL success, chosen "
                  << SSL_CIPHER_get_name(cipher) << "/" << proto_version << " " << protocol_id;
      }
      break;
    }

    ec = HandleOp(op_result);
    if (ec)
      return make_unexpected(ec);
  }

  return nullptr;
}

error_code TlsSocket::Connect(const endpoint_type& endpoint,
                              std::function<void(int)> on_pre_connect) {
  DCHECK(engine_);
  while (true) {
    Engine::OpResult op_result = engine_->Handshake(Engine::HandshakeType::CLIENT);
    /*
    Commenting this line causes a deadlock in AsyncReadNeedWrite.
    if (op_result == 1) {
      break;
    }
    */
    if (op_result == Engine::EOF_STREAM) {
      return make_error_code(errc::connection_refused);
    }

    // If the socket is already open, we should not call connect on it
    if (!IsOpen()) {
      RETURN_ON_ERROR(next_sock_->Connect(endpoint, std::move(on_pre_connect)));
    }

    // Flush pending output.
    RETURN_ON_ERROR(MaybeSendOutput());

    if (op_result == 1) {
      break;
    }

    // Flush the ssl data to the socket and run the loop that ensures handshaking converges.
    RETURN_ON_ERROR(HandleOp(op_result));
  }

  const SSL_CIPHER* cipher = SSL_get_current_cipher(engine_->native_handle());
  string_view proto_version = SSL_get_version(engine_->native_handle());

  // IANA mapping https://testssl.sh/openssl-iana.mapping.html
  uint16_t protocol_id = SSL_CIPHER_get_protocol_id(cipher);

  VSOCK(1) << "SSL handshake success, chosen " << SSL_CIPHER_get_name(cipher) << "/"
           << proto_version << " " << protocol_id;

  return {};
}

auto TlsSocket::Close() -> error_code {
  DCHECK(engine_);
  return next_sock_->Close();
}

io::Result<size_t> TlsSocket::RecvMsg(const msghdr& msg, int flags) {
  LOG(INFO) << "TlsSocket::RecvMsg called";
  DCHECK(engine_);
  DCHECK_GT(size_t(msg.msg_iovlen), 0u);
  DLOG_IF(INFO, flags) << "Flags argument is not supported " << flags;

  auto* io = msg.msg_iov;
  size_t io_len = msg.msg_iovlen;

  DVSOCK(1) << "RecvMsg " << io_len << " records";

  Engine::MutableBuffer dest{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
  size_t read_total = 0;

  while (true) {
    DCHECK(!dest.empty());

    Engine::OpResult op_result = engine_->Read(dest.data(), dest.size());
    DVLOG(2) << "Engine::Read tried to read " << dest.size() << " bytes, got " << op_result;
    int op_val = op_result;

    if (op_val > 0) {
      read_total += op_val;

      if (size_t(op_val) < dest.size()) {
        // Note that engine can return short reads so we can continue reading until we get
        // op_val < 0 indicating that an upstream read is needed.
        dest.remove_prefix(op_val);
      } else {
        ++io;
        --io_len;
        if (io_len == 0)
          break;  // Fully filled msg.msg_iovlen

        dest = Engine::MutableBuffer{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
      }
      // Repeat the loop to read the next chunk.
      continue;
    }

    if ((read_total > 0) && (op_val == Engine::NEED_READ_AND_MAYBE_WRITE)) {
      // After reading some data, the TLS engine may have generated protocol output (e.g., handshake
      // or alerts). Flush any pending output to ensure protocol progress and avoid stalling the
      // peer.
      if (engine_->OutputPending() > 0) {
        auto ec = MaybeSendOutput();
        if (ec) {
          return make_unexpected(ec);
        }
      }
      return read_total;
    }

    error_code ec = HandleOp(op_val);
    if (ec)
      return make_unexpected(ec);
  }
  return read_total;
}

io::Result<size_t> TlsSocket::Recv(const io::MutableBytes& mb, int flags) {
  LOG(INFO) << "TlsSocket::Recv called";
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
  while (true) {
    PushResult push_res = PushToEngine(ptr, len);
    if (push_res.engine_opcode < 0) {
      auto ec = HandleOp(push_res.engine_opcode);
      if (ec) {
        VLOG(1) << "HandleOp failed " << ec.message();
        return make_unexpected(ec);
      }
    }

    if (push_res.written > 0) {
      auto ec = MaybeSendOutput();
      if (ec) {
        VLOG(1) << "MaybeSendOutput failed " << ec.message();
        return make_unexpected(ec);
      }
      return push_res.written;
    }
  }
}

TlsSocket::PushResult TlsSocket::PushToEngine(const iovec* ptr, uint32_t len) {
  PushResult res;

  // Chosen to be sufficiently smaller than the usual MTU (1500) and a multiple of 16.
  // IP - max 24 bytes. TCP - max 60 bytes. TLS - max 21 bytes.
  static constexpr size_t kBatchSize = 1392;

  while (len) {
    Engine::OpResult op_result;
    Engine::Buffer buf;

    if (ptr->iov_len >= kBatchSize || len == 1) {
      buf = {reinterpret_cast<uint8_t*>(ptr->iov_base), ptr->iov_len};
      op_result = engine_->Write(buf);
      ptr++;
      len--;
    } else {
      size_t batch_size = 0;
      uint8_t batch_buf[kBatchSize];

      do {
        std::memcpy(batch_buf + batch_size, ptr->iov_base, ptr->iov_len);
        batch_size += ptr->iov_len;
        ptr++;
        len--;
      } while (len && (batch_size + ptr->iov_len) <= kBatchSize);

      buf = {batch_buf, batch_size};

      // In general we should pass the same arguments in case of retries, but since we
      // configure the engine with SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, we can change the
      // buffer between retries.
      op_result = engine_->Write(buf);
    }

    int op_val = op_result;
    if (op_val < 0) {
      res.engine_opcode = op_val;
      return res;
    }

    CHECK_GT(op_val, 0);
    res.written += op_val;
    if (unsigned(op_val) != buf.size()) {
      break;  // need to flush the SSL output buffer to the underlying socket.
    }
  }
  return res;
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
  if (state_[WRITE_IN_PROGRESS]) {
    // This should not happen, as we call MaybeSendOutput from reads only when
    // there is no ongoing write.
    LOG(DFATAL) << "Should not happen";

    // Fiber1 (Connection fiber):
    // Reads SSL renegotiation request
    // Sets WRITE_IN_PROGRESS in state_
    // Tries to write renegotiation response to socket
    // BIO (SSL buffer) gets filled up
    // Fiber2 (runs command):
    // Tries to write response to socket
    // Can't write to socket because WRITE_IN_PROGRESS is set.

    // Wait for the other write to complete.
    fb2::NoOpLock lock;
    block_concurrent_cv_.wait(lock, [&] { return !state_[WRITE_IN_PROGRESS]; });

    return error_code{};
  }

  return WriteToUpstreamSocket();
}

auto TlsSocket::MaybeSendOutputNonBlocking() -> error_code {
  FiberAtomicGuard guard;
  if (engine_->OutputPending() == 0)
    return {};

  if (state_[WRITE_IN_PROGRESS]) {
    return make_error_code(errc::resource_unavailable_try_again);
  }

  return WriteToUpstreamSocketNonBlocking();
}

auto TlsSocket::ReadFromUpstreamSocket() -> error_code {
  if (engine_->OutputPending() != 0) {
    VLOG(1) << "ReadFromUpstreamSocket OutputPending: " << engine_->OutputPending()
            << " WriteInProgress: " << state_[WRITE_IN_PROGRESS];
    // All tls operations should make sure that output is flushed before finishing.
    // If the state machine requested reading, then the output has been flushed,
    // or there is a concurrent write in progress.
    if (!state_[WRITE_IN_PROGRESS]) {
      LOG(DFATAL) << "Should not happen";
      RETURN_ON_ERROR(MaybeSendOutput());
    }
  }

  if (state_[READ_IN_PROGRESS]) {
    // This may happen as both write and read paths may request reading from upstream during
    // renegotiation. There is asymmetry with write and reads, as writes are controlled by
    // our process, and every write by the Engine should follow with SSL_WANT_WRITE, while
    // reads may be the result of renegotiation requests from the peer.

    // Wait for the other read to complete.
    fb2::NoOpLock lock;
    block_concurrent_cv_.wait(lock, [&] { return !state_[READ_IN_PROGRESS]; });
    return error_code{};
  }

  auto mut_buf = engine_->PeekInputBuf();
  state_.set(READ_IN_PROGRESS);
  io::Result<size_t> esz = next_sock_->Recv(mut_buf, 0);
  state_.reset(READ_IN_PROGRESS);
  block_concurrent_cv_.notify_one();
  if (!esz) {
    return esz.error();
  }

  if (*esz == 0) {
    // TODO: For TLS sockets we still propagate EOF via connection_aborted errors,
    // as it requires more changes to the upper layers to handle EOF properly.
    return make_error_code(errc::connection_aborted);
  }

  DVSOCK(1) << "ReadFromUpstreamSocket " << *esz << " bytes";

  engine_->CommitInput(*esz);

  return error_code{};
}

error_code TlsSocket::ReadFromUpstreamSocketNonBlocking() {
  FiberAtomicGuard guard;
  if (engine_->OutputPending() != 0) {
    // All tls operations should make sure that output is flushed before finishing.
    // If the state machine requested reading, then the output has been flushed,
    // or there is a concurrent write in progress.
    if (!state_[WRITE_IN_PROGRESS]) {
      RETURN_ON_ERROR(MaybeSendOutputNonBlocking());
    }
  }

  if (state_[READ_IN_PROGRESS]) {
    return make_error_code(errc::resource_unavailable_try_again);
  }

  auto mut_buf = engine_->PeekInputBuf();
  state_.set(READ_IN_PROGRESS);
  io::Result<size_t> esz = next_sock_->TryRecv(mut_buf);
  state_.reset(READ_IN_PROGRESS);
  block_concurrent_cv_.notify_one();
  if (!esz) {
    return esz.error();
  }

  if (*esz == 0) {
    // TODO: For TLS sockets we still propagate EOF via connection_aborted errors,
    // as it requires more changes to the upper layers to handle EOF properly.
    return make_error_code(errc::connection_aborted);
  }

  DVSOCK(1) << "ReadFromUpstreamSocketNonBlocking " << *esz << " bytes";

  engine_->CommitInput(*esz);

  return error_code{};
}

error_code TlsSocket::WriteToUpstreamSocket() {
  Engine::Buffer buffer = engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());

  if (buffer.empty())
    return {};

  DVSOCK(2) << "WriteToUpstreamSocket " << buffer.size();

  error_code ec;
  // we do not allow concurrent writes from multiple fibers.
  state_.set(WRITE_IN_PROGRESS);
  do {
    io::Result<size_t> write_result = next_sock_->WriteSome(buffer);

    DCHECK(engine_);
    if (!write_result) {
      ec = write_result.error();
      break;
    }
    CHECK_GT(*write_result, 0u);

    upstream_write_ += *write_result;
    engine_->ConsumeOutputBuf(*write_result);

    // We could preempt while calling WriteSome, and the engine could get more data to write.
    // Therefore we sync the buffer.
    buffer = engine_->PeekOutputBuf();
  } while (!buffer.empty());

  DCHECK(engine_->OutputPending() == 0 || ec);

  state_.reset(WRITE_IN_PROGRESS);
  block_concurrent_cv_.notify_one();

  return ec;
}

error_code TlsSocket::WriteToUpstreamSocketNonBlocking() {
  FiberAtomicGuard guard;
  Engine::Buffer buffer = engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());

  if (buffer.empty())
    return {};

  DVSOCK(2) << "WriteToUpstreamSocketNonBlocking " << buffer.size();

  error_code ec;
  // we do not allow concurrent writes from multiple fibers.
  state_.set(WRITE_IN_PROGRESS);

  io::Result<size_t> write_result = next_sock_->TrySend(buffer);

  DCHECK(engine_);
  if (!write_result) {
    ec = write_result.error();
    VLOG(1) << "WriteToUpstreamSocketNonBlocking failed: " << ec.message();
  } else {
    CHECK_GT(*write_result, 0u);
    upstream_write_ += *write_result;
    engine_->ConsumeOutputBuf(*write_result);
    VLOG(1) << "WriteToUpstreamSocketNonBlocking wrote " << *write_result
            << " bytes. Pending: " << engine_->OutputPending();
  }

  state_.reset(WRITE_IN_PROGRESS);
  block_concurrent_cv_.notify_one();

  return ec;
}

error_code TlsSocket::HandleOp(int op_val) {
  switch (op_val) {
    case Engine::EOF_STREAM:
      VLOG(1) << "EOF_STREAM received " << next_sock_->native_handle();
      return make_error_code(errc::connection_aborted);
    case Engine::NEED_READ_AND_MAYBE_WRITE:
      return ReadFromUpstreamSocket();
    case Engine::NEED_WRITE:
      return MaybeSendOutput();
    default:
      LOG(DFATAL) << "Unsupported " << op_val;
  }
  return {};
}

error_code TlsSocket::HandleOpNonBlocking(int op_val) {
  FiberAtomicGuard guard;
  switch (op_val) {
    case Engine::EOF_STREAM:
      VLOG(1) << "EOF_STREAM received " << next_sock_->native_handle();
      return make_error_code(errc::connection_aborted);
    case Engine::NEED_READ_AND_MAYBE_WRITE:
      return ReadFromUpstreamSocketNonBlocking();
    case Engine::NEED_WRITE:
      return MaybeSendOutputNonBlocking();
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

void TlsSocket::RegisterOnRecv(OnRecvCb cb) {
  recv_cb_ = std::move(cb);

  if (engine_->OutputPending() > 0) {
    fb2::Fiber("tls_flush", [this] {
      while (engine_->OutputPending() > 0) {
        auto ec = MaybeSendOutputNonBlocking();
        if (ec) {
          VLOG(1) << "Flush failed: " << ec.message();
          break;
        }
      }
    }).Detach();
  }

  next_sock_->RegisterOnRecv([this](const RecvNotification& rn) { RecvAsync(rn); });
}

void TlsSocket::ResetOnRecvHook() {
  recv_cb_ = nullptr;
  next_sock_->ResetOnRecvHook();
}

void TlsSocket::RecvAsync(const RecvNotification& rn) {
  if (std::holds_alternative<FiberSocketBase::RecvCompletion>(rn.read_result) ||
      std::holds_alternative<std::error_code>(rn.read_result)) {
    if (recv_cb_) {
      recv_cb_(rn);
    }
    return;
  }

  if (std::holds_alternative<io::MutableBytes>(rn.read_result)) {
    VLOG(1) << "[TLS DBG] RecvAsync: got MutableBytes, size="
            << std::get<io::MutableBytes>(rn.read_result).size();
    auto& buf = std::get<io::MutableBytes>(rn.read_result);
    VLOG(1) << "[TLS DBG] RecvAsync: buf.data="
            << std::string(reinterpret_cast<const char*>(buf.data()), buf.size());

    auto input_buf = engine_->PeekInputBuf();
    VLOG(1) << "[TLS DBG] RecvAsync: engine_->PeekInputBuf size=" << input_buf.size();
    CHECK_GE(input_buf.size(), buf.size());
    std::memcpy(input_buf.data(), buf.data(), buf.size());
    engine_->CommitInput(buf.size());
    VLOG(1) << "[TLS DBG] RecvAsync: committed " << buf.size() << " bytes to engine input";

    // Loop to read and deliver all available decrypted application data.
    while (true) {
      static constexpr size_t kAppBufSize = 4096;
      std::array<uint8_t, kAppBufSize> app_buf{};
      int red_res = engine_->Read(app_buf.data(), app_buf.size());
      VLOG(1) << "[TLS DBG] RecvAsync: engine_->Read tried to read " << app_buf.size()
              << " bytes, got " << red_res;
      if (red_res > 0) {
        VLOG(1) << "[TLS DBG] RecvAsync: decrypted " << red_res << " bytes: "
                << std::string(reinterpret_cast<const char*>(app_buf.data()), red_res);
        RecvNotification internal_rn;
        internal_rn.read_result = io::MutableBytes{app_buf.data(), static_cast<size_t>(red_res)};
        VLOG(1) << "[TLS DBG] RecvAsync: calling recv_cb_ with decrypted data";
        recv_cb_(internal_rn);
        // Continue looping to deliver all buffered decrypted data.
        continue;
      } else if (red_res == Engine::NEED_READ_AND_MAYBE_WRITE || red_res == Engine::NEED_WRITE) {
        VLOG(1) << "[TLS DBG] RecvAsync: engine needs more read/write, calling HandleOp(" << red_res
                << ")";
        HandleOp(red_res);
        // After handling, check if more decrypted data is available and continue loop.
        continue;
      } else {
        VLOG(1) << "[TLS DBG] RecvAsync: engine->Read returned " << red_res << ", breaking";
        break;
      }
    }
  }
}

io::Result<size_t> TlsSocket::TrySend(io::Bytes buf) {
  iovec v{const_cast<uint8_t*>(buf.data()), buf.size()};
  return TrySend(&v, 1);
}

io::Result<size_t> TlsSocket::TrySend(const iovec* v, uint32_t len) {
  FiberAtomicGuard guard;
  size_t total_written{};
  size_t iov_idx{};
  size_t iov_offset{};

  // DEBUG: Print total bytes to send and plaintext payload for all iovec
  size_t total_bytes_to_send = 0;
  std::string plaintext;
  for (uint32_t i = 0; i < len; ++i) {
    total_bytes_to_send += v[i].iov_len;
    const auto* data_ptr = reinterpret_cast<const char*>(v[i].iov_base);
    plaintext.append(data_ptr, v[i].iov_len);
  }
  if (total_bytes_to_send > 0) {
    VLOG(1) << "[DEBUG] TrySend total bytes to send: " << total_bytes_to_send << " (" << len
            << " iovecs), plaintext payload: " << absl::CHexEscape(plaintext);
  }

  while (iov_idx < len) {
    // Flush any pending output from previous iterations or previous calls.
    while (engine_->OutputPending() > 0) {
      auto ec = MaybeSendOutputNonBlocking();
      if (ec) {
        if (ec == errc::resource_unavailable_try_again) {
          // If we have already consumed some input in this call, return that amount.
          // The caller will retry the rest.
          if (total_written > 0) {
            return total_written;
          }
          // If we haven't consumed anything, return EAGAIN so the caller waits.
          return make_unexpected(ec);
        }
        VLOG(1) << "MaybeSendOutputNonBlocking failed " << ec.message();
        return make_unexpected(ec);
      }
      // If MaybeSendOutputNonBlocking succeeded, it means we wrote something.
      // We loop to check if there is more pending output.
    }

    // Create a temporary iovec representing the unsent portion of the current buffer.
    // This allows us to handle partial writes and retries without modifying the original
    // iovec array.
    iovec tmp;
    tmp.iov_base = static_cast<uint8_t*>(v[iov_idx].iov_base) + iov_offset;
    tmp.iov_len = v[iov_idx].iov_len - iov_offset;
    PushResult push_res = PushToEngine(&tmp, 1);
    if (push_res.engine_opcode < 0) {
      auto ec = HandleOpNonBlocking(push_res.engine_opcode);
      if (ec) {
        if (ec == errc::resource_unavailable_try_again && total_written > 0) {
          return total_written;
        }
        if (ec == errc::resource_unavailable_try_again) {
          return make_unexpected(ec);
        }
        VLOG(1) << "HandleOpNonBlocking failed " << ec.message();
        return make_unexpected(ec);
      }
    }

    if (push_res.written > 0) {
      size_t written = push_res.written;
      total_written += written;
      if (written < tmp.iov_len) {
        iov_offset += written;
      } else {
        ++iov_idx;
        iov_offset = 0;
      }
      // We pushed data, so we likely have output.
      // The next iteration of the loop will handle flushing it.
      continue;
    }
  }

  // We finished consuming input. Try to flush one last time.
  // If we fail to flush here, we still return total_written (success).
  // This creates a risk of buffered data not being sent.
  // However, since we flushed at the start of the loop, the amount of buffered data
  // is limited to what was generated by the last chunk(s).
  // To handle this case, RegisterOnRecv (and Recv) must ensure pending output is flushed.
  while (engine_->OutputPending() > 0) {
    auto ec = MaybeSendOutputNonBlocking();
    if (ec) {
      VLOG(1) << "MaybeSendOutputNonBlocking returned error: " << ec.message();
      // If EAGAIN, we can't flush everything.
      // We must return total_written because we consumed the input.
      break;
    }
  }

  if (engine_->OutputPending() > 0) {
    VLOG(1) << "Spawning flush fiber. Pending: " << engine_->OutputPending();
    fb2::Fiber("tls_flush_trysend", [this] {
      VLOG(1) << "Flush fiber started. Pending: " << engine_->OutputPending();
      while (engine_->OutputPending() > 0) {
        auto ec = MaybeSendOutputNonBlocking();
        if (ec) {
          VLOG(1) << "Flush failed: " << ec.message();
          break;
        }
      }
      VLOG(1) << "Flush fiber finished. Pending: " << engine_->OutputPending();
    }).Detach();
  } else {
    VLOG(1) << "TrySend flushed everything.";
  }

  return total_written;
}

io::Result<size_t> TlsSocket::TryRecv(io::MutableBytes buf) {
  size_t total_read = 0;
  VLOG(1) << "[TLS DBG] TryRecv: buf.size=" << buf.size();
  while (!buf.empty()) {
    int n = engine_->Read(buf.data(), buf.size());
    VLOG(1) << "[TLS DBG] TryRecv: engine_->Read tried to read " << buf.size() << " bytes, got "
            << n;
    if (n > 0) {
      VLOG(1) << "[TLS DBG] TryRecv: read " << n
              << " bytes, data=" << std::string(reinterpret_cast<const char*>(buf.data()), n);
      buf.remove_prefix(n);
      total_read += n;
      continue;
    } else if (n == Engine::NEED_READ_AND_MAYBE_WRITE || n == Engine::NEED_WRITE) {
      VLOG(1) << "[TLS DBG] TryRecv: engine needs more read/write, n=" << n;
      auto input_buf = engine_->PeekInputBuf();
      VLOG(1) << "[TLS DBG] TryRecv: engine_->PeekInputBuf size=" << input_buf.size();
      if (input_buf.empty()) {
        VLOG(1) << "[TLS DBG] TryRecv: input_buf empty, total_read=" << total_read;
        if (total_read == 0) {
          VLOG(1) << "[TLS DBG] TryRecv: returning EAGAIN";
          return make_unexpected(make_error_code(std::errc::resource_unavailable_try_again));
        }
        break;
      }
      io::Result<size_t> res = next_sock_->TryRecv(input_buf);
      VLOG(1) << "[TLS DBG] TryRecv: next_sock_->TryRecv returned "
              << (res ? std::to_string(*res) : res.error().message());
      if (res && (*res > 0)) {
        VLOG(1) << "[TLS DBG] TryRecv: committing " << *res << " bytes to engine input";
        engine_->CommitInput(*res);
        continue;
      }
      if (total_read == 0) {
        VLOG(1) << "[TLS DBG] TryRecv: returning error from next_sock_->TryRecv";
        return res;
      }
      break;
    } else {
      VLOG(1) << "[TLS DBG] TryRecv: engine_->Read returned " << n << " (error/EOF)";
      return make_unexpected(n == Engine::EOF_STREAM
                                 ? make_error_code(std::errc::connection_aborted)
                                 : make_error_code(std::errc::io_error));
    }
  }
  VLOG(1) << "[TLS DBG] TryRecv: returning total_read=" << total_read;
  return total_read;
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

void TlsSocket::AsyncReq::MaybeSendOutputAsyncWithRead() {
  if (owner_->engine_->OutputPending() != 0) {
    // Once the networking socket completes the write, it will start the read path
    // We use this bool to signal this.
    should_read_ = true;
    WriteToUpstreamSocketAsync();
    return;
  }

  StartUpstreamRead();
}

void TlsSocket::AsyncReq::AsyncReadProgressCb(io::Result<size_t> read_result) {
  owner_->state_.reset(READ_IN_PROGRESS);
  RunPending();
  if (!read_result) {
    // Erronous path. Apply the completion callback and exit.
    CompleteAsyncReq(read_result);
    return;
  }

  if (*read_result == 0) {  // TODO: EOF, but we should propagate 0 to the user callback.
    CompleteAsyncReq(make_unexpected(make_error_code(errc::connection_aborted)));
    return;
  }

  DVLOG(1) << "AsyncProgressCb " << *read_result << " bytes";
  owner_->engine_->CommitInput(*read_result);
  AsyncRoleBasedAction();
}

void TlsSocket::AsyncReq::StartUpstreamRead() {
  // Even if we early return below we still should not try to read. When we
  // wake up we will poll the SSL engine which will dictate the next action/step.
  should_read_ = false;
  if (owner_->state_[READ_IN_PROGRESS]) {
    auto* prev = std::exchange(owner_->blocked_async_req_, this);
    CHECK(prev == nullptr);
    return;
  }

  auto buffer = owner_->engine_->PeekInputBuf();
  owner_->state_.set(READ_IN_PROGRESS);

  auto& scratch = scratch_iovec_;
  scratch.iov_base = const_cast<uint8_t*>(buffer.data());
  scratch.iov_len = buffer.size();

  owner_->next_sock_->AsyncReadSome(&scratch, 1,
                                    [this](auto res) { this->AsyncReadProgressCb(res); });
}

void TlsSocket::AsyncReq::CompleteAsyncReq(io::Result<size_t> result) {
  std::unique_ptr<AsyncReq> current;
  if (role_ == Role::READER) {
    current = std::move(owner_->async_read_req_);
  } else {
    current = std::move(owner_->async_write_req_);
  }
  current->caller_completion_cb_(result);
}

void TlsSocket::AsyncReq::HandleOpAsync(int op_val) {
  if (op_val > 0) {
    CompleteAsyncReq(op_val);
    return;
  }
  switch (op_val) {
    case Engine::NEED_READ_AND_MAYBE_WRITE:
      MaybeSendOutputAsyncWithRead();
      break;
    case Engine::NEED_WRITE:
      MaybeSendOutputAsync();
      break;
    case Engine::EOF_STREAM:
      CompleteAsyncReq(make_unexpected(make_error_code(errc::connection_aborted)));
      break;
    default:
      // EOF_STREAM should be handled earlier
      LOG(DFATAL) << "Unsupported " << op_val;
  }
}

void TlsSocket::AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  // Engine read
  CHECK(!async_read_req_);

  Engine::OpResult op_val = engine_->Read(reinterpret_cast<uint8_t*>(v->iov_base), v->iov_len);
  DVLOG(2) << "Engine::Read tried to read " << v->iov_len << " bytes, got " << op_val;
  // We read some data from the engine. Satisfy the request and return.
  if (op_val > 0) {
    return cb(op_val);
  }

  if (op_val == Engine::EOF_STREAM) {
    VLOG(1) << "EOF_STREAM received " << next_sock_->native_handle();
    return cb(make_unexpected(make_error_code(errc::connection_aborted)));
  }

  // We could not read from the engine. Dispatch async op.
  DCHECK_GT(len, 0u);
  auto req = AsyncReq{this, std::move(cb), v, len, AsyncReq::READER};
  async_read_req_ = std::make_unique<AsyncReq>(std::move(req));
  async_read_req_->HandleOpAsync(op_val);
}

void TlsSocket::AsyncReq::AsyncWriteProgressCb(io::Result<size_t> write_result) {
  if (!write_result) {
    owner_->state_.reset(WRITE_IN_PROGRESS);

    // broken_pipe - happens when the other side closes the connection. do not log this.
    if (write_result.error() != errc::broken_pipe) {
      VLOG(1) << "sock[" << owner_->native_handle() << "], state " << owner_->state_.to_ulong()
              << ", write_total:" << owner_->upstream_write_ << " "
              << " pending output: " << owner_->engine_->OutputPending()
              << " HandleUpstreamAsyncWrite failed " << write_result.error();
    }

    // We are done. Errornous exit.
    RunPending();
    CompleteAsyncReq(write_result);
    return;
  }

  CHECK_GT(*write_result, 0u);
  owner_->upstream_write_ += *write_result;
  owner_->engine_->ConsumeOutputBuf(*write_result);
  // We might have more data pending. Peek again.
  Buffer buffer = owner_->engine_->PeekOutputBuf();

  // We are not done. Re-arm the async write until we drive it to completion or error.
  // We would also like to avoid fragmented socket writes so we make sure we drain it here
  if (!buffer.empty()) {
    auto& scratch = scratch_iovec_;
    scratch.iov_base = const_cast<uint8_t*>(buffer.data());
    scratch.iov_len = buffer.size();
    owner_->next_sock_->AsyncWriteSome(
        &scratch, 1, [this](auto write_result) { AsyncWriteProgressCb(write_result); });
    return;
  }

  if (owner_->engine_->OutputPending() > 0) {
    LOG(DFATAL) << "ssl buffer is not empty with " << owner_->engine_->OutputPending()
                << " bytes. Async short write detected";
  }

  owner_->state_.reset(WRITE_IN_PROGRESS);
  RunPending();

  // We are done with the write, check if we also need to read because we are
  // in NEED_READ_AND_MAYBE_WRITE state
  if (should_read_) {
    StartUpstreamRead();
    return;
  }

  AsyncRoleBasedAction();
}

void TlsSocket::AsyncReq::AsyncRoleBasedAction() {
  if (role_ == READER) {
    auto op_val = owner_->engine_->Read(reinterpret_cast<uint8_t*>(vec_->iov_base), vec_->iov_len);
    DVLOG(2) << "Engine::Read tried to read " << vec_->iov_len << " bytes, got " << op_val;
    HandleOpAsync(op_val);
    return;
  }

  DCHECK(role_ == WRITER);
  // We wrote some therefore we can complete
  if (engine_written_ > 0) {
    CompleteAsyncReq(engine_written_);
    return;
  }
  // We need to call PushToTheEngine again
  PushResult push_res = owner_->PushToEngine(vec_, len_);
  Engine::OpResult op_val = push_res.engine_opcode;
  engine_written_ = push_res.written;

  if (engine_written_ > 0) {
    // Data was successfully written to the TLS engine.
    // If the engine has pending output (encrypted data to send), flush it asynchronously.
    if (owner_->engine_->OutputPending() > 0) {
      WriteToUpstreamSocketAsync();
      return;
    }
    // Otherwise, all output was consumed internally (no flush needed), so complete the request.
    CompleteAsyncReq(engine_written_);
    return;
  }

  if (op_val < 0) {
    HandleOpAsync(op_val);
    return;
  }

  WriteToUpstreamSocketAsync();
}

void TlsSocket::AsyncReq::WriteToUpstreamSocketAsync() {
  if (owner_->state_[WRITE_IN_PROGRESS]) {
    CHECK(owner_->blocked_async_req_ == nullptr);
    owner_->blocked_async_req_ = this;
    return;
  }

  Engine::Buffer buffer = owner_->engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());
  DCHECK(!owner_->state_[WRITE_IN_PROGRESS]);

  DVLOG(2) << "StartUpstreamWrite " << buffer.size();
  // we do not allow concurrent writes from multiple fibers.
  owner_->state_.set(WRITE_IN_PROGRESS);

  auto& scratch = scratch_iovec_;
  scratch.iov_base = const_cast<uint8_t*>(buffer.data());
  scratch.iov_len = buffer.size();

  owner_->next_sock_->AsyncWriteSome(
      &scratch, 1, [this](auto write_result) { AsyncWriteProgressCb(write_result); });
}

void TlsSocket::AsyncReq::MaybeSendOutputAsync() {
  if (owner_->engine_->OutputPending() == 0) {
    return;
  }

  if (owner_->state_[WRITE_IN_PROGRESS]) {
    CHECK(owner_->blocked_async_req_ == nullptr);
    owner_->blocked_async_req_ = this;
    return;
  }

  WriteToUpstreamSocketAsync();
}

/*
   TODO: Async write path can be improved. We should separate the asynchronous flow that pulls
   data from the engine and pushes it to the upstream socket and the flow that pushes data
   from the user to the engine. We could call AsyncProgressCb with the result as soon as we push
   data to the engine, even if the engine is not flushed yet, as long as we guarantee that the
   engine is eventually flushed. This may create cases where we "miss" socket errors, as we
   discover them eventually. But it's fine as long as we manage this properly in tls socket
   states. Why it is better? Because during happy path, we can push data to the engine, and then
   flush to the socket via TrySend and all this without allocations and asynchronous state that
   needs to be managed. Only if TrySend does not flush everything, we need to enter the async
   state machine. All this is similar to how posix write path works.
*/
void TlsSocket::AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  FiberAtomicGuard guard;
  CHECK(!async_write_req_);

  // Write to the engine
  PushResult push_res = PushToEngine(v, len);

  auto req = AsyncReq{this, std::move(cb), v, len, AsyncReq::WRITER};
  req.SetEngineWritten(push_res.written);

  async_write_req_ = std::make_unique<AsyncReq>(std::move(req));
  const int op_val = push_res.engine_opcode;

  if (push_res.written > 0) {
    // Flush engine's output buffer immediately to avoid extra latency.
    if (engine_->OutputPending() > 0) {
      async_write_req_->WriteToUpstreamSocketAsync();
    } else {
      async_write_req_->HandleOpAsync(push_res.written);
    }
    return;
  }

  // Handle engine state.
  // NEED_WRITE or NEED_READ_AND_MAYBE_WRITE or EOF
  if (op_val < 0) {
    //  We pay for the allocation if op_val=EOF_STREAM but this is a very unlikely case
    //  and I rather keep this function small than actually handling this case explicitly
    //  with an if branch.
    async_write_req_->HandleOpAsync(op_val);
  } else {
    async_write_req_->WriteToUpstreamSocketAsync();
  }
}

void TlsSocket::AsyncReq::RunPending() {
  if (!owner_->blocked_async_req_) {
    return;
  }

  auto* blocked = std::exchange(owner_->blocked_async_req_, nullptr);

  if (blocked->should_read_) {
    blocked->StartUpstreamRead();
    return;
  }

  if (blocked->role_ == Role::WRITER) {
    auto current = std::move(owner_->async_write_req_);
    owner_->AsyncWriteSome(current->vec_, current->len_, std::move(current->caller_completion_cb_));
    return;
  }
  auto current = std::move(owner_->async_read_req_);
  owner_->AsyncReadSome(current->vec_, current->len_, std::move(current->caller_completion_cb_));
}

void TlsSocket::__DebugForceNeedWriteOnAsyncRead(const iovec* v, uint32_t len,
                                                 io::AsyncProgressCb cb) {
  // Engine read
  CHECK(!async_read_req_);
  auto req = AsyncReq{this, std::move(cb), v, len, AsyncReq::READER};
  async_read_req_ = std::make_unique<AsyncReq>(std::move(req));
  async_read_req_->HandleOpAsync(Engine::NEED_WRITE);
}

void TlsSocket::__DebugForceNeedWriteOnAsyncWrite(const iovec* v, uint32_t len,
                                                  io::AsyncProgressCb cb) {
  CHECK(!async_write_req_);
  auto req = AsyncReq{this, std::move(cb), v, len, AsyncReq::WRITER};
  async_write_req_ = std::make_unique<AsyncReq>(std::move(req));

  // Simulate NEED_READ_AND_MAYBE_WRITE. By the end of the async write we should have
  // sent 2x v->iov_len.
  // The reason for this is that we "mock" the state machine with v->iov_len data
  // which we treat as protocol data.
  async_write_req_->HandleOpAsync(Engine::NEED_READ_AND_MAYBE_WRITE);
}

}  // namespace tls
}  // namespace util

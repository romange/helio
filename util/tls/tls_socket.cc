// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/tls/tls_socket.h"

#include <absl/container/inlined_vector.h>
#include <openssl/err.h>

#include <algorithm>

#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"
#include "util/tls/iovec_utils.h"
#include "util/tls/tls_engine.h"

#define VSOCK(verbosity)                                                      \
  VLOG(verbosity) << "sock[" << native_handle() << "], state " << int(state_) \
                  << ", write_total:" << upstream_write_ << " "               \
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
  DCHECK_EQ(state_ & (WRITE_IN_PROGRESS | READ_IN_PROGRESS | SHUTDOWN_IN_PROGRESS), 0);
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
  if (state_ & (SHUTDOWN_DONE | SHUTDOWN_IN_PROGRESS)) {
    return {};
  }

  state_ |= SHUTDOWN_IN_PROGRESS;
  Engine::OpResult op_result = engine_->Shutdown();

  // TODO: this flow is hacky and should be reworked.
  // 1. If we are blocked on writes, then MaybeSendOutput() will block as well and shutdown
  // might deadlock. but if we do not call MaybeSendOutput, then the peer will not get
  // the close_notify message.
  // Furthermore, the call `next_sock_->Shutdown` below can race with sending close_notify
  // message.
  // For now we just try to call MaybeSendOutput only if there is no ongoing write.
  if (op_result && (state_ & WRITE_IN_PROGRESS) == 0) {
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

    if ((op_result == Engine::EOF_ABRUPT) || (op_result == Engine::EOF_GRACEFUL)) {
      VLOG(1) << "EOF_ABRUPT/EOF_GRACEFUL received (Handshake Aborted) fd="
              << next_sock_->native_handle();
      return make_unexpected(make_error_code(errc::connection_reset));
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

    // Server hung up (EOF_ABRUPT) or explicitly rejected us (EOF_GRACEFUL)
    // We tried to connect, but the other side closed the door.
    if (op_result == Engine::EOF_ABRUPT || op_result == Engine::EOF_GRACEFUL) {
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

    int op_val = op_result;

    DVSOCK(2) << "Engine::Read " << dest.size() << " bytes, got " << op_val;

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

    if (read_total > 0 && op_val == Engine::NEED_READ_AND_MAYBE_WRITE) {
      // If we have read some data, we should not block on further reads.
      // TODO: for async reads though we could issue a read request since we know the engine
      // buffer is empty.
      return read_total;
    }
    if (op_val == Engine::EOF_GRACEFUL) {
      VLOG(1) << "EOF_GRACEFUL detected in RecvMsg loop";
      return read_total;  // Return whatever data we have (0 if true EOF)
    }

    error_code ec = HandleOp(op_val);
    if (ec) {
      // If we already have data, return it now.  The application will process it and call RecvMsg
      // again, at which point we will hit the error again and return it then.
      if (read_total > 0) {
        return read_total;
      }
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
  while (true) {
    PushResult push_res = PushToEngine(ptr, len);
    if (push_res.engine_opcode < 0) {
      if (push_res.engine_opcode == Engine::EOF_GRACEFUL) {
        return make_unexpected(make_error_code(std::errc::broken_pipe));
      }
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
  if (state_ & WRITE_IN_PROGRESS) {
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
    block_concurrent_cv_.wait(lock, [&] { return !(state_ & WRITE_IN_PROGRESS); });

    return error_code{};
  }

  return HandleUpstreamWrite();
}

auto TlsSocket::HandleUpstreamRead() -> error_code {
  if (engine_->OutputPending() != 0) {
    // All tls operations should make sure that output is flushed before finisihing.
    // If the state machine requested reading, then the output has been flushed,
    // or there is a concurrent write in progress.
    if ((state_ & WRITE_IN_PROGRESS) == 0) {
      LOG(DFATAL) << "Should not happen";
      RETURN_ON_ERROR(MaybeSendOutput());
    }
  }

  if (state_ & READ_IN_PROGRESS) {
    // This may happen as both write and read paths may request reading from upstream during
    // renegotiation. There is assymetry with write and reads, as writes are controlled by
    // our process, and every write by the Engine should follow with SSL_WANT_WRITE, while
    // reads may be the result of renegotiation requests from the peer.

    // Wait for the other read to complete.
    fb2::NoOpLock lock;
    block_concurrent_cv_.wait(lock, [&] { return !(state_ & READ_IN_PROGRESS); });
    return error_code{};
  }

  auto mut_buf = engine_->PeekInputBuf();
  state_ |= READ_IN_PROGRESS;
  io::Result<size_t> esz = next_sock_->Recv(mut_buf, 0);
  state_ &= ~READ_IN_PROGRESS;
  block_concurrent_cv_.notify_one();
  if (!esz) {
    return esz.error();
  }

  if (*esz == 0) {
    // TODO: For TLS sockets we still propagate EOF via connection_aborted errors,
    // as it requires more changes to the upper layers to handle EOF properly.
    return make_error_code(errc::connection_aborted);
  }

  DVSOCK(1) << "HandleUpstreamRead " << *esz << " bytes";

  engine_->CommitInput(*esz);

  return error_code{};
}

error_code TlsSocket::HandleUpstreamWrite() {
  Engine::Buffer buffer = engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());

  if (buffer.empty())
    return {};

  DVSOCK(2) << "HandleUpstreamWrite " << buffer.size();

  error_code ec;
  // we do not allow concurrent writes from multiple fibers.
  state_ |= WRITE_IN_PROGRESS;
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

  state_ &= ~WRITE_IN_PROGRESS;
  block_concurrent_cv_.notify_one();

  return ec;
}

error_code TlsSocket::HandleOp(int op_val) {
  switch (op_val) {
    case Engine::EOF_ABRUPT:
      VLOG(1) << "EOF_ABRUPT received " << next_sock_->native_handle();
      return make_error_code(errc::connection_reset);
    case Engine::EOF_GRACEFUL:
      // Peer said goodbye cleanly.
      // However, EOF_GRACEFUL should be handled by the callers (Accept/Connect/Recv/Write)
      // explicitly before calling HandleOp.
      LOG(DFATAL) << "EOF_GRACEFUL received in HandleOp *Should be handled by caller) fd=" << next_sock_->native_handle();
      return std::error_code{};
    case Engine::NEED_READ_AND_MAYBE_WRITE:
      return HandleUpstreamRead();
    case Engine::NEED_WRITE:
      return MaybeSendOutput();
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
  DCHECK(cb);
  next_sock_->RegisterOnRecv(
      [recv_cb = std::move(cb), this](const RecvNotification& rn) { OnRecv(rn, recv_cb); });
}

void TlsSocket::OnRecv(const RecvNotification& rn, const OnRecvCb& recv_cb) {
  if ((std::holds_alternative<RecvNotification::RecvCompletion>(rn.read_result)) ||
      (std::holds_alternative<std::error_code>(rn.read_result))) {
    recv_cb(rn);
    return;
  }
  if (auto* buf{std::get_if<io::MutableBytes>(&rn.read_result)}) {
    // Copy the arriving data to the TLS engine's input buffer, commit it, and invoke the receive
    // callback.
    auto input_buf{engine_->PeekInputBuf()};
    DVSOCK(3) << "OnRecv callback invoked (MutableBytes), #bytes =" << buf->size();

    // Note about the next CHECK: We must ensure the arriving data (buf) fits entirely into
    // the TLS engine's currently available input buffer space (input_buf). This check
    // enforces the engine's fundamental buffer invariant. This is currently safe because we
    // use the 'Pull' I/O model (multishot/provided buffers are disabled), allowing TlsSocket
    // to control the read size.
    // If multishot/provided Buffers are enabled, the 'Push' I/O model will be active, and a
    // complex overflow buffer implementation would be necessary to prevent buffer exhaustion
    // and crash.
    CHECK_GE(input_buf.size(), buf->size())
        << "input_buf too small for memcpy: " << input_buf.size() << " < " << buf->size();
    std::memcpy(input_buf.data(), buf->data(), buf->size());
    engine_->CommitInput(buf->size());
    recv_cb({RecvNotification::RecvCompletion{true}});
  }
  LOG(FATAL) << "Unhandled type in RecvNotification::read_result variant";
}

io::Result<size_t> TlsSocket::TrySend(io::Bytes buf) {
  iovec vec[1];
  vec[0].iov_base = const_cast<uint8_t*>(buf.data());
  vec[0].iov_len = buf.size();
  return TrySend(vec, 1);
}

io::Result<size_t> TlsSocket::TrySend(const iovec* v, uint32_t len) {
  if (IsEmptyIovec(v, len)) {
    DCHECK(false) << "TrySend with empty iovec";
    return 0;  // nothing to send (POSIX allows zero-length writes)
  }
  if ((state_ & WRITE_IN_PROGRESS) != 0) {
    // Another fiber is currently writing, we cannot safely proceed
    DVSOCK(3) << "TrySend blocked: WRITE_IN_PROGRESS detected";
    return make_unexpected(make_error_code(errc::resource_unavailable_try_again));
  }
  bool read_in_progress{(state_ & READ_IN_PROGRESS) != 0};
  size_t total_bytes_sent{};
  std::error_code returned_status{};
  // We make a local mutable copy of the iovec descriptors because AdvanceIovec
  // modifies them (adjusting base pointers and lengths) to track partial writes.
  // The input array 'v' is const and belongs to the caller, so we cannot modify it.
  static constexpr size_t kMaxStackIovecs = 16;
  uint32_t curr_iovec_len{len};
  absl::InlinedVector<iovec, kMaxStackIovecs> curr_iov(curr_iovec_len);
  iovec* iov_cursor = curr_iov.data();
  std::memcpy(iov_cursor, v, curr_iovec_len * sizeof(iovec));

  while ((curr_iovec_len > 0) || (engine_->OutputPending() > 0)) {
    // 1. Flush into the upstream socket any pending output from the engine output buffer before
    // pushing more data to the engine from the user. These might be bytes from previous call.
    if (engine_->OutputPending() > 0) {
      auto output_buf{engine_->PeekOutputBuf()};
      DCHECK(!output_buf.empty());
      auto send_result{next_sock_->TrySend(output_buf)};
      if (send_result) {
        CHECK_LE(*send_result, output_buf.size());
        engine_->ConsumeOutputBuf(*send_result);
        DVSOCK(3) << "Flushed " << *send_result << " bytes to upstream";
        if ((*send_result) < output_buf.size()) {  // case 1.A: partial write
          // upstream socket is full - try again later
          returned_status = make_error_code(errc::resource_unavailable_try_again);
          break;
        }
        // case 1.B: full write - fall through to the next step
      } else {  // case 1.C: write failed (EAGAIN or other Error).
        returned_status = send_result.error();
        DLOG_IF(WARNING, (returned_status != errc::resource_unavailable_try_again))
            << "Upstream write error in TrySend: " << returned_status.message();
        break;
      }
    }

    // 2. Skip empty iovec entries (handling 0-length inputs gracefully)
    while (curr_iovec_len > 0 && iov_cursor->iov_len == 0) {
      ++iov_cursor;
      --curr_iovec_len;
    }
    // Check if we are done (either processed all, or all remaining were empty)
    if (curr_iovec_len == 0) {
      break;
    }

    // 3. Push data from user buffer into the engine
    DCHECK_EQ(engine_->OutputPending(), 0u);
    DCHECK_GT(iov_cursor->iov_len, 0u);
    PushResult push_result{PushToEngine(iov_cursor, curr_iovec_len)};
    // PushToEngine Result Semantics:
    // 1. written > 0:   Bytes successfully consumed from the user buffer. This happens even if an
    // error/requirement (opcode < 0) immediately follows.
    // 2. opcode < 0:    Engine requires action (NEED_READ/WRITE) or failed (EOF).
    // If written > 0 AND opcode < 0, it means a partial write occurred before the stop.
    // 3. opcode == 0:   Success. All bytes in this chunk were consumed. In this case, we expect
    // curr_iovec_len to be zero after AdvanceIovec(..).
    // NOTE: We must handle 'written' first. Even if an error or state change (opcode < 0)
    // forces us to stop, the bytes successfully processed so far are valid and must be
    // reported to the caller.
    if (push_result.written > 0) {
      // Advance the iovec array position by the number of bytes written (push_result.written) into
      // the engine
      AdvanceIovec(&iov_cursor, &curr_iovec_len, push_result.written);
      total_bytes_sent += push_result.written;
    }

    if (push_result.engine_opcode < 0) {
      if (push_result.engine_opcode == Engine::NEED_WRITE) {
        // The engine has pending output to flush - loop back to flush it
        continue;
      }
      if (push_result.engine_opcode == Engine::NEED_READ_AND_MAYBE_WRITE) {
        // We MUST read to satisfy the engine.
        if (read_in_progress) {
          DVSOCK(3) << "Read conflict detected in TrySend (usage error)";
          returned_status = make_error_code(errc::resource_unavailable_try_again);
          break;
        }
        auto input_buf{engine_->PeekInputBuf()};
        DCHECK(!input_buf.empty()) << "Engine demanded read but has no input space";
        auto recv_res{next_sock_->TryRecv(input_buf)};
        if (recv_res) {
          if (*recv_res > 0) {
            engine_->CommitInput(*recv_res);
            DVSOCK(3) << "Satisfied NEED_READ with " << *recv_res << " bytes";
            continue;  // Success! Retry the write loop.
          } else {
            // TCP FIN received without TLS close_notify (dirty shutdown)
            DVSOCK(1) << "Upstream EOF during handshake/renegotiation";
            returned_status = make_error_code(errc::connection_reset);
            break;
          }
        } else {
          returned_status = recv_res.error();  // Read blocked (EAGAIN) or socket error
          break;
        }
      }
      if (push_result.engine_opcode == Engine::EOF_STREAM) {
        returned_status = make_error_code(errc::connection_aborted);
        break;
      }
      LOG(FATAL) << "Unexpected engine opcode: " << push_result.engine_opcode;
      returned_status = make_error_code(errc::operation_not_permitted);
      break;
    }  // push_result.engine_opcode < 0
  }    // while
  if (total_bytes_sent > 0) {
    DVSOCK(3) << "TrySend returning " << total_bytes_sent << " bytes";
    return total_bytes_sent;
  }
  if (!returned_status) {
    return 0;  // No error, Clean EOF case
  }
  return make_unexpected(returned_status);
}

io::Result<size_t> TlsSocket::TryRecv(io::MutableBytes buf) {
  size_t total_bytes_read{};
  bool write_in_progress{(state_ & WRITE_IN_PROGRESS) != 0};
  bool read_in_progress{(state_ & READ_IN_PROGRESS) != 0};
  std::error_code returned_status{};  // init to no error

  while (!buf.empty()) {
    auto read_result = engine_->Read(buf.data(), buf.size());
    // Possible values in read_result:
    // - >0: number of bytes read (application data bytes decrypted)
    // - =0: Clean, graceful EOF (peer sent close_notify alert)
    // - NEED_READ_AND_MAYBE_WRITE: TLS engine generated outbound data
    // (handshake/renegotiation/alerts) - drain via writes first, then read peer response
    // - NEED_WRITE: need to write to upstream socket
    // - EOF_ABRUPT: connection closed abruptly by peer (no close_notify) / fatal TLS alert /
    // system-level I/O erro
    // - EOF_GRACEFUL: clean EOF
    // - <0 and not one of the above: fatal TLS (error / protocol violation)
    if (read_result > 0) {  // case 1:
      buf.remove_prefix(read_result);
      total_bytes_read += read_result;
      continue;
    } else if ((read_result == Engine::NEED_READ_AND_MAYBE_WRITE) ||
               (read_result == Engine::NEED_WRITE)) {  // case 2: Handle NEED_READ/WRITE
      ///////////////////////////////////////////////////////////////
      // 1. Check for a write conflict:
      // Write conflicts are expected: The TLS state machine (e.g., renegotiation) may trigger
      // writes during a read operation.
      if (write_in_progress) {
        // Another fiber is handling TLS writes (handshake/renegotiation);
        // we cannot safely proceed without blocking this read fiber.
        // Return partial data if available, or signal retry (EAGAIN) to caller.
        returned_status = make_error_code(errc::resource_unavailable_try_again);
        break;
      }
      ///////////////////////////////////////////////////////////////

      // 2. Handle Pending Output from TLs engine to upstream socket (write_in_progress is false)
      // If the engine generated TLS data (handshake/alerts), flush it now.
      // Otherwise, skip to reading.
      size_t output_pending_bytes{engine_->OutputPending()};
      DCHECK((read_result != Engine::NEED_WRITE) || (output_pending_bytes > 0))
          << "SSL BUG: Engine demands a write but provided no data.";
      if (output_pending_bytes > 0) {
        auto output_buf{engine_->PeekOutputBuf()};
        auto send_result{next_sock_->TrySend(output_buf)};
        if (!send_result) {
          // Write failed (EAGAIN or Error).

          returned_status = send_result.error();
          break;
        }
        engine_->ConsumeOutputBuf(*send_result);

        // If we couldn't send everything, we can't proceed safely.
        if ((*send_result) < output_buf.size()) {
          returned_status = make_error_code(errc::resource_unavailable_try_again);
          LOG(WARNING) << "TlsSocket::TrySend: partial next_sock_->TrySend: sent" << (*send_result)
                       << " out of " << output_buf.size() << " bytes. breaking...";
          break;
        }

        // If we handled a NEED_WRITE successfully, we loop again to see what engine wants next
        if (read_result == Engine::NEED_WRITE)
          continue;
      }
      ///////////////////////////////////////////////////////////////
      // 3. Check for read conflict:
      // A read conflict implies the application is polling TryRecv while concurrently
      // blocked on Recv, which is a usage error. We check this to prevent buffer corruption/crash.
      if (read_in_progress) {
        LOG(DFATAL) << "Concurrent TryRecv and Recv detected - this is a usage error.";
        returned_status = make_error_code(errc::resource_unavailable_try_again);
        break;
      }
      ///////////////////////////////////////////////////////////////
      // 4. Handle Pending Reads From Upstream Socket
      DCHECK(engine_->OutputPending() == 0) << "Pending output must be zero before reading input.";
      auto input_buf{engine_->PeekInputBuf()};
      auto recv_result{next_sock_->TryRecv(input_buf)};
      if (recv_result) {
        if ((*recv_result) > 0) {
          engine_->CommitInput(*recv_result);
          // Loop back to call engine_->Read() again and decrypt these bytes.
          continue;
        }
        // *recv_result == 0 (Upstream socket EOF)
        // The engine returned NEED_READ, but the socket is "dead".
        // This is a "Dirty EOF" (TCP closed without TLS close_notify).
        // We must return connection_reset to signal the dirty shutdown.
        // If total_bytes_read==0 (no decrypted data to return), and socket is "dead".
        // We report this as a connection reset/abort because it wasn't a clean TLS shutdown.
        returned_status = make_error_code(errc::connection_reset);
        break;
      }

      // recv_result has an error (e.g. EAGAIN)
      returned_status = recv_result.error();
      break;
    } else if (read_result == Engine::EOF_ABRUPT) {  // case 3: Abrupt EOF
      // The engine detected an abrupt/dirty EOF (no close_notify) from peer.
      // We report this as a connection reset/abort because it wasn't a clean TLS shutdown.
      returned_status = make_error_code(errc::connection_reset);
      break;
    } else if (read_result == Engine::EOF_GRACEFUL) {  // case 4: Clean EOF
      // Peer said goodbye cleanly.
      // We are done. Return success (0) to indicate EOF.
      returned_status = {};
      break;
    } else {
      LOG(DFATAL) << "BUG: Unsupported read_result " << read_result;
      return make_unexpected(make_error_code(errc::operation_not_permitted));
    }
  }  // while

  if (total_bytes_read > 0) {
    DVSOCK(3) << "TryRecv returning " << total_bytes_read << " bytes";
    return total_bytes_read;
  }
  if (!returned_status) {
    return 0;  // No error, Clean EOF case
  }
  return make_unexpected(returned_status);
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
    StartUpstreamWrite();
    return;
  }

  StartUpstreamRead();
}

void TlsSocket::AsyncReq::AsyncReadProgressCb(io::Result<size_t> read_result) {
  owner_->state_ &= ~READ_IN_PROGRESS;
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
  if (owner_->state_ & READ_IN_PROGRESS) {
    auto* prev = std::exchange(owner_->blocked_async_req_, this);
    CHECK(prev == nullptr);
    return;
  }

  auto buffer = owner_->engine_->PeekInputBuf();
  owner_->state_ |= READ_IN_PROGRESS;

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
    case Engine::EOF_ABRUPT:
      CompleteAsyncReq(make_unexpected(make_error_code(errc::connection_reset)));
      break;
    case Engine::EOF_GRACEFUL:
      // Peer said goodbye cleanly.
      // We are done. Return success (0) to indicate EOF.
      CompleteAsyncReq(0);
      break;
    default:
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

  if (op_val == Engine::EOF_ABRUPT) {
    VLOG(1) << "EOF_ABRUPT received " << next_sock_->native_handle();
    return cb(make_unexpected(make_error_code(errc::connection_reset)));
  }
  if (op_val == Engine::EOF_GRACEFUL) {
    VLOG(1) << "EOF_GRACEFUL received " << next_sock_->native_handle();
    return cb(0);  // return 0 to indicate EOF
  }

  // We could not read from the engine. Dispatch async op.
  DCHECK_GT(len, 0u);
  auto req = AsyncReq{this, std::move(cb), v, len, AsyncReq::READER};
  async_read_req_ = std::make_unique<AsyncReq>(std::move(req));
  async_read_req_->HandleOpAsync(op_val);
}

void TlsSocket::AsyncReq::AsyncWriteProgressCb(io::Result<size_t> write_result) {
  if (!write_result) {
    owner_->state_ &= ~WRITE_IN_PROGRESS;

    // broken_pipe - happens when the other side closes the connection. do not log this.
    if (write_result.error() != errc::broken_pipe) {
      VLOG(1) << "sock[" << owner_->native_handle() << "], state " << int(owner_->state_)
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

  owner_->state_ &= ~WRITE_IN_PROGRESS;
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
  if (op_val < 0) {
    HandleOpAsync(op_val);
    return;
  }

  StartUpstreamWrite();
}

void TlsSocket::AsyncReq::StartUpstreamWrite() {
  if (owner_->state_ & WRITE_IN_PROGRESS) {
    CHECK(owner_->blocked_async_req_ == nullptr);
    owner_->blocked_async_req_ = this;
    return;
  }

  Engine::Buffer buffer = owner_->engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());
  DCHECK((owner_->state_ & WRITE_IN_PROGRESS) == 0);

  DVLOG(2) << "StartUpstreamWrite " << buffer.size();
  // we do not allow concurrent writes from multiple fibers.
  owner_->state_ |= WRITE_IN_PROGRESS;

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

  if (owner_->state_ & WRITE_IN_PROGRESS) {
    CHECK(owner_->blocked_async_req_ == nullptr);
    owner_->blocked_async_req_ = this;
    return;
  }

  StartUpstreamWrite();
}

/*
   TODO: Async write path can be improved. We should separate the asynchronous flow that pulls
   data from the engine and pushes it to the upstream socket and the flow that pushes data
   from the user to the engine. We could call AsyncProgressCb with the result as soon as we push
   data to the engine, even if the engine is not flushed yet, as long as we guarantee that the
   engine is eventually flushed. This may create cases where we "miss" socket errors, as we discover
   them eventually. But it's fine as long as we manage this properly in tls socket states. Why it is
   better? Because during happy path, we can push data to the engine, and then flush to the socket
   via TrySend and all this without allocations and asynchronous state that needs to be managed.
   Only if TrySend does not flush everything, we need to enter the async state machine. All this is
   similar to how posix write path works.
*/
void TlsSocket::AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  CHECK(!async_write_req_);

  // Write to the engine
  PushResult push_res = PushToEngine(v, len);

  auto req = AsyncReq{this, std::move(cb), v, len, AsyncReq::WRITER};
  req.SetEngineWritten(push_res.written);

  async_write_req_ = std::make_unique<AsyncReq>(std::move(req));
  const int op_val = push_res.engine_opcode;

  // Handle engine state.
  // NEED_WRITE or NEED_READ_AND_MAYBE_WRITE or EOF
  if (op_val < 0) {
    //  We pay for the allocation if op_val=EOF_STREAM but this is a very unlikely case
    //  and I rather keep this function small than actually handling this case explicitly
    //  with an if branch.
    async_write_req_->HandleOpAsync(op_val);
  } else {
    async_write_req_->StartUpstreamWrite();
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

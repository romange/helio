// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/tls/tls_socket.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/inlined_vector.h>
#include <openssl/err.h>

#include <algorithm>

#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"
#include "util/tls/iovec_utils.h"
#include "util/tls/tls_engine.h"

#define VSOCK(verbosity)                                                             \
  VLOG(verbosity) << "sock[" << native_handle() << "], state " << int(flags_.bits()) \
                  << ", write_total:" << upstream_write_ << " "                      \
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

void TlsSocket::SocketFlags::clear_io_in_progress_and_notify(uint8_t mask) {
  DCHECK(mask == WRITE_IN_PROGRESS || mask == READ_IN_PROGRESS);
  DCHECK(state_ & mask);
  state_ &= ~mask;
  // Waiters have different predicates, so wake all and let each re-check its condition.
  cv_.notify_all();
}

void TlsSocket::SocketFlags::complete_shutdown() {
  set_shutdown_done();
  clear_shutdown_in_progress();
  // Waiters have different predicates, so wake all and let each re-check its condition.
  cv_.notify_all();
}

void TlsSocket::SocketFlags::wait_until_clear(uint8_t mask) {
  DCHECK(mask != 0);
  DCHECK((mask & ~(WRITE_IN_PROGRESS | READ_IN_PROGRESS | SHUTDOWN_IN_PROGRESS)) == 0);
  if ((state_ & mask) == 0) {
    return;
  }

  fb2::NoOpLock lock;
  cv_.wait(lock, [this, mask] { return (state_ & mask) == 0; });
}

TlsSocket::TlsSocket(std::unique_ptr<FiberSocketBase> next)
    : FiberSocketBase(next ? next->proactor() : nullptr), next_sock_(std::move(next)) {
}

TlsSocket::TlsSocket(FiberSocketBase* next) : TlsSocket(std::unique_ptr<FiberSocketBase>(next)) {
}

TlsSocket::~TlsSocket() {
  // sanity check that all pending ops are done.
  DCHECK(!flags_.io_or_shutdown_in_progress());
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
  auto& socket_flags = flags_;
  if (socket_flags.shutdown_done() || socket_flags.shutdown_in_progress()) {
    return {};
  }

  socket_flags.set_shutdown_in_progress();
  Engine::OpResult op_result = engine_->Shutdown();

  // TODO: this flow is hacky and should be reworked.
  // 1. If we are blocked on writes, then MaybeSendEngineOutput() will block as well and shutdown
  // might deadlock. but if we do not call MaybeSendEngineOutput, then the peer will not get
  // the close_notify message.
  // Furthermore, the call `next_sock_->Shutdown` below can race with sending close_notify
  // message.
  // For now we just try to call MaybeSendEngineOutput only if there is no ongoing write.
  if (op_result && !socket_flags.write_in_progress()) {
    // engine_ could send notification messages to the peer.
    std::ignore = MaybeSendEngineOutput();
  }

  // In any case we should also shutdown the underlying TCP socket without relying on the
  // the peer. This unblocks any sync operations (like Recv) that are waiting for data. It could be
  // that when we are in the middle of MaybeSendEngineOutput, and the other fiber calls Close() on
  // this socket. In this case next_sock_ will be closed by the time we reach this line, so we omit
  // calling Shutdown(). It's not the best behavior, but it's also not disastrous either, because
  // such interaction happens only during the server shutdown.
  error_code res;
  if (next_sock_->IsOpen()) {
    res = next_sock_->Shutdown(how);
  }
  socket_flags.wait_for_io_completion();

  socket_flags.complete_shutdown();
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
    error_code ec = MaybeSendEngineOutput();
    if (ec) {
      VSOCK(1) << "MaybeSendEngineOutput failed " << ec;
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

    ec = HandleEngineOp(op_result);
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
    RETURN_ON_ERROR(MaybeSendEngineOutput());

    if (op_result == 1) {
      break;
    }

    // Flush the ssl data to the socket and run the loop that ensures handshaking converges.
    RETURN_ON_ERROR(HandleEngineOp(op_result));
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

  // Close the underlying socket. This unblocks any sync operations.
  auto res = next_sock_->Close();

  flags_.wait_for_io_or_shutdown_completion();

  return res;
}

io::Result<size_t> TlsSocket::RecvMsg(const msghdr& msg, int flags) {
  DCHECK(engine_);
  DCHECK_GT(size_t(msg.msg_iovlen), 0u);
  DLOG_IF(INFO, flags) << "Flags argument is not supported " << flags;

  // A user-level Recv() call is mutually exclusive with other Recv() or TryRecv() calls.
  // We set a flag to detect this usage error.
  if (flags_.user_recv_in_progress()) {
    LOG(DFATAL)
        << "Usage Error: Concurrent Recv/RecvMsg call detected while another is in progress.";
    return make_unexpected(make_error_code(errc::operation_in_progress));
  }
  flags_.set_user_recv_in_progress();
  auto guard{absl::MakeCleanup([this] { flags_.clear_user_recv_in_progress(); })};

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

    error_code ec = HandleEngineOp(op_val);
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
    PushResult push_res = PushUserDataToEngine(ptr, len);
    if (push_res.engine_opcode < 0) {
      if (push_res.engine_opcode == Engine::EOF_GRACEFUL) {
        return make_unexpected(make_error_code(std::errc::broken_pipe));
      }
      auto ec = HandleEngineOp(push_res.engine_opcode);
      if (ec) {
        VLOG(1) << "HandleEngineOp failed " << ec.message();
        return make_unexpected(ec);
      }
    }

    if (push_res.written > 0) {
      auto ec = MaybeSendEngineOutput();
      if (ec) {
        VLOG(1) << "MaybeSendEngineOutput failed " << ec.message();
        return make_unexpected(ec);
      }
      return push_res.written;
    }
  }
}

TlsSocket::PushResult TlsSocket::PushUserDataToEngine(const iovec* ptr, uint32_t len) {
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

auto TlsSocket::MaybeSendEngineOutput() -> error_code {
  if (engine_->OutputPending() == 0)
    return {};

  // Called from both the read and write paths, which may run concurrently on different fibers.
  // On the write path this is straightforward. On the read path, the TLS engine can generate
  // outbound data that must be flushed before reading can continue — this happens during
  // protocol renegotiation (TLS 1.2) or a KeyUpdate exchange (TLS 1.3), both of which cause
  // SSL_read to return SSL_ERROR_WANT_WRITE.
  // In that case a concurrent write fiber may already hold WRITE_IN_PROGRESS. We must wait for
  // it to finish rather than spinning: without yielding, the fiber spins indefinitely and never
  // passes control to anyone else, so WRITE_IN_PROGRESS never clears and the engine loops on
  // NEED_WRITE forever.
  // See Tls13KeyUpdateNeedWrite test for the concrete scenario.
  auto& socket_flags = flags_;
  if (socket_flags.write_in_progress()) {
    socket_flags.wait_for_write_completion();
    return error_code{};
  }

  return HandleUpstreamWrite();
}

auto TlsSocket::HandleUpstreamRead() -> error_code {
  auto& socket_flags = flags_;
  if (engine_->OutputPending() != 0) {
    // Normally output is flushed before the read path needs upstream data, or a concurrent
    // write fiber is already in progress (WRITE_IN_PROGRESS). During a TLS 1.3 KeyUpdate the
    // write fiber may exit (e.g. after a connection reset) without draining the ack, leaving
    // OutputPending > 0 with WRITE_IN_PROGRESS clear. Flush here; MaybeSendEngineOutput will either
    // succeed or return the connection error, which is the right outcome either way.
    if (!socket_flags.write_in_progress()) {
      RETURN_ON_ERROR(MaybeSendEngineOutput());
    }
  }

  if (socket_flags.read_in_progress()) {
    // This may happen as both write and read paths may request reading from upstream during
    // renegotiation. There is assymetry with write and reads, as writes are controlled by
    // our process, and every write by the Engine should follow with SSL_WANT_WRITE, while
    // reads may be the result of renegotiation requests from the peer.

    // Wait for the other read to complete.
    socket_flags.wait_for_read_completion();
    return error_code{};
  }

  auto mut_buf = engine_->PeekInputBuf();
  socket_flags.set_read_in_progress();
  auto guard = absl::MakeCleanup(
      [&socket_flags] { socket_flags.clear_io_in_progress_and_notify(READ_IN_PROGRESS); });

  io::Result<size_t> esz = next_sock_->Recv(mut_buf, 0);
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
  auto& socket_flags = flags_;
  Engine::Buffer buffer = engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());

  if (buffer.empty())
    return {};

  DVSOCK(2) << "HandleUpstreamWrite " << buffer.size();

  error_code ec;
  // we do not allow concurrent writes from multiple fibers.
  socket_flags.set_write_in_progress();
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

  socket_flags.clear_io_in_progress_and_notify(WRITE_IN_PROGRESS);

  return ec;
}

error_code TlsSocket::HandleEngineOp(int op_val) {
  switch (op_val) {
    case Engine::EOF_ABRUPT:
      VLOG(1) << "EOF_ABRUPT received " << next_sock_->native_handle();
      return make_error_code(errc::connection_reset);
    case Engine::EOF_GRACEFUL:
      // Peer said goodbye cleanly.
      // However, EOF_GRACEFUL should be handled by the callers (Accept/Connect/Recv/Write)
      // explicitly before calling HandleEngineOp.
      LOG(DFATAL) << "EOF_GRACEFUL received in HandleEngineOp (should be handled by caller) fd="
                  << next_sock_->native_handle();
      return std::error_code{};
    case Engine::NEED_READ_AND_MAYBE_WRITE:
      return HandleUpstreamRead();
    case Engine::NEED_WRITE:
      return MaybeSendEngineOutput();
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
  // Note: It is vital to store the callback only once (avoid copy!). Both wake paths - the
  // next_sock_ recv hook (via HandleRecvNotification) and a recv-path engine-output drain
  // completion (StartAsyncWriteForTryRecv) - invoke this single copy, so a callback that carries
  // state by value cannot diverge between two independent copies.
  on_recv_cb_ = std::move(cb);
  next_sock_->RegisterOnRecv(
      [this](const RecvNotification& rn) { HandleRecvNotification(rn, on_recv_cb_); });
}

void TlsSocket::HandleRecvNotification(const RecvNotification& rn, const OnRecvCb& recv_cb) {
  // The recv hook can fire once more after ResetOnRecvHook() has cleared on_recv_cb_, if a
  // notification from next_sock_ was already in flight
  if (!recv_cb) {
    return;
  }
  if ((std::holds_alternative<RecvNotification::RecvCompletion>(rn.read_result)) ||
      (std::holds_alternative<std::error_code>(rn.read_result))) {
    recv_cb(rn);
    return;
  }
  if (auto* buf{std::get_if<io::MutableBytes>(&rn.read_result)}) {
    // Copy the arriving data to the TLS engine's input buffer, commit it, and invoke the receive
    // callback.
    auto input_buf{engine_->PeekInputBuf()};
    DVSOCK(3) << "HandleRecvNotification callback invoked (MutableBytes), #bytes =" << buf->size();

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
    return;
  }
  LOG(FATAL) << "Unhandled type in RecvNotification::read_result variant";
}

void TlsSocket::StartAsyncWrite(io::AsyncProgressCb async_write_cb) {
  // Hard CHECK: overwriting a live async_write_req_ would free an AsyncReq still referenced by
  // in-flight AsyncWriteSome callbacks (use-after-free). Callers guarantee no write is in flight
  // (TrySend/TryRecv bail out early on WRITE_IN_PROGRESS), so this never fires in correct runs.
  CHECK(!async_write_req_);
  DCHECK_GT(engine_->OutputPending(), 0u);
  // (vec, len) = (nullptr, 0): no new user bytes, we only send what the engine already buffered.
  // AsyncRoleBasedAction treats a WRITER with vec_ == nullptr as output-only and ends when drained.
  async_write_req_ =
      std::make_unique<AsyncReq>(this, std::move(async_write_cb), nullptr, 0, AsyncReq::WRITER);
  async_write_req_->StartUpstreamWrite();
}

void TlsSocket::StartAsyncWriteForTryRecv() {
  // Preconditions (no write in flight, output pending) are checked by StartAsyncWrite.
  // Mark the recv-path engine-output drain as in flight before it starts: while it holds
  // WRITE_IN_PROGRESS, TryRecv must surface EAGAIN (a wake IS coming via on_recv_cb_) rather than
  // EBUSY. Cleared below when the write completes.
  flags_.set_drain_engine_in_flight();
  StartAsyncWrite([this](io::Result<size_t> res) {
    flags_.clear_drain_engine_in_flight();
    if (!on_recv_cb_) {
      // No recv callback registered (blocking Recv() path, or a TryRecv() caller that retries on
      // its own) - nobody to wake. This is fine, not an error.
      return;
    }
    if (res)
      on_recv_cb_(RecvNotification{RecvNotification::RecvCompletion{true}});
    else
      on_recv_cb_(RecvNotification{res.error()});
  });
}

io::Result<size_t> TlsSocket::TrySend(io::Bytes buf) {
  iovec vec[1];
  vec[0].iov_base = const_cast<uint8_t*>(buf.data());
  vec[0].iov_len = buf.size();
  return TrySend(vec, 1);
}

io::Result<size_t> TlsSocket::TrySend(const iovec* v, uint32_t len) {
  auto& socket_flags = flags_;
  size_t iovec_total_bytes = GetIovecTotalBytes(v, len);
  if (iovec_total_bytes == 0) {
    LOG(DFATAL) << "TrySend with empty iovec";
    return 0;  // nothing to send (POSIX allows zero-length writes)
  }
  if (socket_flags.write_in_progress()) {
    // Another fiber is currently writing, we cannot safely proceed
    DVSOCK(3) << "TrySend blocked: WRITE_IN_PROGRESS detected";
    return make_unexpected(make_error_code(errc::resource_unavailable_try_again));
  }
  bool read_in_progress{socket_flags.read_in_progress()};
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
    PushResult push_result{PushUserDataToEngine(iov_cursor, curr_iovec_len)};
    // PushUserDataToEngine Result Semantics:
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
      if (push_result.engine_opcode == Engine::EOF_ABRUPT) {
        // The TCP connection "vanished" or a protocol violation occurred. This is a "hard" failure.
        returned_status = make_error_code(errc::connection_aborted);
        break;
      }
      if (push_result.engine_opcode == Engine::EOF_GRACEFUL) {
        // We are trying to write, but the peer has closed the connection.
        // Return "broken pipe" to signal this is a fatal write error.
        returned_status = make_error_code(errc::broken_pipe);
        break;
      }

      LOG(FATAL) << "Unexpected engine opcode: " << push_result.engine_opcode;
      returned_status = make_error_code(errc::operation_not_permitted);
      break;
    }  // push_result.engine_opcode < 0
  }    // while

  if (total_bytes_sent > 0) {
    DVSOCK(3) << "TrySend returning " << total_bytes_sent << " bytes";

    // The user interprets a full write as completion and may not call the socket again.
    // To prevent stranding pending TLS output (causing deadlocks), we must flush
    // it asynchronously in the background.
    size_t engine_output_pending = engine_->OutputPending();
    if ((total_bytes_sent == iovec_total_bytes) && (engine_output_pending > 0)) {
      DVSOCK(3) << "TrySend success but OutputPending=" << engine_output_pending
                << ". Offloading TLS engine's flush to background.";

      // Background write: the user treats a full write as done and may not call us again, so drain
      // the engine output in the background. No reader is waiting on this write, so it needs no
      // wake.
      StartAsyncWrite([](io::Result<size_t>) {});
    }

    return total_bytes_sent;
  }
  if (!returned_status) {
    return 0;  // No error, Clean EOF case
  }
  return make_unexpected(returned_status);
}

io::Result<size_t> TlsSocket::TryRecv(io::MutableBytes buf) {
  auto& socket_flags = flags_;
  size_t total_bytes_read{};
  bool write_in_progress{socket_flags.write_in_progress()};
  std::error_code returned_status{};  // init to no error

  // A user-level TryRecv() call is mutually exclusive with a blocking Recv().
  // We check for the flag set by Recv/RecvMsg to detect this usage error.
  if (socket_flags.user_recv_in_progress()) {
    LOG(DFATAL) << "Usage Error: A blocking Recv/RecvMsg is already in progress on this socket.";
    return make_unexpected(make_error_code(errc::operation_in_progress));
  }

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
      // Check for a write conflict:
      // Write conflicts are expected: The TLS state machine (e.g., renegotiation) may trigger
      // writes during a read operation.
      if (write_in_progress) {
        if (socket_flags.drain_engine_in_flight()) {
          // The in-flight write is THIS socket's own recv-path engine-output drain, so a wake IS
          // coming: surface EAGAIN, not EBUSY. See TryRecv's result-code contract in tls_socket.h.
          returned_status = make_error_code(errc::resource_unavailable_try_again);
        } else {
          // A genuinely concurrent, non-waking write from another context is using the socket, so
          // we could not read this time and NO wake is coming: return EBUSY so the caller keeps
          // expecting input and retries on a later pass. See TryRecv's result-code contract in
          // tls_socket.h.
          returned_status = make_error_code(errc::device_or_resource_busy);
        }
        break;
      }
      ///////////////////////////////////////////////////////////////
      // Handle Pending Output from TLS engine to upstream socket (write_in_progress is false)
      // If the engine generated TLS data (handshake/alerts), flush it now.
      // Otherwise, skip to reading.
      size_t output_pending_bytes{engine_->OutputPending()};
      DCHECK((read_result != Engine::NEED_WRITE) || (output_pending_bytes > 0))
          << "SSL BUG: Engine demands a write but provided no data.";
      if (output_pending_bytes > 0) {
        auto output_buf{engine_->PeekOutputBuf()};
        auto send_result{next_sock_->TrySend(output_buf)};
        if (!send_result) {
          // Nothing was sent, so we never got to the actual read.
          auto send_ec = send_result.error();
          if (send_ec == errc::resource_unavailable_try_again ||
              send_ec == errc::operation_would_block) {
            // Send buffer is full: drain this engine's output in a background write and return
            // EAGAIN. That write's completion re-arms the read via the recv callback, so a wake IS
            // coming - the caller may clear its pending-input flag and park. See TryRecv's
            // result-code contract in tls_socket.h.
            StartAsyncWriteForTryRecv();
            returned_status = make_error_code(errc::resource_unavailable_try_again);
          } else {
            returned_status = send_ec;
          }
          break;
        }
        engine_->ConsumeOutputBuf(*send_result);

        // Partial send: the buffer filled mid-write, so the rest of our output is still stuck and
        // we never reached the read. Same handling as the full-block case above - trigger a
        // background write and return EAGAIN (a wake is coming via the recv callback). Log at debug
        // only - this is normal backpressure, not an error.
        if ((*send_result) < output_buf.size()) {
          StartAsyncWriteForTryRecv();
          returned_status = make_error_code(errc::resource_unavailable_try_again);
          DVSOCK(1) << "TlsSocket::TryRecv: partial upstream send " << (*send_result) << "/"
                    << output_buf.size() << " bytes; started background engine-output drain";
          break;
        }

        // If we handled a NEED_WRITE successfully, we loop again to see what engine wants next
        if (read_result == Engine::NEED_WRITE)
          continue;
      }
      ///////////////////////////////////////////////////////////////

      ///////////////////////////////////////////////////////////////
      // Handle Pending Reads From Upstream Socket
      // An internal read (blocking) might be in progress from another fiber (e.g. during a
      // write). This is not a user error, but a temporary resource contention.
      if (socket_flags.read_in_progress()) {
        // A read is using the socket, so we could not read this call. The reason does not matter:
        // data may still be waiting, so return EBUSY and the caller keeps expecting input. See
        // TryRecv's result-code contract in tls_socket.h.
        DVSOCK(3) << "TryRecv conflict with internal read in progress, returning busy";
        returned_status = make_error_code(errc::device_or_resource_busy);
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

      // Kernel read failed. Normalize would-block to EAGAIN (the contract's single "drained, stop
      // expecting input" code, see tls_socket.h); any other error is fatal - propagate as-is.
      auto recv_ec = recv_result.error();
      if (recv_ec == errc::operation_would_block)
        recv_ec = make_error_code(errc::resource_unavailable_try_again);
      returned_status = recv_ec;
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

}  // namespace tls
}  // namespace util

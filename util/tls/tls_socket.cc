// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/tls/tls_socket.h"

#include <openssl/err.h>

#include <algorithm>

#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"
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
    Engine::OpResult op_result = engine_->WriteBuf(prefix);
    CHECK_EQ(unsigned(op_result), prefix.size());
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

  return HandleUpstreamWrite();
}

auto TlsSocket::HandleUpstreamRead() -> error_code {
  RETURN_ON_ERROR(MaybeSendOutput());

  if (state_ & READ_IN_PROGRESS) {
    // We need to Yield because otherwise we might end up in an infinite loop.
    // See also comments in MaybeSendOutput.
    ThisFiber::Yield();
    VSOCK(1) << "HandleUpstreamRead: Yielded";
    return error_code{};
  }

  auto mut_buf = engine_->PeekInputBuf();
  state_ |= READ_IN_PROGRESS;
  io::Result<size_t> esz = next_sock_->Recv(mut_buf, 0);
  state_ &= ~READ_IN_PROGRESS;
  if (!esz) {
    // log any errors as well as situations where we have unflushed output.
    if (esz.error() != errc::connection_aborted || engine_->OutputPending() > 0) {
      VSOCK(1) << "HandleUpstreamRead failed " << esz.error();
    }
    return esz.error();
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

  // we do not allow concurrent writes from multiple fibers.
  state_ |= WRITE_IN_PROGRESS;
  do {
    io::Result<size_t> write_result = next_sock_->WriteSome(buffer);

    DCHECK(engine_);
    if (!write_result) {
      state_ &= ~WRITE_IN_PROGRESS;

      // broken_pipe - happens when the other side closes the connection. do not log this.
      if (write_result.error() != errc::broken_pipe) {
        VSOCK(1) << "HandleUpstreamWrite failed " << write_result.error();
      }

      return write_result.error();
    }
    CHECK_GT(*write_result, 0u);

    upstream_write_ += *write_result;
    engine_->ConsumeOutputBuf(*write_result);

    // We could preempt while calling WriteSome, and the engine could get more data to write.
    // Therefore we sync the buffer.
    buffer = engine_->PeekOutputBuf();
  } while (!buffer.empty());

  DCHECK_EQ(engine_->OutputPending(), 0u);

  state_ &= ~WRITE_IN_PROGRESS;

  return error_code{};
}

error_code TlsSocket::HandleOp(int op_val) {
  switch (op_val) {
    case Engine::EOF_STREAM:
      VLOG(1) << "EOF_STREAM received " << next_sock_->native_handle();
      return make_error_code(errc::connection_aborted);
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
  LOG(FATAL) << "Not implemented";
}

void TlsSocket::ResetOnRecvHook() {
  LOG(FATAL) << "Not implemented";
}

io::Result<size_t> TlsSocket::TrySend(io::Bytes buf) {
  LOG(DFATAL) << "Not implemented";
  return 0;
}

io::Result<size_t> TlsSocket::TrySend(const iovec* v, uint32_t len) {
  LOG(DFATAL) << "Not implemented";
  return 0;
}

io::Result<size_t> TlsSocket::TryRecv(io::MutableBytes buf) {
  LOG(DFATAL) << "Not implemented";
  return 0;
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
    // log any errors as well as situations where we have unflushed output.
    if (read_result.error() != errc::connection_aborted || owner_->engine_->OutputPending() > 0) {
      VLOG(1) << "sock[" << owner_->native_handle() << "], state " << int(owner_->state_)
              << ", write_total:" << owner_->upstream_write_ << " "
              << " pending output: " << owner_->engine_->OutputPending() << " "
              << "AsyncProgressCb failed " << read_result.error();
    }
    // Erronous path. Apply the completion callback and exit.
    CompleteAsyncReq(read_result);
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

  owner_->next_sock_->AsyncReadSome(&scratch, 1, [this](auto res) { this->AsyncReadProgressCb(res); });
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

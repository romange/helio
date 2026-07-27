// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <system_error>
#include <utility>

#include "base/logging.h"
#include "util/tls/tls_engine.h"
#include "util/tls/tls_socket.h"

namespace util {
namespace tls {

using namespace std;
using nonstd::make_unexpected;

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
  owner_->ClearInProgressAndNotify(READ_IN_PROGRESS);
  RunPending();
  if (!read_result) {
    // Erroneous path. Apply the completion callback and exit.
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
    owner_->ClearInProgressAndNotify(WRITE_IN_PROGRESS);

    // broken_pipe - happens when the other side closes the connection. do not log this.
    if (write_result.error() != errc::broken_pipe) {
      VLOG(1) << "sock[" << owner_->native_handle() << "], state " << int(owner_->state_)
              << ", write_total:" << owner_->upstream_write_ << " "
              << " pending output: " << owner_->engine_->OutputPending()
              << " HandleUpstreamAsyncWrite failed " << write_result.error();
    }

    // We are done. Erroneous exit.
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

  owner_->ClearInProgressAndNotify(WRITE_IN_PROGRESS);
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

  // Check if this is a "flush-only" request (from TrySend, for example)
  if (vec_ == nullptr) {
    // We have flushed the pending buffer (AsyncWriteProgressCb ensures this before calling us),
    // and we have no new data to push. We are finished.
    CompleteAsyncReq(0);
    return;
  }

  // We wrote some therefore we can complete
  if (engine_written_ > 0) {
    CompleteAsyncReq(engine_written_);
    return;
  }
  // We need to call PushUserDataToEngine again
  PushResult push_res = owner_->PushUserDataToEngine(vec_, len_);
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
  PushResult push_res = PushUserDataToEngine(v, len);

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

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <system_error>
#include <utility>

#include "base/logging.h"
#include "util/tls/tls_async_io.h"
#include "util/tls/tls_async_req.h"
#include "util/tls/tls_engine.h"
#include "util/tls/tls_socket.h"

namespace util {
namespace tls {

using namespace std;
using nonstd::make_unexpected;

TlsAsyncIo::TlsAsyncIo(TlsSocket* owner) : owner_(owner) {
}

TlsAsyncIo::~TlsAsyncIo() = default;

int TlsAsyncIo::EngineRead(const iovec* v) {
  int op_val = owner_->engine_->Read(reinterpret_cast<uint8_t*>(v->iov_base), v->iov_len);
  DVLOG(2) << "Engine::Read tried to read " << v->iov_len << " bytes, got " << op_val;
  return op_val;
}

TlsAsyncIo::PushResult TlsAsyncIo::PushUserDataToEngine(const iovec* v, uint32_t len) {
  TlsSocket::PushResult result = owner_->PushUserDataToEngine(v, len);
  return {result.written, result.engine_opcode};
}

size_t TlsAsyncIo::EngineOutputPending() const {
  return owner_->engine_->OutputPending();
}

void TlsAsyncIo::EngineCommitInput(size_t size) {
  owner_->engine_->CommitInput(size);
}

void TlsAsyncIo::EngineConsumeOutput(size_t size) {
  owner_->upstream_write_ += size;
  owner_->engine_->ConsumeOutputBuf(size);
}

bool TlsAsyncIo::read_in_progress() const {
  return owner_->flags_.read_in_progress();
}

bool TlsAsyncIo::write_in_progress() const {
  return owner_->flags_.write_in_progress();
}

void TlsAsyncIo::clear_io_in_progress_and_notify(IoFlag flag) {
  const uint8_t owner_flag = (flag == IoFlag::kReadInProgress) ? TlsSocket::READ_IN_PROGRESS
                                                               : TlsSocket::WRITE_IN_PROGRESS;
  owner_->flags_.clear_io_in_progress_and_notify(owner_flag);
}

uint8_t TlsAsyncIo::flags_bits() const {
  return owner_->flags_.bits();
}

size_t TlsAsyncIo::upstream_write() const {
  return owner_->upstream_write_;
}

FiberSocketBase::native_handle_type TlsAsyncIo::native_handle() const {
  return owner_->native_handle();
}

void TlsAsyncIo::StartUpstreamRead(iovec* scratch, io::AsyncProgressCb cb) {
  auto buffer = owner_->engine_->PeekInputBuf();
  owner_->flags_.set_read_in_progress();
  scratch->iov_base = const_cast<uint8_t*>(buffer.data());
  scratch->iov_len = buffer.size();
  owner_->next_sock_->AsyncReadSome(scratch, 1, std::move(cb));
}

void TlsAsyncIo::StartUpstreamWrite(iovec* scratch, io::AsyncProgressCb cb) {
  Engine::Buffer buffer = owner_->engine_->PeekOutputBuf();
  DCHECK(!buffer.empty());
  DCHECK(!owner_->flags_.write_in_progress());
  DVLOG(2) << "StartUpstreamWrite " << buffer.size();
  owner_->flags_.set_write_in_progress();
  scratch->iov_base = const_cast<uint8_t*>(buffer.data());
  scratch->iov_len = buffer.size();
  owner_->next_sock_->AsyncWriteSome(scratch, 1, std::move(cb));
}

bool TlsAsyncIo::ContinueUpstreamWrite(iovec* scratch, io::AsyncProgressCb cb) {
  Engine::Buffer buffer = owner_->engine_->PeekOutputBuf();
  if (buffer.empty()) {
    return false;
  }
  DCHECK(owner_->flags_.write_in_progress());
  scratch->iov_base = const_cast<uint8_t*>(buffer.data());
  scratch->iov_len = buffer.size();
  owner_->next_sock_->AsyncWriteSome(scratch, 1, std::move(cb));
  return true;
}

void TlsAsyncIo::StartAsyncWrite(io::AsyncProgressCb async_write_cb) {
  // Hard CHECK: overwriting a live request would free a TlsAsyncReq still referenced by in-flight
  // AsyncWriteSome callbacks (use-after-free). Callers guarantee no write is in flight
  // (TrySend/TryRecv bail out early on WRITE_IN_PROGRESS), so this never fires in correct runs.
  CHECK(!async_write_req_);
  DCHECK_GT(EngineOutputPending(), 0u);
  // (vec, len) = (nullptr, 0): no new user bytes, only the engine's buffered output is sent.
  // AsyncRoleBasedAction treats a WRITER with vec_ == nullptr as output-only and ends when drained.
  async_write_req_ = std::make_unique<TlsAsyncReq>(this, std::move(async_write_cb), nullptr, 0,
                                                   TlsAsyncReq::WRITER);
  async_write_req_->StartUpstreamWrite();
}

void TlsAsyncReq::MaybeSendOutputAsyncWithRead() {
  if (async_io_->EngineOutputPending() != 0) {
    // Once the networking socket completes the write, it will start the read path
    // We use this bool to signal this.
    should_read_ = true;
    StartUpstreamWrite();
    return;
  }

  StartUpstreamRead();
}

void TlsAsyncReq::AsyncReadProgressCb(io::Result<size_t> read_result) {
  async_io_->clear_io_in_progress_and_notify(TlsAsyncIo::IoFlag::kReadInProgress);
  async_io_->RunPending();
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
  async_io_->EngineCommitInput(*read_result);
  AsyncRoleBasedAction();
}

void TlsAsyncReq::StartUpstreamRead() {
  // Even if we early return below we still should not try to read. When we
  // wake up we will poll the SSL engine which will dictate the next action/step.
  should_read_ = false;
  if (async_io_->read_in_progress()) {
    auto* prev = std::exchange(async_io_->blocked_async_req_, this);
    CHECK(prev == nullptr);
    return;
  }

  async_io_->StartUpstreamRead(&scratch_iovec_,
                               [this](auto res) { this->AsyncReadProgressCb(res); });
}

void TlsAsyncReq::CompleteAsyncReq(io::Result<size_t> result) {
  std::unique_ptr<TlsAsyncReq> current;
  if (role_ == Role::READER) {
    current = std::move(async_io_->async_read_req_);
  } else {
    current = std::move(async_io_->async_write_req_);
  }
  CHECK(current.get() == this);
  current->caller_completion_cb_(result);
}

void TlsAsyncReq::HandleOpAsync(int op_val) {
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

void TlsAsyncIo::AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  // Engine read
  CHECK(!async_read_req_);

  Engine::OpResult op_val = EngineRead(v);
  // We read some data from the engine. Satisfy the request and return.
  if (op_val > 0) {
    return cb(op_val);
  }

  if (op_val == Engine::EOF_ABRUPT) {
    VLOG(1) << "EOF_ABRUPT received " << native_handle();
    return cb(make_unexpected(make_error_code(errc::connection_reset)));
  }
  if (op_val == Engine::EOF_GRACEFUL) {
    VLOG(1) << "EOF_GRACEFUL received " << native_handle();
    return cb(0);  // return 0 to indicate EOF
  }

  // We could not read from the engine. Dispatch async op.
  DCHECK_GT(len, 0u);
  async_read_req_ = std::make_unique<TlsAsyncReq>(this, std::move(cb), v, len, TlsAsyncReq::READER);
  async_read_req_->HandleOpAsync(op_val);
}

void TlsAsyncReq::AsyncWriteProgressCb(io::Result<size_t> write_result) {
  if (!write_result) {
    async_io_->clear_io_in_progress_and_notify(TlsAsyncIo::IoFlag::kWriteInProgress);

    // broken_pipe - happens when the other side closes the connection. do not log this.
    if (write_result.error() != errc::broken_pipe) {
      VLOG(1) << "sock[" << async_io_->native_handle() << "], state "
              << int(async_io_->flags_bits()) << ", write_total:" << async_io_->upstream_write()
              << " pending output: " << async_io_->EngineOutputPending()
              << " HandleUpstreamAsyncWrite failed " << write_result.error();
    }

    // We are done. Erroneous exit.
    async_io_->RunPending();
    CompleteAsyncReq(write_result);
    return;
  }

  CHECK_GT(*write_result, 0u);
  async_io_->EngineConsumeOutput(*write_result);

  // Re-arm the async write until we drive it to completion or error.
  // We would also like to avoid fragmented socket writes so we make sure we drain it here.
  if (async_io_->ContinueUpstreamWrite(&scratch_iovec_,
                                       [this](auto result) { AsyncWriteProgressCb(result); })) {
    return;
  }

  if (async_io_->EngineOutputPending() > 0) {
    LOG(DFATAL) << "ssl buffer is not empty with " << async_io_->EngineOutputPending()
                << " bytes. Async short write detected";
  }

  async_io_->clear_io_in_progress_and_notify(TlsAsyncIo::IoFlag::kWriteInProgress);
  async_io_->RunPending();

  // We are done with the write, check if we also need to read because we are
  // in NEED_READ_AND_MAYBE_WRITE state
  if (should_read_) {
    StartUpstreamRead();
    return;
  }

  AsyncRoleBasedAction();
}

void TlsAsyncReq::AsyncRoleBasedAction() {
  if (role_ == READER) {
    auto op_val = async_io_->EngineRead(vec_);
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
  TlsAsyncIo::PushResult push_res = async_io_->PushUserDataToEngine(vec_, len_);
  Engine::OpResult op_val = push_res.engine_opcode;
  engine_written_ = push_res.written;
  if (op_val < 0) {
    HandleOpAsync(op_val);
    return;
  }

  StartUpstreamWrite();
}

void TlsAsyncReq::StartUpstreamWrite() {
  if (async_io_->write_in_progress()) {
    CHECK(async_io_->blocked_async_req_ == nullptr);
    async_io_->blocked_async_req_ = this;
    return;
  }

  async_io_->StartUpstreamWrite(&scratch_iovec_,
                                [this](auto result) { AsyncWriteProgressCb(result); });
}

void TlsAsyncReq::MaybeSendOutputAsync() {
  if (async_io_->EngineOutputPending() == 0) {
    return;
  }

  if (async_io_->write_in_progress()) {
    CHECK(async_io_->blocked_async_req_ == nullptr);
    async_io_->blocked_async_req_ = this;
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
void TlsAsyncIo::AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  CHECK(!async_write_req_);

  // Write to the engine
  PushResult push_res = PushUserDataToEngine(v, len);

  async_write_req_ =
      std::make_unique<TlsAsyncReq>(this, std::move(cb), v, len, TlsAsyncReq::WRITER);
  async_write_req_->SetEngineWritten(push_res.written);
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

void TlsAsyncIo::RunPending() {
  if (!blocked_async_req_) {
    return;
  }

  auto* blocked = std::exchange(blocked_async_req_, nullptr);

  if (blocked->should_read_) {
    blocked->StartUpstreamRead();
    return;
  }

  if (blocked->role_ == TlsAsyncReq::WRITER) {
    auto current = std::move(async_write_req_);
    AsyncWriteSome(current->vec_, current->len_, std::move(current->caller_completion_cb_));
    return;
  }
  auto current = std::move(async_read_req_);
  AsyncReadSome(current->vec_, current->len_, std::move(current->caller_completion_cb_));
}

}  // namespace tls
}  // namespace util

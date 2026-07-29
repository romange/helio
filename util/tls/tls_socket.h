// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <openssl/ssl.h>

#include <memory>

#include "util/fiber_socket_base.h"
#include "util/fibers/synchronization.h"
#include "util/tls/tls_engine.h"

namespace util {
namespace fb2 {
class AsyncTlsSocketNeedWrite;
}  // namespace fb2

namespace tls {

class Engine;

class TlsSocket final : public FiberSocketBase {
  friend class TestDelegator;  // for testing only
 public:
  using Buffer = Engine::Buffer;
  using FiberSocketBase::endpoint_type;

  // --- Construction / destruction ---
  TlsSocket(std::unique_ptr<FiberSocketBase> next);

  // Takes ownership of next
  TlsSocket(FiberSocketBase* next);

  ~TlsSocket();

  // --- FiberSocketBase overrides ---

  error_code Shutdown(int how) final override;

  AcceptResult Accept() final override;

  // The endpoint should not really pass here, it is to keep
  // the interface with FiberSocketBase.
  error_code Connect(const endpoint_type& ep,
                     std::function<void(int)> on_pre_connect = {}) final override;

  error_code Close() final override;

  bool IsOpen() const final override {
    return next_sock_->IsOpen();
  }

  void set_timeout(uint32_t msec) final override {
    next_sock_->set_timeout(msec);
  }

  uint32_t timeout() const final override {
    return next_sock_->timeout();
  }

  io::Result<size_t> RecvMsg(const msghdr& msg, int flags) final override;
  io::Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  ::io::Result<size_t> WriteSome(const iovec* ptr, uint32_t len) final override;
  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) final override;
  void AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) final override;

  endpoint_type LocalEndpoint() const override;
  endpoint_type RemoteEndpoint() const override;

  void RegisterOnErrorCb(std::function<void(uint32_t)> cb) override;
  void CancelOnErrorCb() override;

  bool IsUDS() const override;

  using FiberSocketBase::native_handle_type;
  native_handle_type native_handle() const override;

  error_code Create(unsigned short protocol_family = 2) override;

  ABSL_MUST_USE_RESULT error_code Bind(const struct sockaddr* bind_addr,
                                       unsigned addr_len) override;
  ABSL_MUST_USE_RESULT error_code Listen(unsigned backlog) override;

  ABSL_MUST_USE_RESULT error_code Listen(uint16_t port, unsigned backlog) override;

  ABSL_MUST_USE_RESULT error_code ListenUDS(const char* path, mode_t permissions,
                                            unsigned backlog) override;

  void SetProactor(ProactorBase* p) override;

  void RegisterOnRecv(OnRecvCb cb) override;
  void ResetOnRecvHook() override {
    on_recv_cb_ = {};
    next_sock_->ResetOnRecvHook();
  }

  // Result-code contract for the non-blocking TrySend / TryRecv. A returned code describes the
  // socket's state, not what the caller must do. One correctness rule matters for reads: TryRecv
  // can report a short-read or EBUSY WITHOUT reaching the kernel, so unread bytes may still be
  // buffered out of the caller's sight - it must keep expecting input and retry until it gets a
  // clean EAGAIN, otherwise it may hang waiting for a reply that is never read.
  //   * >0: progress this call, but possibly short and NOT a completion signal - more data/room
  //     may still be pending (in the kernel or the TLS engine). Keep the operation live and retry
  //     later, even without a new readiness notification.
  //   * resource_unavailable_try_again (EAGAIN):
  //     - TryRecv: nothing to read now, but a wake IS coming - safe to stop expecting input and
  //     park (only if a recv callback was registered via RegisterOnRecv; otherwise poll-retry).
  //     Covers both a drained kernel and output that TryRecv had to defer to a background write.
  //     - TrySend: generic "would block, retry later" (full send buffer, or local TLS concurrency).
  //   * device_or_resource_busy (EBUSY): TryRecv only - a genuinely concurrent, non-waking
  //     context (another fiber's in-progress write or read) holds the socket, so the kernel was
  //     not consulted and, unlike EAGAIN, NO wake is coming. This holds unconditionally: the
  //     socket's OWN recv-path engine-output drain is NOT reported here - its completion re-arms
  //     the read via the recv callback, so that write surfaces EAGAIN above. Keep expecting input
  //     and retry on a later pass (not a tight loop); do not park.
  //   * connection_reset / broken_pipe / etc.: fatal - the connection is gone - propagate.
  io::Result<size_t> TrySend(io::Bytes buf) override;
  io::Result<size_t> TrySend(const iovec* v, uint32_t len) override;
  io::Result<size_t> TryRecv(io::MutableBytes buf) override;

  // --- TlsSocket-specific API (not inherited from FiberSocketBase) ---

  // prefix points to the buffer that optionally holds first bytes from the TLS data stream.
  void InitSSL(SSL_CTX* context, Buffer prefix = {});

  SSL* ssl_handle();

 private:
  // Declared first (destroyed last): some blocking/non-blocking/async states below may still
  // touch the upstream socket while unwinding, so it must outlive them.
  std::unique_ptr<FiberSocketBase> next_sock_;

  // --- TLS engine bridge: feeds the engine from the upstream socket and/or the user, and
  // flushes the engine's encrypted output back to the upstream socket ---

  // TLS engine bridge - Common: used by all three paths (blocking, non-blocking, and async) -

  // Both opcode and written can be set.
  struct PushResult {
    size_t written = 0;
    int engine_opcode = 0;  // Engine::OpCode
  };

  // Pure in-memory engine/crypto operation, does not touch next_sock_. Feeds user-supplied
  // plaintext into the engine to be encrypted, until either everything is written, or an error
  // occurs, or the engine needs to flush its output. It's up to the caller to send the output
  // buffer to the network.
  PushResult PushUserDataToEngine(const iovec* ptr, uint32_t len);

  // - TLS engine bridge- Blocking path only -

  /// Feed encrypted data from the TLS engine into the network socket.
  ABSL_MUST_USE_RESULT error_code MaybeSendEngineOutput();

  /// Read encrypted data from the network socket and feed it into the TLS engine.
  ABSL_MUST_USE_RESULT error_code HandleUpstreamRead();

  ABSL_MUST_USE_RESULT error_code HandleUpstreamWrite();
  ABSL_MUST_USE_RESULT error_code HandleEngineOp(int op);

  // - TLS engine bridge - Async path only -

  // Async-notification counterpart of HandleUpstreamRead: when arriving data appears as a
  // notification instead of a blocking read, this copies it into the engine's input buffer;
  // retry/error notifications are simply forwarded to the recv callback.
  void HandleRecvNotification(const RecvNotification& rn, const OnRecvCb& recv_cb);

  std::unique_ptr<Engine> engine_;
  size_t upstream_write_ = 0;
  // Stored recv callback. Needed so that a read-retry or error can be delivered to the caller
  // asynchronously, from whichever internal path ends up producing it, without the caller having
  // to poll or re-register.
  OnRecvCb on_recv_cb_;

  // --- Background engine-output draining: implemented via the async write mechanism ---

  // Starts a background write that sends the engine's already-buffered output to next_sock_ and
  // calls `async_write_cb` when it finishes (or errors). Precondition: no async write is already
  // in flight.
  void StartAsyncWrite(io::AsyncProgressCb async_write_cb);

  // Starts a background write (via StartAsyncWrite) to drain engine output that did not fit
  // inline in the socket buffer. When the write finishes it wakes the reader through on_recv_cb_
  // (a read-retry RecvCompletion on success, the error on failure), which re-arms the deferred
  // read.
  void StartAsyncWriteForTryRecv();

  enum {
    WRITE_IN_PROGRESS = 1,
    READ_IN_PROGRESS = 1 << 1,
    SHUTDOWN_IN_PROGRESS = 1 << 2,
    SHUTDOWN_DONE = 1 << 3,
    USER_RECV_IN_PROGRESS = 1 << 4,
    // A recv-path background write draining the engine's output is in flight.
    RECV_DRAIN_ENGINE_IN_FLIGHT = 1 << 5,
  };

  // Shared state and synchronization for the blocking, non-blocking, and async paths.
  class SocketFlags {
   public:
    // Test-only whole-state replacement.
    void overwrite(uint8_t state) {
      state_ = state;
    }
    uint8_t bits() const {
      return state_;
    }

    // Predicates
    bool write_in_progress() const {
      return (state_ & WRITE_IN_PROGRESS) != 0;
    }
    bool read_in_progress() const {
      return (state_ & READ_IN_PROGRESS) != 0;
    }
    bool shutdown_in_progress() const {
      return (state_ & SHUTDOWN_IN_PROGRESS) != 0;
    }
    bool shutdown_done() const {
      return (state_ & SHUTDOWN_DONE) != 0;
    }
    bool user_recv_in_progress() const {
      return (state_ & USER_RECV_IN_PROGRESS) != 0;
    }
    bool drain_engine_in_flight() const {
      return (state_ & RECV_DRAIN_ENGINE_IN_FLIGHT) != 0;
    }
    bool io_in_progress() const {
      return write_in_progress() || read_in_progress();
    }
    bool io_or_shutdown_in_progress() const {
      return io_in_progress() || shutdown_in_progress();
    }

    // Setters
    void set_write_in_progress() {
      state_ |= WRITE_IN_PROGRESS;
    }
    void set_read_in_progress() {
      state_ |= READ_IN_PROGRESS;
    }
    void set_shutdown_in_progress() {
      state_ |= SHUTDOWN_IN_PROGRESS;
    }
    void set_user_recv_in_progress() {
      state_ |= USER_RECV_IN_PROGRESS;
    }
    void set_drain_engine_in_flight() {
      state_ |= RECV_DRAIN_ENGINE_IN_FLIGHT;
    }

    // Clearers
    void clear_user_recv_in_progress() {
      state_ &= ~USER_RECV_IN_PROGRESS;
    }
    void clear_drain_engine_in_flight() {
      state_ &= ~RECV_DRAIN_ENGINE_IN_FLIGHT;
    }

    // In-progress operation synchronization.
    // Clears exactly one completed read/write bit, then wakes completion waiters.
    void clear_io_in_progress_and_notify(uint8_t mask);
    void wait_for_write_completion() {
      wait_until_clear(WRITE_IN_PROGRESS);
    }
    void wait_for_read_completion() {
      wait_until_clear(READ_IN_PROGRESS);
    }
    void wait_for_io_completion() {
      wait_until_clear(WRITE_IN_PROGRESS | READ_IN_PROGRESS);
    }
    void wait_for_io_or_shutdown_completion() {
      wait_until_clear(WRITE_IN_PROGRESS | READ_IN_PROGRESS | SHUTDOWN_IN_PROGRESS);
    }
    void complete_shutdown();

   private:
    // Wait masks may combine WRITE_IN_PROGRESS, READ_IN_PROGRESS, and SHUTDOWN_IN_PROGRESS.
    void wait_until_clear(uint8_t mask);
    void set_shutdown_done() {
      state_ |= SHUTDOWN_DONE;
    }
    void clear_shutdown_in_progress() {
      state_ &= ~SHUTDOWN_IN_PROGRESS;
    }

    uint8_t state_{0};
    fb2::CondVarAny cv_;
  };

  // --- Async I/O primitives: drive a read or write to completion via callbacks instead of
  // blocking the fiber ---
  class AsyncReq {
   public:
    enum Role : std::uint8_t { READER, WRITER };

    AsyncReq(TlsSocket* owner, io::AsyncProgressCb cb, const iovec* v, uint32_t len, Role role)
        : owner_(owner), caller_completion_cb_(std::move(cb)), vec_(v), len_(len), role_(role) {
    }

    void HandleOpAsync(int op_val);
    void StartUpstreamWrite();
    void SetEngineWritten(size_t written) {
      engine_written_ = written;
    }

   private:
    TlsSocket* owner_;
    // Callback passed from the user.
    io::AsyncProgressCb caller_completion_cb_;

    const iovec* vec_;
    uint32_t len_;

    Role role_;

    iovec scratch_iovec_ = {};

    size_t engine_written_ = 0;
    bool should_read_ = false;

    // Asynchronous helpers
    void MaybeSendOutputAsyncWithRead();
    void MaybeSendOutputAsync();

    void StartUpstreamRead();

    void CompleteAsyncReq(io::Result<size_t> result);

    void AsyncWriteProgressCb(io::Result<size_t> write_result);
    void AsyncReadProgressCb(io::Result<size_t> result);

    // Both reader and writer can at any point dispatch a RW operation.
    // So, AsyncWriteProgress* must decide how to complete based on its role and this
    // function extracts the common execution paths.
    void AsyncRoleBasedAction();

    // Helper function to handle WRITE_IN_PROGRESS and READ_IN_PROGRESS without preemption.
    // When an operation can't continue because there is already one in progress, it early returns
    // and copies itself to blocked_async_req_. When the in progress operation completes,
    // it resumes the one pending.
    void RunPending();
  };

  std::unique_ptr<AsyncReq> async_read_req_;
  std::unique_ptr<AsyncReq> async_write_req_;

  // Pending request that is blocked on WRITE_IN_PROGRESS or READ_IN_PROGRESS. Since we can't
  // preempt in function context, we simply subscribe the async request to the one in-flight and
  // once that completes it will also continue the one pending/blocked.
  AsyncReq* blocked_async_req_ = nullptr;

  // --- Testing-only members (not part of the public API) ---
  friend class fb2::AsyncTlsSocketNeedWrite;

  // This function simulates a corner case of AsyncReadSome: engine_->Read(...) returning
  // NEED_WRITE. This scenario reproduces roughly as follows:
  // 1. Client connects to server and server accepts.
  // 2. Handshake completes.
  // 3. Client stops reading from the socket -> no acks are sent.
  // 4. Server keeps sending data until TCP send buffers are full (because the client has not
  //    yet acked).
  // 5. Server calls SSL_renegotiate followed by SSL_handshake.
  // 6. Server calls AsyncRead, which calls engine->Read(), which should return NEED_WRITE
  //    because the state machine requires a protocol renegotiation and the internal buffers
  //    are full.
  // The idea is that when the server reads, the internal OpenSSL state machine needs to
  // exchange protocol data but cannot, because the TCP buffers are full and consequently the
  // internal BIO buffers are not yet flushed, so engine->Read() will return NEED_WRITE so that
  // the protocol renegotiation can kick in. Even though this scenario seems easy to simulate, it
  // does not reliably reproduce NEED_WRITE, so for now this function simulates it directly.
  void __DebugForceNeedWriteOnAsyncRead(const iovec* v, uint32_t len, io::AsyncProgressCb cb);

  // Used to test AsyncWrite  NEED_WRITE on first PushUserDataToEngine. As this scenario is
  // difficult to time, this function helps simulate it.
  void __DebugForceNeedWriteOnAsyncWrite(const iovec* v, uint32_t len, io::AsyncProgressCb cb);

  // - Owns the shared state bits and CV used by the blocking, non-blocking, and async paths. A
  // fiber that conflicts with an in-progress read, write, or shutdown waits on this CV instead of
  // spinning, then retries when the operation clears its bit and notifies waiters.
  // - Declared last so it is destroyed first, preserving the prior CV destruction order relative to
  // the members above.
  SocketFlags flags_;
};

}  // namespace tls
}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <openssl/ssl.h>

#include <memory>

#include "util/fiber_socket_base.h"
#include "util/tls/tls_engine.h"
#include "util/fibers/synchronization.h"

namespace util {
namespace tls {

class Engine;

class TlsSocket final : public FiberSocketBase {
 public:
  using Buffer = Engine::Buffer;
  using FiberSocketBase::endpoint_type;

  TlsSocket(std::unique_ptr<FiberSocketBase> next);

  // Takes ownership of next
  TlsSocket(FiberSocketBase* next);

  ~TlsSocket();

  // prefix points to the buffer that optionally holds first bytes from the TLS data stream.
  void InitSSL(SSL_CTX* context, Buffer prefix = {});

  error_code Shutdown(int how) final;

  AcceptResult Accept() final;

  // The endpoint should not really pass here, it is to keep
  // the interface with FiberSocketBase.
  error_code Connect(const endpoint_type& ep, std::function<void(int)> on_pre_connect = {}) final;

  error_code Close() final;

  bool IsOpen() const final {
    return next_sock_->IsOpen();
  }

  void set_timeout(uint32_t msec) final override {
    next_sock_->set_timeout(msec);
  }

  uint32_t timeout() const final override {
    return next_sock_->timeout();
  }

  io::Result<size_t> RecvMsg(const msghdr& msg, int flags) final;
  io::Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  ::io::Result<size_t> WriteSome(const iovec* ptr, uint32_t len) final;
  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) final;
  void AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) final;

  SSL* ssl_handle();

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

  virtual void SetProactor(ProactorBase* p) override;

  virtual void RegisterOnRecv(OnRecvCb cb) override;
  virtual void ResetOnRecvHook() override {
    next_sock_->ResetOnRecvHook();
  }

  io::Result<size_t> TrySend(io::Bytes buf) override;
  io::Result<size_t> TrySend(const iovec* v, uint32_t len) override;
  io::Result<size_t> TryRecv(io::MutableBytes buf) override;

  // * NOT PART OF THE API -- USED FOR TESTING PURPOSES ONLY *
  // This function is used to simulate a corner case of AsyncReadSome. In particular,
  // when engine_->Read(...) returns NEED_WRITE. According to chatgpt and google
  // This scenario reproduces "roughly" by:
  // 1. client connects to server and server accepts.
  // 2. handshake completes
  // 4. client stops reading from the socket -> no acks are sent
  // 5. server keeps sending data until TCP sent buffers are full (because client has not yet acked)
  // 6. server calls sll_renegotiate followed by an ssl_handshake
  // 7. server calls AsyncRead which calls engine->Read() which should return NEED_WRITE
  //    because the state machine requires a protocol renegotiation and the internal buffers
  //    are full.
  // So the idea is that when the server reads, the internal openssl state machine needs to
  // exchange protocol data but it can not because the TCP buffers are full and consequently
  // the internall BIO buffers are not yet flushed so the engine->Read() will return NEED_WRITE
  // such that the protocol renegotiation can kick in. Even though this scenario seems easy to
  // simulate it does not reproduce NEED_WRITE and for now I use this function to simulate it.
  void __DebugForceNeedWriteOnAsyncRead(const iovec* v, uint32_t len, io::AsyncProgressCb cb);

  // Used to test AsyncWrite  NEED_WRITE on first PushToEngine. As this scenario is difficult to
  // time, this function helps simulate it.
  void __DebugForceNeedWriteOnAsyncWrite(const iovec* v, uint32_t len, io::AsyncProgressCb cb);

 private:
  // Both opcode and written can be set.
  struct PushResult {
    size_t written = 0;
    int engine_opcode = 0;  // Engine::OpCode
  };

  // Pushes the buffers into input ssl buffer until either everything is written,
  // or an error occurs or the engine needs to flush its output. Does not interact with the network,
  // just with the engine. It's up to the caller to send the output buffer to the network.
  PushResult PushToEngine(const iovec* ptr, uint32_t len);

  /// Feed encrypted data from the TLS engine into the network socket.
  error_code MaybeSendOutput();
  error_code MaybeSendOutputAsync();

  /// Read encrypted data from the network socket and feed it into the TLS engine's input buffer.
  error_code ReadFromUpstreamSocket();
  error_code ReadFromUpstreamSocketAsync();

  /// Write all pending encrypted data from the TLS engine's output buffer to the network socket
  error_code WriteToUpstreamSocket();
  /// Attempts to write pending encrypted data from the TLS engine's output buffer to the network
  /// socket. May perform a partial write. caller should check OutputPending() and retry if needed.
  error_code WriteToUpstreamSocketAsync();

  error_code HandleOp(int op);
  error_code HandleOpAsync(int op);

  // Asynchronously receives and delivers decrypted data, handling TLS engine state.
  void RecvAsync(const RecvNotification& rn, const OnRecvCb& cb);

  std::unique_ptr<FiberSocketBase> next_sock_;
  std::unique_ptr<Engine> engine_;
  size_t upstream_write_ = 0;

  enum {
    WRITE_IN_PROGRESS = 1,
    READ_IN_PROGRESS = 2,
    SHUTDOWN_IN_PROGRESS = 4,
    SHUTDOWN_DONE = 8,
  };
  uint8_t state_{0};

  class AsyncReq {
   public:
    enum Role : std::uint8_t { READER, WRITER };

    AsyncReq(TlsSocket* owner, io::AsyncProgressCb cb, const iovec* v, uint32_t len, Role role)
        : owner_(owner), caller_completion_cb_(std::move(cb)), vec_(v), len_(len), role_(role) {
    }

    void HandleOpAsync(int op_val);
    void WriteToUpstreamSocketAsync();
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

    friend class TlsSocket;

  };  // class AsyncReq

  std::unique_ptr<AsyncReq> async_read_req_;
  std::unique_ptr<AsyncReq> async_write_req_;

  // Pending request that is blocked on WRITE_IN_PROGRESS or READ_IN_PROGRESS. Since we can't
  // preempt in function context, we simply subscribe the async request to the one in-flight and
  // once that completes it will also continue the one pending/blocked.
  AsyncReq* blocked_async_req_ = nullptr;

  fb2::CondVarAny block_concurrent_cv_;

  // Resumes any async request (read or write) that was blocked due to an in-progress operation.
  // Called when the current async operation completes.
  void ResumeBlockedAsyncReq();

  // State and helpers for managing a pending low-level async write (TrySend) to the underlying socket.
  // These handle scheduling, progress, and completion of the socket write operation.
  struct TrySendReq {
    iovec iov;
    bool active;
    error_code ec{};
  } try_send_req_{};

  void StartPendingTrySend();
  void ScheduleTrySendWrite();
  void HandleTrySendWrite(io::Result<size_t> write_result);
  void FinishPendingTrySend();
};

}  // namespace tls
}  // namespace util

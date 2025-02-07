// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <openssl/ssl.h>

#include <memory>
#include <optional>

#include "util/fiber_socket_base.h"
#include "util/tls/tls_engine.h"

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

  unsigned RecvProvided(unsigned buf_len, ProvidedBuffer* dest) final;
  void ReturnProvided(const ProvidedBuffer& pbuf) final;

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

 private:
  struct PushResult {
    size_t written = 0;
    int engine_opcode = 0;  // Engine::OpCode
  };

  // Pushes the buffers into input ssl buffer until either everything is written,
  // or an error occurs or the engine needs to flush its output. Does not interact with the network,
  // just with the engine. It's up to the caller to send the output buffer to the network.
  io::Result<PushResult> PushToEngine(const iovec* ptr, uint32_t len);

  /// Feed encrypted data from the TLS engine into the network socket.
  error_code MaybeSendOutput();

  /// Read encrypted data from the network socket and feed it into the TLS engine.
  error_code HandleUpstreamRead();

  error_code HandleUpstreamWrite();
  error_code HandleOp(int op);

  std::unique_ptr<FiberSocketBase> next_sock_;
  std::unique_ptr<Engine> engine_;
  size_t upstream_write_ = 0;

  struct AsyncReqBase {
    AsyncReqBase(TlsSocket* owner, io::AsyncProgressCb caller_cb, const iovec* vec, uint32_t len)
        : owner(owner), caller_completion_cb(std::move(caller_cb)), vec(vec), len(len) {
    }

    TlsSocket* owner;
    // Callback passed from the user.
    io::AsyncProgressCb caller_completion_cb;

    const iovec* vec;
    uint32_t len;

    std::function<void()> continuation;
    iovec scratch_iovec;

    // Asynchronous helpers
    void MaybeSendOutputAsync();

    void HandleUpstreamAsyncWrite(io::Result<size_t> write_result, Engine::Buffer buffer);
    void HandleUpstreamAsyncRead();

    void HandleOpAsync(int op_val);

    void StartUpstreamWrite();
    void StartUpstreamRead();

    virtual void Run() = 0;
  };

  struct AsyncWriteReq : AsyncReqBase {
    using AsyncReqBase::AsyncReqBase;

    // TODO simplify state transitions
    // TODO handle async yields to avoid deadlocks (see HandleOp)
    enum State { PushToEngine, HandleOpAsyncTag, MaybeSendOutputAsyncTag, Done };
    State state = PushToEngine;
    PushResult last_push;

    // Main loop
    void Run() override;
  };

  friend AsyncWriteReq;

  struct AsyncReadReq : AsyncReqBase {
    using AsyncReqBase::AsyncReqBase;

    Engine::MutableBuffer dest;
    size_t read_total = 0;

    // Main loop
    void Run() override;
  };

  friend AsyncReadReq;

  // TODO clean up the optional before we yield such that progress callback can dispatch another
  // async operation
  std::optional<AsyncWriteReq> async_write_req_;
  std::optional<AsyncReadReq> async_read_req_;

  enum { WRITE_IN_PROGRESS = 1, READ_IN_PROGRESS = 2, SHUTDOWN_IN_PROGRESS = 4, SHUTDOWN_DONE = 8 };
  uint8_t state_{0};
};

}  // namespace tls
}  // namespace util

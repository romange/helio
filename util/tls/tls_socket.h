// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <openssl/ssl.h>

#include <memory>

#include "util/fiber_socket_base.h"
#include "util/tls/tls_engine.h"

namespace util {
namespace tls {

class Engine;

class TlsSocket : public FiberSocketBase {
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
  error_code Connect(const endpoint_type&) final;

  error_code Close() final;

  bool IsOpen() const final {
    return next_sock_->IsOpen();
  }

  io::Result<size_t> RecvMsg(const msghdr& msg, int flags) final;
  io::Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0) override;

  ::io::Result<size_t> WriteSome(const iovec* ptr, uint32_t len) final;
  void AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) final;

  SSL* ssl_handle();

  endpoint_type LocalEndpoint() const override;
  endpoint_type RemoteEndpoint() const override;

  void RegisterOnErrorCb(std::function<void (uint32_t)> cb) override;
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

 private:
  io::Result<size_t> SendBuffer(Buffer buf);

  /// Feed encrypted data from the TLS engine into the network socket.
  error_code MaybeSendOutput();

  /// Read encrypted data from the network socket and feed it into the TLS engine.
  error_code HandleRead();

  std::unique_ptr<FiberSocketBase> next_sock_;
  std::unique_ptr<Engine> engine_;

  enum { WRITE_IN_PROGRESS = 1, READ_IN_PROGRESS = 2};
  uint8_t state_{0};
};

}  // namespace tls
}  // namespace util

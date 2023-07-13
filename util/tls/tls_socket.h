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
  TlsSocket(std::unique_ptr<FiberSocketBase>);

  ~TlsSocket();

  void InitSSL(SSL_CTX* context);

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

  // Enables caching the first n_bytes received in HandleRead()
  // such that it can be used later to downgrade tls to non-tls
  // connections
  void CacheOnce();

  using Buffer = Engine::Buffer;

  Buffer GetCachedBuffer() const;

  void PlaceBufferInCache(Buffer buffer, size_t n_bytes);
  using FiberSocketBase::endpoint_type;
  endpoint_type LocalEndpoint() const override;
  endpoint_type RemoteEndpoint() const override;

  uint32_t PollEvent(uint32_t event_mask, std::function<void(uint32_t)> cb) override;

  uint32_t CancelPoll(uint32_t id) override;

  bool IsUDS() const override;
  bool IsDirect() const override;

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
  error_code MaybeSendOutput();
  error_code HandleRead();

  std::unique_ptr<FiberSocketBase> next_sock_;
  std::unique_ptr<Engine> engine_;

  // Used to signal HandleRead() to cache the first n_bytes
  bool cache_{false};
  size_t n_bytes_{0};
  Buffer cached_bytes_;
};

}  // namespace tls
}  // namespace util

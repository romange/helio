// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <openssl/ssl.h>

#include "util/fiber_socket_base.h"
#include "util/tls/tls_engine.h"

namespace util {
namespace tls {

class Engine;

class TlsSocket : public FiberSocketBase {
 public:
  //! Does not take ownership over next. 'next' must be live as long as we use
  //! TlsSocket instance.
  TlsSocket(FiberSocketBase* next = nullptr);

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

 private:
  error_code MaybeSendOutput();
  error_code HandleRead();

  FiberSocketBase* next_sock_;
  std::unique_ptr<Engine> engine_;

  // Used to signal HandleRead() to cache the first n_bytes
  bool cache_{false};
  size_t n_bytes_{0};
  Buffer cached_bytes_;
};

}  // namespace tls
}  // namespace util

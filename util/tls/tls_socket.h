// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <openssl/ssl.h>

#include "util/fiber_socket_base.h"


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

  error_code Connect(const endpoint_type& ep) final;

  error_code Close() final;

  bool IsOpen() const final {
    return next_sock_->IsOpen();
  }

  ::io::Result<size_t> RecvMsg(const msghdr& msg, int flags) final;

  ::io::Result<size_t> WriteSome(const iovec* ptr, uint32_t len) final;
  void AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb);

  SSL* ssl_handle();

 private:
  error_code MaybeSendOutput();
  error_code HandleRead();

  FiberSocketBase* next_sock_;
  std::unique_ptr<Engine> engine_;
};

}  // namespace tls
}  // namespace util

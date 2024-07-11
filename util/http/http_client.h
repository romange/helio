// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <openssl/ssl.h>  // required by SSL_CTX

#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>
#include <string_view>

#include "util/asio_stream_adapter.h"
#include "util/fibers/proactor_base.h"

namespace util {

class FiberSocketBase;

namespace http {

/*
  Single threaded, fiber-friendly synchronous client: Upon IO block, the calling fiber blocks
  but the thread can switch to other active fibers.
*/
class Client {
 public:
  using Response = boost::beast::http::response<boost::beast::http::dynamic_body>;
  using Verb = boost::beast::http::verb;
  using BoostError = boost::system::error_code;

  explicit Client(fb2::ProactorBase* proactor);
  ~Client();

  std::error_code Connect(std::string_view host, std::string_view service);
  std::error_code Reconnect();

  /*! @brief Sends http request but does not read response back.
   *
   *  Possibly retries and reconnects if there are problems with connection.
   *  See set_retry_count(uint32_t) method.
   */
  template <typename Req> BoostError Send(const Req& req);

  /*! @brief Sends http request and reads response back.
   *
   *  Possibly retries and reconnects if there are problems with connection.
   *  See set_retry_count(uint32_t) method for details.
   */
  template <typename Req, typename Resp> BoostError Send(const Req& req, Resp* resp);
  template <typename Resp> BoostError Recv(Resp* resp);
  BoostError ReadHeader(::boost::beast::http::basic_parser<false>* parser);

  void Shutdown();

  bool IsConnected() const;

  void set_connect_timeout_ms(uint32_t ms) {
    connect_timeout_ms_ = ms;
  }

  uint32_t connect_timeout_ms() const {
    return connect_timeout_ms_;
  }

  const std::string& host() const {
    return host_;
  }

  void AssignOnConnect(std::function<void(int)> cb) {
    on_connect_cb_ = std::move(cb);
  }

  void set_retry_count(uint32_t cnt) { retry_cnt_ = cnt; }

  auto native_handle() const {
    return socket_->native_handle();
  }

  ProactorBase* proactor() const {
    return proactor_;
  }

 protected:
  std::unique_ptr<FiberSocketBase> socket_;

 private:
  static bool IsIoError(BoostError ec) {
    using err = ::boost::beast::http::error;
    return ec && ec != err::need_buffer;
  }

  BoostError HandleError(BoostError ec) {
    if (IsIoError(ec)) {
      socket_->Close();
    }
    return ec;
  }

  fb2::ProactorBase* proactor_;
  uint32_t connect_timeout_ms_ = 2000;
  uint32_t retry_cnt_ = 1;
  ::boost::beast::flat_buffer tmp_buffer_;

  std::string host_;
  uint16_t port_ = 0;
  std::function<void(int)> on_connect_cb_;
};

template <typename Req, typename Resp> auto Client::Send(const Req& req, Resp* resp) -> BoostError {
  namespace h2 = ::boost::beast::http;
  BoostError ec, read_ec;
  AsioStreamAdapter<> adapter(*socket_);

  for (uint32_t i = 0; i < retry_cnt_; ++i) {
    ec = Send(req);
    if (IsIoError(ec))  // Send already retries.
      break;

    h2::read(adapter, tmp_buffer_, *resp, read_ec);

    if (!IsIoError(read_ec)) {
      return read_ec;  // surface up the http error.
    }

    *resp = Resp{};
    ec = read_ec;
  }

  return HandleError(ec);
}

template <typename Req> auto Client::Send(const Req& req) -> BoostError {
  BoostError ec;
  AsioStreamAdapter<> adapter(*socket_);

  for (uint32_t i = 0; i < retry_cnt_; ++i) {
    ::boost::beast::http::write(adapter, req, ec);

    if (IsIoError(ec)) {
      break;
    }
  }

  return HandleError(ec);
}

template <typename Resp> auto Client::Recv(Resp* resp) -> BoostError {
  BoostError ec;
  AsioStreamAdapter<> adapter(*socket_);

  ::boost::beast::http::read(adapter, tmp_buffer_, *resp, ec);

  return HandleError(ec);
}

inline auto Client::ReadHeader(::boost::beast::http::basic_parser<false>* parser) -> BoostError {
  BoostError ec;
  AsioStreamAdapter<> adapter(*socket_);
  ::boost::beast::http::read_header(adapter, tmp_buffer_, *parser, ec);

  return HandleError(ec);
}

///////////////////////////////////////////////////////////////////////////////
// Add support for HTTPS on top of the client above.
class TlsClient : public Client {
 public:
  // note: This context is for client only! it should never be used
  // on the server side!!
  // Call this function before you starting any connection.
  static SSL_CTX* CreateSslContext();

  // This should be called when you're done with the clients you're
  // using. But it should be safe, since internally open SSL are using
  // ref count when freeing resources.
  static void FreeContext(SSL_CTX* ctx);

  explicit TlsClient(fb2::ProactorBase* proactor) : Client(proactor) {
  }

  /*! @brief Connect to remote server and preform TLS handshake.
   *
   *  @param host: the name of the host to connect to.
   *  @param service: the port number (this must be convertible to short).
   *  @param context a valid SSL context that was created with the function CreateSslContext
   */
  std::error_code Connect(std::string_view host, std::string_view service, SSL_CTX* context);
};

}  // namespace http
}  // namespace util

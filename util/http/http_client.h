// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>
#include <string_view>

#include "util/asio_stream_adapter.h"

namespace util {

class ProactorBase;
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
  using StringPiece = ::std::string_view;

  explicit Client(ProactorBase* proactor);
  ~Client();

  std::error_code Connect(StringPiece host, StringPiece service);

  std::error_code Send(Verb verb, StringPiece url, StringPiece body, Response* response);
  std::error_code Send(Verb verb, StringPiece url, Response* response) {
    return Send(verb, url, StringPiece{}, response);
  }

  /*! @brief Sends http request but does not read response back.
   *
   *  Possibly retries and reconnects if there are problems with connection.
   *  See set_retry_count(uint32_t) method.
   */
  template <typename Req> std::error_code Send(const Req& req);

  /*! @brief Sends http request and reads response back.
   *
   *  Possibly retries and reconnects if there are problems with connection.
   *  See set_retry_count(uint32_t) method for details.
   */
  template <typename Req, typename Resp> std::error_code Send(const Req& req, Resp* resp);

  void Shutdown();

  bool IsConnected() const;

  void set_connect_timeout_ms(uint32_t ms) {
    connect_timeout_ms_ = ms;
  }

  // Adds header to all future requests.
  void AddHeader(std::string name, std::string value) {
    headers_.emplace_back(std::move(name), std::move(value));
  }

  const std::string host() const { return host_; }

 private:
  static bool IsIoError(std::error_code ec) {
    return bool(ec);  // TODO: currently all erros are io errors
  }

  static std::error_code HandleError(std::error_code ec) {
    return ec;  // TODO: a hook to print warning errors, change state etc.
  }

  ProactorBase* proactor_;
  uint32_t connect_timeout_ms_ = 2000;
  uint32_t retry_cnt_ = 1;
  ::boost::beast::flat_buffer tmp_buffer_;

  using HeaderPair = std::pair<std::string, std::string>;

  std::vector<HeaderPair> headers_;
  std::unique_ptr<FiberSocketBase> socket_;
  std::string host_;
};

template <typename Req, typename Resp> std::error_code Client::Send(const Req& req, Resp* resp) {
  namespace h2 = ::boost::beast::http;
  std::error_code ec;
  ::boost::system::error_code read_ec;
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
  }

  return HandleError(ec);
}

template <typename Req> std::error_code Client::Send(const Req& req) {
  ::boost::system::error_code ec;
  AsioStreamAdapter<> adapter(*socket_);

  for (uint32_t i = 0; i < retry_cnt_; ++i) {
    ::boost::beast::http::write(adapter, req, ec);

    if (IsIoError(ec)) {
      break;
    }
  }

  return HandleError(ec);
}

}  // namespace http
}  // namespace util

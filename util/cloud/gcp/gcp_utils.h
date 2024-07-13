// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <boost/beast/http/empty_body.hpp>
#include <memory>

#include "io/io.h"
#include "util/http/http_client.h"

namespace util::cloud {
class GCPCredsProvider;
extern const char GCS_API_DOMAIN[];

namespace detail {
inline std::string_view FromBoostSV(boost::string_view sv) {
  return std::string_view(sv.data(), sv.size());
}

class HttpRequestBase {
 public:
  HttpRequestBase(const HttpRequestBase&) = delete;
  HttpRequestBase& operator=(const HttpRequestBase&) = delete;
  HttpRequestBase() = default;

  virtual ~HttpRequestBase() = default;
  virtual std::error_code Send(http::Client* client) = 0;

  virtual const boost::beast::http::header<true>& GetHeaders() const = 0;

  virtual void SetHeader(boost::beast::http::field f, std::string_view value) = 0;
};

class EmptyRequestImpl : public HttpRequestBase {
  using EmptyRequest = boost::beast::http::request<boost::beast::http::empty_body>;
  EmptyRequest req_;

 public:
  EmptyRequestImpl(boost::beast::http::verb req_verb, std::string_view url,
                   const std::string_view access_token);

  void SetUrl(std::string_view url) {
    req_.target(boost::string_view{url.data(), url.size()});
  }

  void Finalize() {
    req_.prepare_payload();
  }

  void SetHeader(boost::beast::http::field f, std::string_view value) final {
    req_.set(f, boost::string_view{value.data(), value.size()});
  }

  const boost::beast::http::header<true>& GetHeaders() const final {
    return req_.base();
  }

  std::error_code Send(http::Client* client) final;
};

class DynamicBodyRequestImpl : public HttpRequestBase {
  using DynamicBodyRequest = boost::beast::http::request<boost::beast::http::dynamic_body>;
  DynamicBodyRequest req_;

 public:
  DynamicBodyRequestImpl(DynamicBodyRequestImpl&&) = default;

  explicit DynamicBodyRequestImpl(std::string_view url)
      : req_(boost::beast::http::verb::post, boost::string_view{url.data(), url.size()}, 11) {
  }

  template <typename BodyArgs> void SetBody(BodyArgs&& body_args) {
    req_.body() = std::forward<BodyArgs>(body_args);
  }

  void SetHeader(boost::beast::http::field f, std::string_view value) final {
    req_.set(f, boost::string_view{value.data(), value.size()});
  }

  void Finalize() {
    req_.prepare_payload();
  }

  const boost::beast::http::header<true>& GetHeaders() const final {
    return req_.base();
  }

  std::error_code Send(http::Client* client) final;
};

}  // namespace detail

class RobustSender {
  RobustSender(const RobustSender&) = delete;
  RobustSender& operator=(const RobustSender&) = delete;

 public:
  using HeaderParserPtr =
      std::unique_ptr<boost::beast::http::response_parser<boost::beast::http::empty_body>>;

  RobustSender(unsigned num_iterations, GCPCredsProvider* provider);

  io::Result<HeaderParserPtr> Send(http::Client* client, detail::HttpRequestBase* req);

 private:
  unsigned num_iterations_;
  GCPCredsProvider* provider_;
};

std::string AuthHeader(std::string_view access_token);

#define RETURN_UNEXPECTED(x)                               \
  do {                                                     \
    auto ec = (x);                                         \
    if (ec) {                                              \
      VLOG(1) << "Failed " << #x << ": " << ec.message();  \
      return nonstd::make_unexpected(ec);                  \
    }                                                      \
  } while (false)

}  // namespace util::cloud
// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>

#include "util/http/http_client.h"
#include "util/http/https_client_pool.h"
#include "io/file.h"

#define RETURN_ERROR(x)                                          \
  do {                                                           \
    auto ec = (x);                                               \
    if (ec) {                                                    \
      VLOG(1) << "Error calling " << #x << ": " << ec.message(); \
      return ec;                                                 \
    }                                                            \
  } while (false)

#define RETURN_UNEXPECTED(x)                              \
  do {                                                    \
    auto ec = (x);                                        \
    if (ec) {                                             \
      VLOG(1) << "Failed " << #x << ": " << ec.message(); \
      return nonstd::make_unexpected(ec);                 \
    }                                                     \
  } while (false)

namespace util::cloud {

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

  const boost::beast::http::header<true>& GetHeaders() const {
    return const_cast<HttpRequestBase*>(this)->GetHeadersInternal();
  }

  void SetHeader(boost::beast::http::field f, std::string_view value) {
    GetHeadersInternal().set(f, boost::string_view{value.data(), value.size()});
  }

  void SetHeader(std::string_view f, std::string_view value) {
    GetHeadersInternal().set(boost::string_view{f.data(), f.size()},
                             boost::string_view{value.data(), value.size()});
  }

  virtual boost::beast::http::verb GetMethod() const = 0;

 protected:
  virtual boost::beast::http::header<true>& GetHeadersInternal() = 0;
};

class EmptyRequestImpl : public HttpRequestBase {
  using EmptyRequest = boost::beast::http::request<boost::beast::http::empty_body>;
  EmptyRequest req_;

 public:
  EmptyRequestImpl(boost::beast::http::verb req_verb, std::string_view url);
  EmptyRequestImpl(EmptyRequestImpl&& other) : req_(std::move(other.req_)) {
  }

  void SetUrl(std::string_view url) {
    req_.target(boost::string_view{url.data(), url.size()});
  }

  void Finalize() {
    req_.prepare_payload();
  }

  std::error_code Send(http::Client* client) final;

  boost::beast::http::verb GetMethod() const final {
    return req_.method();
  }

 protected:
  boost::beast::http::header<true>& GetHeadersInternal() final {
    return req_.base();
  }
};

class DynamicBodyRequestImpl : public HttpRequestBase {
  using DynamicBodyRequest = boost::beast::http::request<boost::beast::http::dynamic_body>;
  DynamicBodyRequest req_;

 public:
  DynamicBodyRequestImpl(DynamicBodyRequestImpl&& other) : req_(std::move(other.req_)) {
  }

  explicit DynamicBodyRequestImpl(std::string_view url, boost::beast::http::verb verb)
      : req_(verb, boost::string_view{url.data(), url.size()}, 11) {
  }

  template <typename BodyArgs> void SetBody(BodyArgs&& body_args) {
    req_.body() = std::forward<BodyArgs>(body_args);
  }

  void Finalize() {
    req_.prepare_payload();
  }

  std::error_code Send(http::Client* client) final;

  boost::beast::http::verb GetMethod() const final {
    return req_.method();
  }

 protected:
  boost::beast::http::header<true>& GetHeadersInternal() final {
    return req_.base();
  }
};

std::error_code EnableKeepAlive(int fd);

// File handle that writes to cloud storage.
//
// This uses multipart uploads, where it will buffer upto the configured part
// size before uploading.
class AbstractStorageFile : public io::WriteFile {
 public:
  AbstractStorageFile(std::string_view create_file_name, size_t part_size)
      : WriteFile(create_file_name), body_mb_(part_size) {
  }

  // Writes bytes to the cloud object. This will either buffer internally or
  // write a part to the cloud.
  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override;

 private:
  std::error_code FillBuf(const uint8_t* buffer, size_t length);

 protected:
  virtual std::error_code Upload() = 0;

  boost::beast::multi_buffer body_mb_;
};

}  // namespace detail

class CredentialsProvider {
 public:
  virtual ~CredentialsProvider() = default;

  virtual std::error_code Init(unsigned connect_ms) = 0;

  virtual std::string ServiceEndpoint() const = 0;

  virtual void Sign(detail::HttpRequestBase* req) const = 0;
  virtual std::error_code RefreshToken() = 0;
};

struct StorageListItem {
  size_t size = 0;
  std::string_view key;
  int64_t mtime_ns = 0;
  bool is_prefix = false;
};

class RobustSender {
  RobustSender(const RobustSender&) = delete;
  RobustSender& operator=(const RobustSender&) = delete;

 public:
  struct SenderResult {
    std::unique_ptr<boost::beast::http::response_parser<boost::beast::http::empty_body>> eb_parser;
    http::ClientPool::ClientHandle client_handle;
  };

  RobustSender(http::ClientPool* pool, CredentialsProvider* provider);

  std::error_code Send(unsigned num_iterations, detail::HttpRequestBase* req, SenderResult* result);

 private:
  http::ClientPool* pool_;
  CredentialsProvider* provider_;
};

}  // namespace util::cloud
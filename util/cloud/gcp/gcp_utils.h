// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/parser.hpp>
#include <memory>

#include "io/io.h"
#include "util/cloud/utils.h"
#include "util/http/https_client_pool.h"

namespace util::cloud {
class GCPCredsProvider;
extern const char GCS_API_DOMAIN[];

namespace detail {
  EmptyRequestImpl CreateGCPEmptyRequest(boost::beast::http::verb req_verb, std::string_view url,
                                         const std::string_view access_token);

} // namespace detail

class RobustSender {
  RobustSender(const RobustSender&) = delete;
  RobustSender& operator=(const RobustSender&) = delete;

 public:
  struct SenderResult {
    std::unique_ptr<boost::beast::http::response_parser<boost::beast::http::empty_body>> eb_parser;
    http::ClientPool::ClientHandle client_handle;
  };

  RobustSender(http::ClientPool* pool, GCPCredsProvider* provider);

  std::error_code Send(unsigned num_iterations, detail::HttpRequestBase* req, SenderResult* result);

 private:
  http::ClientPool* pool_;
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
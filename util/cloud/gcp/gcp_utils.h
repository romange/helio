// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <boost/beast/http/empty_body.hpp>
#include <memory>

#include "io/io.h"
#include "util/http/http_client.h"

namespace util::cloud {
class GCPCredsProvider;

extern const char GCP_API_DOMAIN[];

using EmptyRequest = boost::beast::http::request<boost::beast::http::empty_body>;

EmptyRequest PrepareRequest(boost::beast::http::verb req_verb, std::string_view url,
                            const std::string_view access_token);

std::string AuthHeader(std::string_view access_token);

class RobustSender {
 public:
  using HeaderParserPtr =
      std::unique_ptr<boost::beast::http::response_parser<boost::beast::http::empty_body>>;

  RobustSender(unsigned num_iterations, GCPCredsProvider* provider);

  io::Result<HeaderParserPtr> Send(http::Client* client, EmptyRequest* req);

 private:
  unsigned num_iterations_;
  GCPCredsProvider* provider_;
};

}  // namespace util::cloud
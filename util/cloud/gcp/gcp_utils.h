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
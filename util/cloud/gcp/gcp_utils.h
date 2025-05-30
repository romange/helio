// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>
#include <memory>

#include "util/cloud/utils.h"
#include "util/http/https_client_pool.h"

namespace util::cloud {

namespace detail {
EmptyRequestImpl CreateGCPEmptyRequest(boost::beast::http::verb req_verb, std::string_view endpoint,
                                       std::string_view url, const std::string_view access_token);

std::string AuthHeader(std::string_view access_token);

}  // namespace detail

}  // namespace util::cloud
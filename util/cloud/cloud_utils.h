// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "util/http/http_client.h"

namespace util {
namespace cloud {

class AWS;

using HttpParser = ::boost::beast::http::response_parser<::boost::beast::http::buffer_body>;

std::error_code SendRequest(
    AWS* aws, http::Client* client,
    ::boost::beast::http::request<::boost::beast::http::empty_body>* req,
    ::boost::beast::http::response<::boost::beast::http::string_body>* resp);

std::error_code SendRequest(AWS* aws, http::Client* client,
                            ::boost::beast::http::request<::boost::beast::http::empty_body>* req,
                            HttpParser* resp);

}  // namespace cloud
}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/beast/http/file_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "util/http/http_common.h"

namespace util {
namespace http {

typedef ::boost::beast::http::response<::boost::beast::http::string_body> StringResponse;

inline StringResponse MakeStringResponse(
    ::boost::beast::http::status st = ::boost::beast::http::status::ok) {
  return StringResponse(st, 11);
}

StringResponse BuildStatusPage(const QueryArgs& args, std::string_view resource_prefix);
StringResponse ProfilezHandler(const QueryArgs& args);

extern const char kProfilesFolder[];

}  // namespace http
}  // namespace util

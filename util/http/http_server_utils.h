// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/beast/http/file_body.hpp>
#include <boost/beast/http/string_body.hpp>

namespace util {
namespace http {

// URL consists of path and query delimited by '?'.
// query can be broken into query args delimited by '&'.
// Each query arg can be a pair of "key=value" values.
// In case there is not '=' delimiter, only the first field is filled.
using QueryParam = std::pair<std::string_view, std::string_view>;
typedef std::vector<QueryParam> QueryArgs;

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

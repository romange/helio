// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/beast/http/field.hpp>
#include <string_view>
#include <vector>

namespace util {
namespace http {

template <typename Fields> inline void SetMime(const char* mime, Fields* dest) {
  dest->set(::boost::beast::http::field::content_type, mime);
}

extern const char kHtmlMime[];
extern const char kJsonMime[];
extern const char kSvgMime[];
extern const char kTextMime[];
extern const char kXmlMime[];
extern const char kBinMime[];

// URL consists of path and query delimited by '?'.
// query can be broken into query args delimited by '&'.
// Each query arg can be a pair of "key=value" values.
// In case there is not '=' delimiter, only the first field is filled.
using QueryParam = std::pair<std::string_view, std::string_view>;
typedef std::vector<QueryParam> QueryArgs;

// Returns a pair of path and query.
QueryParam ParseQuery(std::string_view str);

// Splits query into key-value pairs.
QueryArgs SplitQuery(std::string_view query);

}  // namespace http
}  // namespace util

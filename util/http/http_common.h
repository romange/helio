// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>
#include <boost/beast/http/field.hpp>

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

}  // namespace http
}  // namespace util

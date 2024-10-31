// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/http_common.h"

#include <absl/strings/str_split.h>

#include "base/logging.h"

namespace util {
namespace http {
using namespace std;

const char kHtmlMime[] = "text/html";
const char kJsonMime[] = "application/json";
const char kSvgMime[] = "image/svg+xml";
const char kTextMime[] = "text/plain";
const char kXmlMime[] = "application/xml";
const char kBinMime[] = "application/octet-stream";


QueryParam ParseQuery(std::string_view str) {
  std::pair<std::string_view, std::string_view> res;
  size_t pos = str.find('?');
  res.first = str.substr(0, pos);
  if (pos != std::string_view::npos) {
    res.second = str.substr(pos + 1);
  }
  return res;
}

QueryArgs SplitQuery(std::string_view query) {
  vector<std::string_view> args = absl::StrSplit(query, '&');
  vector<std::pair<std::string_view, std::string_view>> res(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    size_t pos = args[i].find('=');
    res[i].first = args[i].substr(0, pos);
    res[i].second = (pos == std::string_view::npos) ? std::string_view() : args[i].substr(pos + 1);
  }
  return res;
}

}  // namespace http
}  // namespace util

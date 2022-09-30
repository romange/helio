// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

namespace util::http {

// Replace all invalid characters with percent encoding.
std::string UrlEncode(std::string_view part);

}  // namespace util::http

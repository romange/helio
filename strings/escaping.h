// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>

namespace strings {

void AppendUrlEncoded(const std::string_view src, std::string* dest);
bool AppendUrlDecoded(const std::string_view src, std::string* dest);

}  // namespace strings
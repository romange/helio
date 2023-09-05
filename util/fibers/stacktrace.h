// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>

namespace util {

std::string GetStacktrace(bool symbolize = false);

}

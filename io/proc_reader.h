// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "io/io.h"

namespace io {

// Sizes in bytes as opposed to status files where sizes are in kb.
struct StatusData {
  size_t vm_peak = 0;
  size_t vm_rss = 0;
  size_t vm_size = 0;
};

Result<StatusData> ReadStatusInfo();

}  // namespace io

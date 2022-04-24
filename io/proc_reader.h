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

// Sizes in bytes.
struct MemInfoData {
  size_t mem_total = 0;
  size_t mem_free = 0;
  size_t mem_avail = 0;
  size_t mem_buffers = 0;
  size_t mem_cached = 0;
  size_t mem_SReclaimable = 0;

  // in meminfo.c of free program - mem used
  // equals to: total - (free + buffers + cached + SReclaimable).
};

Result<StatusData> ReadStatusInfo();
Result<MemInfoData> ReadMemInfo();

}  // namespace io

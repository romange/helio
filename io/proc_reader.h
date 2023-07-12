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
  size_t hugetlb_pages = 0;
};

// Sizes in bytes.
struct MemInfoData {
  size_t mem_total = 0;
  size_t mem_free = 0;
  size_t mem_avail = 0;
  size_t mem_buffers = 0;
  size_t mem_cached = 0;
  size_t mem_SReclaimable = 0;
  size_t swap_cached = 0;
  size_t swap_total = 0;
  size_t swap_free = 0;

  // in meminfo.c of free program - mem used
  // equals to: total - (free + buffers + cached + SReclaimable).
};

struct SelfStat {
  uint64_t start_time_sec = 0;
};

Result<StatusData> ReadStatusInfo();
Result<MemInfoData> ReadMemInfo();
Result<SelfStat> ReadSelfStat();

}  // namespace io

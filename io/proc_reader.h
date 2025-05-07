// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>
#include <sys/types.h>
#include "io/io.h"

namespace io {

// Sizes in bytes as opposed to status files where sizes are in kb.
struct StatusData {
  size_t vm_peak = 0;
  size_t vm_rss = 0;
  size_t vm_size = 0;
  size_t vm_swap = 0;
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
  uint64_t maj_flt = 0;
};

Result<StatusData> ReadStatusInfo();
Result<MemInfoData> ReadMemInfo();
Result<SelfStat> ReadSelfStat();

// key,value list from /etc/os-release
using DistributionInfo = std::vector<std::pair<std::string, std::string>>;

Result<DistributionInfo> ReadDistributionInfo();

struct TcpInfo {
  bool is_ipv6 = false;
  unsigned state = 0;
  unsigned local_port = 0;
  unsigned remote_port = 0;
  unsigned inode = 0;
  uint32_t local_addr = 0;
  uint32_t remote_addr = 0;
  unsigned char local_addr6[16] = {0};
  unsigned char remote_addr6[16] = {0};
};

std::string TcpStateToString(unsigned state);
Result<TcpInfo> ReadTcpInfo(ino_t sock_inode);
Result<TcpInfo> ReadTcp6Info(ino_t sock_inode);

}  // namespace io

// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <sys/types.h>

#include <string>
#include <string_view>
#include <system_error>
#include <vector>

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

  // The  number  of major faults the process has made which have required
  // loading a memory page from disk.
  uint64_t maj_flt = 0;
};

Result<StatusData> ReadStatusInfo();
Result<MemInfoData> ReadMemInfo();
Result<SelfStat> ReadSelfStat();

// key,value list from /etc/os-release
using DistributionInfo = std::vector<std::pair<std::string, std::string>>;

Result<DistributionInfo> ReadDistributionInfo();

enum class HypervisorStatus { kUnknown, kAbsent, kPresent };

struct CpuInfo {
  // The first nonempty x86-style flags record. kAbsent does not prove bare metal.
  HypervisorStatus hypervisor = HypervisorStatus::kUnknown;
};

ABSL_MUST_USE_RESULT Result<CpuInfo> ReadCpuInfo(std::string_view path = "/proc/cpuinfo");

struct DmiInfo {
  Result<std::string> sys_vendor;
  Result<std::string> product_name;
  Result<std::string> chassis_asset_tag;
  Result<std::string> bios_version;
};

// Values are whitespace-trimmed. Each attribute retains its own read error.
ABSL_MUST_USE_RESULT DmiInfo ReadDmiInfo(std::string_view directory = "/sys/class/dmi/id");

struct TcpInfo {
  bool is_ipv6 = false;
  // TCP state.
  // https://en.wikipedia.org/wiki/Transmission_Control_Protocol#Protocol_operation
  // https://www.ibm.com/support/pages/network-connection-state-meanings-and-transitions
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

// sock_inode can be fetched by fstat call on socket file descriptor
Result<TcpInfo> ReadTcpInfo(ino_t sock_inode);
Result<TcpInfo> ReadTcp6Info(ino_t sock_inode);

}  // namespace io

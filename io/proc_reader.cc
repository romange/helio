// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "io/proc_reader.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <absl/base/internal/endian.h>
#include <fcntl.h>
#include <unistd.h>

#include "base/io_buf.h"
#include "base/logging.h"

namespace io {

using namespace std;
using absl::SimpleAtoi;
using nonstd::make_unexpected;

namespace {

// Reads proc files like /proc/self/status  or /proc/meminfo
// passes to cb: (key, value)
error_code ReadProcFile(const char* name, char c, function<void(string_view, string_view)> cb, bool skip_header = false) {
  int fd = open(name, O_RDONLY | O_CLOEXEC);
  if (fd == -1)
    return error_code{errno, system_category()};

  base::IoBuf buf{512};
  bool header_skipped = !skip_header;

  while (true) {
    auto dest = buf.AppendBuffer();

    int res = read(fd, dest.data(), dest.size());
    if (res == -1) {
      close(fd);
      return error_code{errno, system_category()};
    }

    if (res == 0) {
      break;
    }

    buf.CommitWrite(res);

    while (true) {
      auto input = buf.InputBuffer();
      uint8_t* eol = reinterpret_cast<uint8_t*>(memchr(input.data(), '\n', input.size()));
      if (!eol) {
        if (buf.AppendLen() == 0) {
          buf.EnsureCapacity(buf.Capacity() * 2);
        }

        break;
      }

      string_view line(reinterpret_cast<char*>(input.data()), eol - input.data());
      
      if (!header_skipped) {
        header_skipped = true;
        buf.ConsumeInput(line.size() + 1);
        continue;
      }
      
      size_t pos = line.find(c);
      if (pos == string_view::npos)
        break;
      string_view key = line.substr(0, pos);
      string_view value = absl::StripLeadingAsciiWhitespace(line.substr(pos + 1));

      cb(key, value);

      buf.ConsumeInput(line.size() + 1);
    }
  }
  close(fd);

  return error_code{};
}

inline void ParseKb(string_view num, size_t* dest) {
  CHECK(SimpleAtoi(num, dest));
  *dest *= 1024;
};

size_t find_nth(string_view str, char c, uint32_t index) {
  for (size_t i = 0; i < str.size(); ++i) {
    if (str[i] == c) {
      if (index-- == 0)
        return i;
    }
  }
  return string_view::npos;
}

// Convert hex string to IPv6 address bytes
void HexToIPv6(string_view hex_str, unsigned char* out) {
  for (size_t i = 0; i < 16 && (i * 2 + 1) < hex_str.size(); i++) {
    string_view byte_hex = hex_str.substr(i * 2, 2);
    unsigned int byte;
    if (absl::SimpleHexAtoi(byte_hex, &byte)) {
      out[i] = static_cast<unsigned char>(byte);
    }
  }
}

// Parse a substring after sl from /proc/net/tcp or /proc/net/tcp6 file
// sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode
// https://www.kernel.org/doc/Documentation/networking/proc_net_tcp.txt
bool ParseSocketLine(string_view line, ino_t target_inode, bool is_ipv6, TcpInfo* info) {
  std::vector<string_view> parts = absl::StrSplit(line, absl::ByAnyChar(" \t"), absl::SkipEmpty());

  if (parts.size() < 9)
    return false;

  string_view local_addr_port = parts[0];
  size_t colon_pos = local_addr_port.find(':');
  if (colon_pos == string_view::npos)
    return false;

  string_view local_addr_hex = local_addr_port.substr(0, colon_pos);
  string_view local_port_hex = local_addr_port.substr(colon_pos + 1);

  string_view remote_addr_port = parts[1];
  colon_pos = remote_addr_port.find(':');
  if (colon_pos == string_view::npos)
    return false;

  string_view remote_addr_hex = remote_addr_port.substr(0, colon_pos);
  string_view remote_port_hex = remote_addr_port.substr(colon_pos + 1);

  unsigned int state;
  if (!absl::SimpleHexAtoi(parts[2], &state))
    return false;

  unsigned int inode = 0;
  if (!absl::SimpleAtoi(parts[8], &inode))
    return false;

  if (inode != target_inode)
    return false;

  info->is_ipv6 = is_ipv6;
  info->state = state;
  info->inode = inode;

  unsigned int port;
  if (absl::SimpleHexAtoi(local_port_hex, &port)) {
    info->local_port = port;
  }
  if (absl::SimpleHexAtoi(remote_port_hex, &port)) {
    info->remote_port = port;
  }

  if (is_ipv6) {
    HexToIPv6(local_addr_hex, info->local_addr6);
    HexToIPv6(remote_addr_hex, info->remote_addr6);
  } else {
    unsigned int addr;
    if (absl::SimpleHexAtoi(local_addr_hex, &addr)) {
      info->local_addr = absl::big_endian::Load32(reinterpret_cast<const char*>(&addr));
    }
    if (absl::SimpleHexAtoi(remote_addr_hex, &addr)) {
      info->remote_addr = absl::big_endian::Load32(reinterpret_cast<const char*>(&addr));
    }
  }

  return true;
}

// Reads TCP info from a specified proc file
Result<TcpInfo> ReadTcpInfoFromFile(const char* proc_path, ino_t sock_inode, bool is_ipv6) {
  TcpInfo info;
  bool found = false;

  auto cb = [&](string_view key, string_view value) mutable {
    if (found) {
      return;
    }

    if (ParseSocketLine(value, sock_inode, is_ipv6, &info)) {
      found = true;
    }
  };

  error_code ec = ReadProcFile(proc_path, ':', std::move(cb), true);
  if (ec) {
    return make_unexpected(ec);
  }

  if (!found) {
    return make_unexpected(error_code{ENOENT, system_category()});
  }

  return info;
}

}  // namespace

Result<StatusData> ReadStatusInfo() {
  StatusData sdata;

  auto cb = [&sdata](string_view key, string_view value) {
    size_t space = value.find(' ');
    string_view num = value.substr(0, space);

    if (key == "VmPeak") {
      ParseKb(num, &sdata.vm_peak);
    } else if (key == "VmSize") {
      ParseKb(num, &sdata.vm_size);
    } else if (key == "VmRSS") {
      ParseKb(num, &sdata.vm_rss);
    } else if (key == "VmSwap") {
      ParseKb(num, &sdata.vm_swap);
    } else if (key == "HugetlbPages") {
      ParseKb(num, &sdata.hugetlb_pages);
    }
  };

  error_code ec = ReadProcFile("/proc/self/status", ':', std::move(cb));
  if (ec)
    return make_unexpected(ec);

  return sdata;
}

Result<MemInfoData> ReadMemInfo() {
  MemInfoData mdata;

  auto cb = [&mdata](string_view key, string_view value) {
    size_t space = value.find(' ');
    string_view num = value.substr(0, space);

    if (key == "MemTotal") {
      ParseKb(num, &mdata.mem_total);
    } else if (key == "MemFree") {
      ParseKb(num, &mdata.mem_free);
    } else if (key == "MemAvailable") {
      ParseKb(num, &mdata.mem_avail);
    } else if (key == "Buffers") {
      ParseKb(num, &mdata.mem_buffers);
    } else if (key == "Cached") {
      ParseKb(num, &mdata.mem_cached);
    } else if (key == "SReclaimable") {
      ParseKb(num, &mdata.mem_SReclaimable);
    } else if (key == "SwapCached") {
      ParseKb(num, &mdata.swap_cached);
    } else if (key == "SwapTotal") {
      ParseKb(num, &mdata.swap_total);
    } else if (key == "SwapFree") {
      ParseKb(num, &mdata.swap_free);
    }
  };

  error_code ec = ReadProcFile("/proc/meminfo", ':', std::move(cb));
  if (ec)
    return make_unexpected(ec);

  return mdata;
}

Result<SelfStat> ReadSelfStat() {
  static atomic_uint64_t boot_time(0);

  uint64_t btime = boot_time.load(std::memory_order_relaxed);
  if (btime == 0) {
    auto cb = [&](string_view key, string_view value) {
      if (key == "btime") {
        CHECK(absl::SimpleAtoi(value, &btime));
        boot_time.store(btime, std::memory_order_release);
      }
    };

    error_code ec = ReadProcFile("/proc/stat", ' ', std::move(cb));
    if (ec)
      return make_unexpected(ec);
  }

  int fd = open("/proc/self/stat", O_RDONLY | O_CLOEXEC);
  if (fd < 0)
    return make_unexpected(error_code{errno, system_category()});

  long jiffies_per_second = sysconf(_SC_CLK_TCK);

  uint64_t start_since_boot = 0;
  constexpr size_t kBufSize = 2048;
  std::unique_ptr<char[]> buf(new char[kBufSize]);

  ssize_t bytes_read = read(fd, buf.get(), kBufSize);
  close(fd);
  if (bytes_read <= 0)
    return make_unexpected(error_code{errno, system_category()});

  SelfStat res;
  string_view str(buf.get(), bytes_read);
  size_t pos = find_nth(str, ' ', 10);
  str.remove_prefix(pos);
  if (str.empty())
    return make_unexpected(error_code{EILSEQ, system_category()});

  size_t next = str.find(' ', 1);
  uint64_t maj_flt = 0;
  if (next == string_view::npos || !absl::SimpleAtoi(str.substr(1, next - 1), &maj_flt))
    return make_unexpected(error_code{EILSEQ, system_category()});
  str.remove_prefix(next + 1);

  pos = find_nth(str, ' ', 8);
  str.remove_prefix(pos);
  if (str.empty())
    return make_unexpected(error_code{EILSEQ, system_category()});

  next = str.find(' ', 1);
  CHECK(absl::SimpleAtoi(str.substr(1, next - 1), &start_since_boot));
  start_since_boot /= jiffies_per_second;

  res.start_time_sec = btime + start_since_boot;
  res.maj_flt = maj_flt;
  return res;
}

Result<DistributionInfo> ReadDistributionInfo() {
  DistributionInfo result;
  auto cb = [&result](string_view key, string_view value) {
    result.emplace_back(key, value);
  };

  error_code ec = ReadProcFile("/etc/os-release", '=', std::move(cb));
  if (ec)
    return make_unexpected(ec);
  return result;
}

// Converts numeric TCP state to human-readable string
std::string TcpStateToString(unsigned state) {
  switch (state) {
    case 0x01: return "ESTABLISHED";
    case 0x02: return "SYN_SENT";
    case 0x03: return "SYN_RECV";
    case 0x04: return "FIN_WAIT1";
    case 0x05: return "FIN_WAIT2";
    case 0x06: return "TIME_WAIT";
    case 0x07: return "CLOSE";
    case 0x08: return "CLOSE_WAIT";
    case 0x09: return "LAST_ACK";
    case 0x0A: return "LISTEN";
    case 0x0B: return "CLOSING";
    default: return "UNKNOWN";
  }
}

Result<TcpInfo> ReadTcpInfo(ino_t sock_inode) {
  return ReadTcpInfoFromFile("/proc/net/tcp", sock_inode, false);
}

Result<TcpInfo> ReadTcp6Info(ino_t sock_inode) {
  return ReadTcpInfoFromFile("/proc/net/tcp6", sock_inode, true);
}

}  // namespace io

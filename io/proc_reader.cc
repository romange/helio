// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "io/proc_reader.h"

#include <absl/strings/numbers.h>
#include <absl/strings/strip.h>
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
error_code ReadProcFile(const char* name, char c, function<void(string_view, string_view)> cb) {
  int fd = open(name, O_RDONLY | O_CLOEXEC);
  if (fd == -1)
    return error_code{errno, system_category()};

  base::IoBuf buf{512};

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

}  // namespace io

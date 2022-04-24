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
using nonstd::make_unexpected;
using absl::SimpleAtoi;

namespace {

// Reads proc files like /proc/self/status  or /proc/meminfo
// passes to cb: (key, value)
error_code ReadProcFile(const char* name, function<void(string_view, string_view)> cb) {
  int fd = open(name, O_RDONLY | O_CLOEXEC);
  if (fd == -1)
    return error_code{errno, system_category()};

  base::IoBuf buf;

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
      if (!eol)
        break;

      string_view line(reinterpret_cast<char*>(input.data()), eol - input.data());
      size_t pos = line.find(':');
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
    }
  };

  error_code ec = ReadProcFile("/proc/self/status", move(cb));
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
    }
  };

  error_code ec = ReadProcFile("/proc/meminfo", move(cb));
  if (ec)
    return make_unexpected(ec);

  return mdata;
}

}  // namespace io

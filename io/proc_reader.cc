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

Result<StatusData> ReadStatusInfo() {
  int fd = open("/proc/self/status", O_NOATIME);
  if (fd == -1)
    return make_unexpected(error_code(errno, system_category()));

  base::IoBuf buf;
  StatusData sdata;

  while (true) {
    auto dest = buf.AppendBuffer();

    int res = read(fd, dest.data(), dest.size());
    if (res == -1) {
      close(fd);
      return make_unexpected(error_code(errno, system_category()));
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

      std::string_view line(reinterpret_cast<char*>(input.data()), eol - input.data());
      if (absl::ConsumePrefix(&line, "VmPeak:")) {
        line = absl::StripLeadingAsciiWhitespace(line);
        size_t del = line.find(' ');
        CHECK(absl::SimpleAtoi(line.substr(0, del), &sdata.vm_peak)) << line;
        sdata.vm_peak *= 1024;
      } else if (absl::ConsumePrefix(&line, "VmSize:")) {
        line = absl::StripLeadingAsciiWhitespace(line);
        size_t del = line.find(' ');
        CHECK(absl::SimpleAtoi(line.substr(0, del), &sdata.vm_size));
        sdata.vm_size *= 1024;
      } else if (absl::ConsumePrefix(&line, "VmRSS:")) {
        line = absl::StripLeadingAsciiWhitespace(line);
        size_t del = line.find(' ');
        CHECK(absl::SimpleAtoi(line.substr(0, del), &sdata.vm_rss)) << line;
        sdata.vm_rss *= 1024;
      }
      buf.ConsumeInput(line.size() + 1);
    }
  }
  close(fd);

  return sdata;
}

}  // namespace io

// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/proc_util.h"

#include <spawn.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <mutex>  // once_flag

#include "absl/strings/escaping.h"
#include "absl/strings/numbers.h"
#include "absl/strings/strip.h"
#include "absl/time/time.h"
#include "base/integral_types.h"
#include "base/logging.h"

namespace base {

static size_t find_nth(std::string_view str, char c, uint32_t index) {
  for (size_t i = 0; i < str.size(); ++i) {
    if (str[i] == c) {
      if (index-- == 0)
        return i;
    }
  }
  return std::string_view::npos;
}

// Runs a child process efficiently.
// argv must be null terminated array of arguments where the first item in the array is the base
// name of the executable passed by path.
// Returns 0 if child process run successfully and updates child_status with its return status.
static int spawn(const char* path, char* argv[], int* child_status) {
  pid_t pid;
  // posix_spawn in this configuration calls vfork without duplicating parents
  // virtual memory for the child. That's all we want.
  int status = posix_spawn(&pid, path, NULL, NULL, argv, environ);
  if (status != 0)
    return status;

  if (waitpid(pid, child_status, 0) == -1)
    return errno;
  return 0;
}

ProcessStats ProcessStats::Read() {
  ProcessStats stats;
  FILE* f = fopen("/proc/self/status", "r");
  if (f == nullptr)
    return stats;
  char* line = nullptr;
  size_t len = 128;
  while (getline(&line, &len, f) != -1) {
    line[len - 1] = '\0';
    std::string_view line_view(line, len);
    if (absl::ConsumePrefix(&line_view, "VmPeak:")) {
      line_view = absl::StripLeadingAsciiWhitespace(line_view);
      size_t del = line_view.find(' ');
      CHECK(absl::SimpleAtoi(line_view.substr(0, del), &stats.vm_peak)) << (line + 8);
    } else if (absl::ConsumePrefix(&line_view, "VmSize:")) {
      line_view = absl::StripLeadingAsciiWhitespace(line_view);
      size_t del = line_view.find(' ');
      CHECK(absl::SimpleAtoi(line_view.substr(0, del), &stats.vm_size));
    } else if (absl::ConsumePrefix(&line_view, "VmRSS:")) {
      line_view = absl::StripLeadingAsciiWhitespace(line_view);
      size_t del = line_view.find(' ');
      CHECK(absl::SimpleAtoi(line_view.substr(0, del), &stats.vm_rss)) << line_view;
    }
  }
  fclose(f);
  f = fopen("/proc/self/stat", "r");
  if (f) {
    long jiffies_per_second = sysconf(_SC_CLK_TCK);
    uint64 start_since_boot = 0;
    char buf[512] = {0};
    size_t bytes_read = fread(buf, 1, sizeof buf, f);
    if (bytes_read == sizeof buf) {
      fprintf(stderr, "Buffer is too small %lu\n", sizeof buf);
    } else {
      std::string_view str(buf, bytes_read);
      size_t pos = find_nth(str, ' ', 20);
      if (pos != std::string_view::npos) {
        size_t next = str.find(' ', pos + 1);
        CHECK(absl::SimpleAtoi(str.substr(pos + 1, next - pos - 1), &start_since_boot));
        start_since_boot /= jiffies_per_second;
      }
    }
    fclose(f);
    if (start_since_boot > 0) {
      f = fopen("/proc/stat", "r");
      if (f) {
        while (getline(&line, &len, f) != -1) {
          std::string_view line_view(line, len);

          if (absl::ConsumePrefix(&line_view, "btime ")) {
            uint64_t boot_time;
            size_t next = line_view.find('\n');
            line_view = line_view.substr(0, next);
            CHECK(absl::SimpleAtoi(line_view, &boot_time)) << absl::CHexEscape(line_view);
            if (boot_time > 0)
              stats.start_time_seconds = boot_time + start_since_boot;
          }
        }
        fclose(f);
      }
    }
  }
  free(line);
  return stats;
}

namespace sys {

void GetKernelVersion(KernelVersion* version) {
  struct utsname buffer;

  CHECK_EQ(0, uname(&buffer));

  int res = sscanf(buffer.release, "%u.%u.%u-%u", &version->kernel, &version->major,
                   &version->minor, &version->patch);
  CHECK(res == 3 || res == 4) << res;
}

}  // namespace sys

int sh_exec(const char* cmd) {
  char sh_bin[] = "sh";
  char arg1[] = "-c";

  std::string cmd2(cmd);

  char* argv[] = {sh_bin, arg1, &cmd2.front(), NULL};
  int child_status = 0;
  return spawn("/bin/sh", argv, &child_status);
}

}  // namespace base

std::ostream& operator<<(std::ostream& os, const base::ProcessStats& stats) {
  os << "VmPeak: " << stats.vm_peak << "kb, VmSize: " << stats.vm_size
     << "kb, VmRSS: " << stats.vm_rss
     << "kb, Start Time: " << absl::FormatDuration(absl::Seconds(stats.start_time_seconds));
  return os;
}

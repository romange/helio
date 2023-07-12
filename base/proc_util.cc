// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/proc_util.h"

#include <fcntl.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "absl/strings/escaping.h"
#include "absl/strings/numbers.h"
#include "absl/strings/strip.h"
#include "absl/time/time.h"
#include "base/integral_types.h"
#include "base/logging.h"

// for MacOS we declare explicitly.
extern "C" char** environ;

namespace base {

using namespace std;

namespace {
size_t find_nth(string_view str, char c, uint32_t index) {
  for (size_t i = 0; i < str.size(); ++i) {
    if (str[i] == c) {
      if (index-- == 0)
        return i;
    }
  }
  return string_view::npos;
}

bool Matches(string_view str, string_view* line) {
  if (!absl::ConsumePrefix(line, str))
    return false;
  *line = absl::StripLeadingAsciiWhitespace(*line);
  return true;
};

inline string_view FirstToken(string_view line, char c) {
  size_t del = line.find(c);
  return line.substr(0, del);
}

}  // namespace

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
  int fd = open("/proc/self/status", O_RDONLY);
  if (fd == -1)
    return stats;

  constexpr size_t kBufSize = 2048;
  std::unique_ptr<char[]> buf(new char[kBufSize]);
  ssize_t bytes_read = read(fd, buf.get(), kBufSize);
  close(fd);

  if (bytes_read == -1) {
    LOG(WARNING) << "Could not read /proc/self/status, err " << errno;

    return stats;
  }

  string_view str(buf.get(), bytes_read);

#define SAFE_ATOI(str, var)                    \
  do {                                         \
    if (!absl::SimpleAtoi(str, &var)) {        \
      LOG(ERROR) << "Could not parse " << str; \
      return ProcessStats{};                   \
    }                                          \
  } while (0)

  while (!str.empty()) {
    size_t pos = str.find('\n');
    string_view line = str.substr(0, pos);

    if (Matches("VmPeak:", &line)) {
      string_view tok = FirstToken(line, ' ');
      SAFE_ATOI(tok, stats.vm_peak);
    } else if (Matches("VmSize:", &line)) {
      string_view tok = FirstToken(line, ' ');
      SAFE_ATOI(tok, stats.vm_size);
    } else if (Matches("VmRSS:", &line)) {
      string_view tok = FirstToken(line, ' ');
      SAFE_ATOI(tok, stats.vm_rss);
    } else if (Matches("HugetlbPages:", &line)) {
      string_view tok = FirstToken(line, ' ');
      SAFE_ATOI(tok, stats.hugetlb_pages);
    }

    if (pos == string_view::npos)
      break;
    str.remove_prefix(pos + 1);
  }

  fd = open("/proc/self/stat", O_RDONLY);
  if (fd > 0) {
    long jiffies_per_second = sysconf(_SC_CLK_TCK);
    uint64 start_since_boot = 0;
    ssize_t bytes_read = read(fd, buf.get(), kBufSize);
    close(fd);
    if (bytes_read > 0) {
      string_view str(buf.get(), bytes_read);
      size_t pos = find_nth(str, ' ', 20);
      str.remove_prefix(pos);
      if (!str.empty()) {
        size_t next = str.find(' ', 1);
        CHECK(absl::SimpleAtoi(str.substr(1, next - 1), &start_since_boot));
        start_since_boot /= jiffies_per_second;
      }
    }

    static atomic_uint64_t boot_time(0);
    uint64_t btime = boot_time.load(std::memory_order_relaxed);
    if (btime == 0) {
      int fd = open("/proc/stat", O_RDONLY);
      if (fd > 0) {
        ssize_t bytes_read = read(fd, buf.get(), kBufSize);
        close(fd);
        if (bytes_read > 0) {
          string_view str(buf.get(), bytes_read);

          while (!str.empty()) {
            size_t pos = str.find('\n');
            string_view line = str.substr(0, pos);

            if (Matches("btime", &line)) {
              string_view tok = FirstToken(line, '\n');
              absl::SimpleAtoi(tok, &btime);
              boot_time.store(btime, std::memory_order_release);
              break;
            }

            if (pos == string_view::npos)
              break;
            str.remove_prefix(pos + 1);
          }
        }
      }
    }

    stats.start_time_seconds = btime + start_since_boot;
  }  // fd > 0

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

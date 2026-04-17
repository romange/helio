// Copyright 2024, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/file_log_sink.h"

#ifdef USE_ABSL_LOG

#include <absl/flags/flag.h>
#include <absl/log/globals.h>
#include <absl/log/log_entry.h>
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "base/logging.h"  // for ProgramBaseName, MyUserName

ABSL_FLAG(std::string, log_dir, "",
          "If specified, log files are written into this directory instead "
          "of the default logging directory.");

using namespace std;

namespace {

const char* kSeverityNames[] = {"INFO", "WARNING", "ERROR"};
constexpr size_t kFlushBytes = 1000000;

// Cached flag values updated via OnUpdate — avoids calling absl::GetFlag on every Send().
atomic<int> g_logbuflevel{0};
atomic<int> g_logbufsecs{30};

constexpr const char* kTempDirEnvVars[] = {"TMPDIR", "TMP"};

optional<string> ProbeWritableDir(const char* dir) {
  if (!dir || dir[0] == '\0')
    return nullopt;

  string dir_str = dir;
  if (dir_str.back() != '/') {
    dir_str.push_back('/');
  }

  struct stat st;
  if (stat(dir, &st) != 0 || !S_ISDIR(st.st_mode))
    return nullopt;

  // Need write + search (execute) permission to create files within the directory.
  if (access(dir, W_OK | X_OK) != 0)
    return nullopt;

  string probe = absl::StrCat(dir_str, "helio-log-probe-XXXXXX");
  vector<char> probe_buf(probe.begin(), probe.end());
  probe_buf.push_back('\0');

  int fd = mkstemp(probe_buf.data());
  if (fd < 0)
    return nullopt;

  close(fd);
  unlink(probe_buf.data());

  return dir_str;
}

string FindLoggingDir() {
  const string configured = absl::GetFlag(FLAGS_log_dir);
  if (!configured.empty()) {
    if (auto candidate = ProbeWritableDir(configured.c_str())) {
      return *candidate;
    }

    fprintf(stderr, "Configured --log_dir is not a writable directory: %s\n", configured.c_str());
    return {};
  }

  for (const char* env_name : kTempDirEnvVars) {
    if (auto candidate = ProbeWritableDir(getenv(env_name)))
      return *candidate;
  }

  for (auto* dir : {"/tmp", "./"}) {
    if (auto candidate = ProbeWritableDir(dir))
      return *candidate;
  }

  return {};
}

}  // namespace

// Messages at severity > logbuflevel are flushed immediately.
// Set to -1 to flush every message (disable buffering entirely).
ABSL_FLAG(int32_t, logbuflevel, 0,
          "Buffer log messages logged at this level or below. "
          "(-1 means don't buffer; 0 means buffer INFO only).")
    .OnUpdate([] { g_logbuflevel.store(absl::GetFlag(FLAGS_logbuflevel)); });

// Maximum seconds between periodic flushes of buffered log data.
ABSL_FLAG(int32_t, logbufsecs, 30, "Buffer log messages for at most this many seconds.")
    .OnUpdate([] { g_logbufsecs.store(absl::GetFlag(FLAGS_logbufsecs)); });

// Approximate maximum log file size (in MB), same semantics as glog's --max_log_size.
ABSL_FLAG(uint32_t, max_log_size, 200,
          "Approximate maximum log file size (in MB). A value of 0 is treated as 1.");

// Backward compatibility flags from glog.
ABSL_FLAG(bool, logtostderr, false, "log messages go to stderr instead of logfiles");

namespace base {

bool FileLogSink::LogFile::Open(const std::string& base_path, int severity,
                                const std::string& pid_str, const std::string& user_str) {
  if (fp_) {
    Close(false);
  }

  if (base_path.empty()) {
    fp_ = reinterpret_cast<FILE*>(1);  // mark as dead
    return false;
  }

  path_ = absl::StrCat(base_path, ".", user_str, ".", kSeverityNames[severity], ".",
                       absl::FormatTime("%Y%m%d-%H%M%S", absl::Now(), absl::UTCTimeZone()), ".",
                       pid_str, ".log");

  fp_ = fopen(path_.c_str(), "ae");  // 'e' = O_CLOEXEC
  if (!fp_) {
    fprintf(stderr, "file_log_sink: fopen on %s failed: %s\n", path_.c_str(), strerror(errno));
    fp_ = reinterpret_cast<FILE*>(1);  // mark as dead
    return false;
  }
  file_length_ = 0;
  ResetFlushThresholds();

  if (!base_path.empty()) {
    string link_path = absl::StrCat(base_path, ".", kSeverityNames[severity]);
    string target = path_.substr(path_.rfind('/') + 1);
    unlink(link_path.c_str());
    (void)symlink(target.c_str(), link_path.c_str());
  }

  return true;
}

void FileLogSink::LogFile::WriteAndMaybeFlush(absl::string_view data, int sev) {
  size_t written = fwrite(data.data(), 1, data.size(), fp_);
  if (written != data.size()) {
    fprintf(stderr, "file_log_sink: fwrite to %s failed: %s; disabling file logging\n",
            path_.c_str(), strerror(errno));
    Close(true);
    return;
  }
  file_length_ += written;
  bytes_since_flush_ += written;

  const absl::Time now = absl::Now();
  if (sev > g_logbuflevel.load() || bytes_since_flush_ >= kFlushBytes || now >= next_flush_time_) {
    FlushLocked();
  }
}

void FileLogSink::LogFile::FlushLocked() {
  assert(fp_ && !dead());
  if (fflush(fp_) != 0) {
    fprintf(stderr, "file_log_sink: fflush to %s failed: %s; disabling file logging\n",
            path_.c_str(), strerror(errno));
    Close(true);
    return;
  }
  ResetFlushThresholds();
}

void FileLogSink::LogFile::Close(bool mark_dead_flag) {
  fclose(fp_);
  fp_ = mark_dead_flag ? reinterpret_cast<FILE*>(1) : nullptr;  // mark as dead or just closed
  file_length_ = 0;
  bytes_since_flush_ = 0;
}

void FileLogSink::LogFile::ResetFlushThresholds() {
  bytes_since_flush_ = 0;
  next_flush_time_ = absl::Now() + absl::Seconds(g_logbufsecs.load());
}

void FileLogSink::Init() {
  // Check backward compatibility flags from glog.
  // logtostderr means no file logging (only stderr).
  if (absl::GetFlag(FLAGS_logtostderr)) {
    absl::SetStderrThreshold(absl::LogSeverityAtLeast::kInfo);
    max_file_size_mb_ = 0;
    return;
  }

  base_dir_ = FindLoggingDir();
  if (base_dir_.empty()) {
    base_path_.clear();
    pid_str_.clear();
    return;
  }

  assert(base_dir_.back() == '/');

  base_path_ = absl::StrCat(base_dir_, ProgramBaseName());
  pid_str_ = std::to_string(getpid());
  user_str_ = MyUserName();
  max_file_size_mb_ = std::max<uint32_t>(1U, absl::GetFlag(FLAGS_max_log_size));
}

FileLogSink::~FileLogSink() {
  for (auto& f : files_) {
    if (f.active())
      f.Close(false);
  }
}

void FileLogSink::Send(const absl::LogEntry& entry) {
  if (max_file_size_mb_ == 0)
    return;

  // Clamp severity to [INFO=0, WARNING=1, ERROR=2]; FATAL → ERROR
  int sev = std::clamp(static_cast<int>(entry.log_severity()), 0, 2);

  absl::string_view line = entry.text_message_with_prefix_and_newline();

  // Write to files[sev] down to files[0].
  for (int i = sev; i >= 0; --i) {
    LogFile& lf = files_[i];
    std::lock_guard<std::mutex> lock(lf.mu_);

    if (lf.dead())
      continue;

    if (lf.needs_open(static_cast<size_t>(max_file_size_mb_) << 20)) {
      if (!lf.Open(base_path_, i, pid_str_, user_str_))
        continue;
    }

    lf.WriteAndMaybeFlush(line, sev);
  }
}

void FileLogSink::Flush() {
  for (auto& f : files_) {
    std::lock_guard<std::mutex> lock(f.mu_);
    if (f.active()) {
      f.FlushLocked();
    }
  }
}

}  // namespace base

#endif  // USE_ABSL_LOG

// Copyright 2024, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/file_log_sink.h"

#ifdef USE_ABSL_LOG

#include <absl/flags/flag.h>
#include <absl/log/log_entry.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <string>
#include <unistd.h>

#include "base/logging.h"  // for ProgramBaseName, MyUserName

namespace {

const char* kSeverityNames[] = {"INFO", "WARNING", "ERROR"};
constexpr size_t kFlushBytes = 1000000;

// Cached flag values updated via OnUpdate — avoids calling absl::GetFlag on every Send().
std::atomic<int> g_logbuflevel{0};
std::atomic<int> g_logbufsecs{30};

}  // namespace

// Messages at severity > logbuflevel are flushed immediately.
// Set to -1 to flush every message (disable buffering entirely).
ABSL_FLAG(int32_t, logbuflevel, 0,
          "Buffer log messages logged at this level or below. "
          "(-1 means don't buffer; 0 means buffer INFO only).")
    .OnUpdate([] { g_logbuflevel.store(absl::GetFlag(FLAGS_logbuflevel)); });

// Maximum seconds between periodic flushes of buffered log data.
ABSL_FLAG(int32_t, logbufsecs, 30,
          "Buffer log messages for at most this many seconds.")
    .OnUpdate([] { g_logbufsecs.store(absl::GetFlag(FLAGS_logbufsecs)); });

namespace base {

bool FileLogSink::LogFile::Open(const std::string& base_path, int severity,
                                const std::string& pid_str) {
  if (fp_) {
    Close(false);
  }

  path_ = base_path + "." + kSeverityNames[severity] + "." +
          absl::FormatTime("%Y%m%d-%H%M%S", absl::Now(), absl::UTCTimeZone()) + "." + pid_str;
  fp_ = fopen(path_.c_str(), "ae");  // 'e' = O_CLOEXEC
  if (!fp_) {
    fprintf(stderr, "file_log_sink: fopen on %s failed: %s\n", path_.c_str(), strerror(errno));
    return false;
  }
  file_length_ = 0;
  ResetFlushThresholds();
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
  if (sev > g_logbuflevel.load() || bytes_since_flush_ >= kFlushBytes ||
      now >= next_flush_time_) {
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

void FileLogSink::Init(std::string log_dir, uint32_t max_file_size_mb) {
  base_path_ = (log_dir.empty() ? "/tmp" : log_dir) + "/" + ProgramBaseName() + "." +
               MyUserName() + ".log";
  pid_str_ = std::to_string(getpid());
  max_file_size_bytes_ = static_cast<size_t>(max_file_size_mb) << 20;
}

FileLogSink::~FileLogSink() {
  for (auto& f : files_) {
    if (f.active())
      f.Close(false);
  }

}

void FileLogSink::Send(const absl::LogEntry& entry) {
  // Clamp severity to [INFO=0, WARNING=1, ERROR=2]; FATAL → ERROR
  int sev = std::clamp(static_cast<int>(entry.log_severity()), 0, 2);

  absl::string_view line = entry.text_message_with_prefix_and_newline();

  // Write to files[sev] down to files[0].
  for (int i = sev; i >= 0; --i) {
    LogFile& lf = files_[i];
    std::lock_guard<std::mutex> lock(lf.mu_);

    if (lf.dead())
      continue;

    if (lf.needs_open(max_file_size_bytes_)) {
      if (!lf.Open(base_path_, i, pid_str_))
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

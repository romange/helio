// Copyright 2024, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Absl LogSink that writes to per-severity log files, mirroring glog behavior.
// Files are named: <program>.<user>.<SEVERITY>.<yyyymmdd-HHMMSS>.<pid>.log
// A message at severity S is written to all files with severity <= S
// (e.g. ERROR goes to ERROR + WARNING + INFO files).
// Only compiled when USE_ABSL_LOG is defined.

#pragma once

#ifdef USE_ABSL_LOG

#include <absl/log/log_sink.h>
#include <absl/time/time.h>

#include <cstdio>
#include <mutex>
#include <string>

namespace base {

class FileLogSink : public absl::LogSink {
 public:
  FileLogSink() {
  }
  ~FileLogSink() override;

  // Uses --max_log_size (in MB) to control rotation size.
  void Init();

  void Send(const absl::LogEntry& entry) override;
  void Flush() override;

  const std::string& base_dir() const {
    return base_dir_;
  }

 private:
  // One per severity level: 0=INFO, 1=WARNING, 2=ERROR
  struct LogFile {
    std::mutex mu_;
    std::string path_;

    bool needs_open(size_t limit) const {
      return fp_ == nullptr || file_length_ >= limit;
    }
    bool dead() const {
      return reinterpret_cast<uintptr_t>(fp_) == 1;
    }
    bool active() const {
      return fp_ != nullptr && !dead();
    }

    bool Open(const std::string& base_path, int severity, const std::string& pid_str,
              const std::string& user_str);
    // Writes data and flushes based on sev and the cached flag values.
    void WriteAndMaybeFlush(absl::string_view data, int sev);
    void FlushLocked();  // caller must hold mu_
    void Close(bool mark_dead);

   private:
    void ResetFlushThresholds();

    FILE* fp_ = nullptr;
    size_t file_length_ = 0;
    size_t bytes_since_flush_ = 0;
    absl::Time next_flush_time_ = absl::InfinitePast();
  };

  std::string base_dir_;
  std::string base_path_;  // cached: <log_dir>/<program>
  std::string pid_str_;    // cached: string form of getpid()
  std::string user_str_;   // cached: MyUserName()
  uint32_t max_file_size_mb_ = 0;
  LogFile files_[3];
};

}  // namespace base

#endif  // USE_ABSL_LOG

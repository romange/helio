// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#ifdef USE_ABSL_LOG
#include <absl/log/absl_log.h>
#include <absl/log/absl_check.h>
#include <absl/log/globals.h>
#include <absl/log/vlog_is_on.h>
#include <absl/log/log_sink_registry.h>

#define CHECK ABSL_CHECK
#define CHECK_GT ABSL_CHECK_GT
#define CHECK_LT ABSL_CHECK_LT
#define CHECK_EQ ABSL_CHECK_EQ
#define CHECK_NE ABSL_CHECK_NE
#define CHECK_GE ABSL_CHECK_GE
#define CHECK_LE ABSL_CHECK_LE

#define DCHECK ABSL_DCHECK
#define DCHECK_GT ABSL_DCHECK_GT
#define DCHECK_LT ABSL_DCHECK_LT
#define DCHECK_EQ ABSL_DCHECK_EQ
#define DCHECK_NE ABSL_DCHECK_NE
#define DCHECK_GE ABSL_DCHECK_GE
#define DCHECK_LE ABSL_DCHECK_LE

#define DVLOG ABSL_DVLOG
#define VLOG ABSL_VLOG
#define LOG ABSL_LOG
#define LOG_IF ABSL_LOG_IF
#define LOG_FIRST_N ABSL_LOG_FIRST_N
#define VLOG_IF(verboselevel, condition) \
  ABSL_LOG_IF(INFO, (condition) && ABSL_VLOG_IS_ON(verboselevel))


template <typename T> T* CHECK_NOTNULL(T* t) {
  CHECK(t);
  return t;
}

#else
#include <glog/logging.h>

#define CONSOLE_INFO LOG_TO_SINK(base::ConsoleLogSink::instance(), INFO)

#endif

#include <string>

namespace base {
std::string ProgramAbsoluteFileName();

std::string ProgramBaseName();

std::string MyUserName();

#ifdef USE_ABSL_LOG

inline void FlushLogs() {
  absl::FlushLogSinks();
}

inline int SetVLogLevel(std::string_view module_pattern, int log_level) {
  return absl::SetVLogLevel(module_pattern, log_level);
}

#else

inline void FlushLogs() {
  google::FlushLogFiles(google::INFO);
}

inline int SetVLogLevel(std::string_view module_pattern, int log_level) {
  return google::SetVLOGLevel(module_pattern.data(), log_level);
}

class ConsoleLogSink : public google::LogSink {
 public:
  virtual void send(google::LogSeverity severity, const char* full_filename,
                    const char* base_filename, int line, const struct ::tm* tm_time,
                    const char* message, size_t message_len) override;

  static ConsoleLogSink* instance();
};
#endif
extern const char* kProgramName;

}  // namespace base

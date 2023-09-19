// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/logger.h"

#include <absl/strings/str_format.h>
#include <aws/core/utils/logging/LogLevel.h>

#include <cstdarg>

#include "base/logging.h"

namespace util {
namespace aws {

Aws::Utils::Logging::LogLevel Logger::GetLogLevel() const {
  return Aws::Utils::Logging::LogLevel::Debug;
}

void Logger::Log(Aws::Utils::Logging::LogLevel level, const char* tag, const char* format_str,
                 ...) {
  // Note Logger::Log is almost unused by the SDK, it instead uses LogStream.

  std::stringstream ss;

  va_list args;
  va_start(args, format_str);

  va_list tmp_args;
  va_copy(tmp_args, args);
  const int len = vsnprintf(nullptr, 0, format_str, tmp_args) + 1;
  va_end(tmp_args);

  std::vector<char> buf(len);
  vsnprintf(buf.data(), len, format_str, args);

  ss << buf.data();

  va_end(args);

  switch (level) {
    case Aws::Utils::Logging::LogLevel::Trace:
      DVLOG(2) << "aws: " << tag << ": " << ss.str();
      break;
    case Aws::Utils::Logging::LogLevel::Debug:
      VLOG(1) << "aws: " << tag << ": " << ss.str();
      break;
    case Aws::Utils::Logging::LogLevel::Info:
      LOG(INFO) << "aws: " << tag << ": " << ss.str();
      break;
    case Aws::Utils::Logging::LogLevel::Warn:
      LOG(WARNING) << "aws: " << tag << ": " << ss.str();
      break;
    case Aws::Utils::Logging::LogLevel::Error:
      LOG(WARNING) << "aws: " << tag << ": " << ss.str();
      break;
    case Aws::Utils::Logging::LogLevel::Fatal:
      LOG(FATAL) << "aws: " << tag << ": " << ss.str();
      break;
    default:
      break;
  }
}

void Logger::LogStream(Aws::Utils::Logging::LogLevel level, const char* tag,
                       const Aws::OStringStream& message_stream) {
  switch (level) {
    case Aws::Utils::Logging::LogLevel::Trace:
      DVLOG(2) << "aws: " << tag << ": " << message_stream.str();
      break;
    case Aws::Utils::Logging::LogLevel::Debug:
      VLOG(1) << "aws: " << tag << ": " << message_stream.str();
      break;
    case Aws::Utils::Logging::LogLevel::Info:
      LOG(INFO) << "aws: " << tag << ": " << message_stream.str();
      break;
    case Aws::Utils::Logging::LogLevel::Warn:
      LOG(WARNING) << "aws: " << tag << ": " << message_stream.str();
      break;
    case Aws::Utils::Logging::LogLevel::Error:
      LOG(WARNING) << "aws: " << tag << ": " << message_stream.str();
      break;
    case Aws::Utils::Logging::LogLevel::Fatal:
      LOG(FATAL) << "aws: " << tag << ": " << message_stream.str();
      break;
    default:
      break;
  }
}

void Logger::Flush() {
}

}  // namespace aws
}  // namespace util

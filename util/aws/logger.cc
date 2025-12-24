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

static void LogTo(Aws::Utils::Logging::LogLevel level, const char* tag, const char* message) {
  switch (level) {
    case Aws::Utils::Logging::LogLevel::Trace:
      DVLOG(2) << "aws: " << tag << ": " << message;
      break;
    case Aws::Utils::Logging::LogLevel::Debug:
      VLOG(2) << "aws: " << tag << ": " << message;
      break;
    case Aws::Utils::Logging::LogLevel::Info:
      VLOG(1) << "aws: " << tag << ": " << message;
      break;
    case Aws::Utils::Logging::LogLevel::Warn:
      VLOG(1) << "aws: " << tag << ": " << message;
      break;
    case Aws::Utils::Logging::LogLevel::Error:
      VLOG(1) << "aws: " << tag << ": " << message;
      break;
    case Aws::Utils::Logging::LogLevel::Fatal:
      LOG(FATAL) << "aws: " << tag << ": " << message;
      break;
    default:
      break;
  }
}

void Logger::Log(Aws::Utils::Logging::LogLevel level, const char* tag, const char* format_str,
                 ...) {
  // Note Logger::Log is almost unused by the SDK, it instead uses LogStream.
  // Copied formatting vararg from the AWS SDK.

  va_list args;
  va_start(args, format_str);

  vaLog(level, tag, format_str, args);

  va_end(args);
}

void Logger::LogStream(Aws::Utils::Logging::LogLevel level, const char* tag,
                       const Aws::OStringStream& message_stream) {
  LogTo(level, tag, message_stream.str().c_str());
}

void Logger::vaLog(Aws::Utils::Logging::LogLevel logLevel, const char* tag, const char* formatStr,
                   va_list args) {
  // 1. Create a copy of va_list to determine the required length
    va_list argsCopy;
    va_copy(argsCopy, args);

    // 2. Determine how many characters are needed (excluding null terminator)
    int len = std::vsnprintf(nullptr, 0, formatStr, argsCopy);
    va_end(argsCopy);

    if (len < 0) {
        // Handle formatting error
        return;
    }

    // 3. Allocate a buffer and format the string
    // We add 1 for the null terminator
    std::vector<char> buffer(len + 1);
    std::vsnprintf(buffer.data(), buffer.size(), formatStr, args);
    LogTo(logLevel, tag, buffer.data());
}

void Logger::Flush() {
}

}  // namespace aws
}  // namespace util

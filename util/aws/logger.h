// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/core/utils/logging/LogSystemInterface.h>

namespace util {
namespace aws {

class Logger : public Aws::Utils::Logging::LogSystemInterface {
 public:
  Aws::Utils::Logging::LogLevel GetLogLevel() const override;

  void Log(Aws::Utils::Logging::LogLevel level, const char* tag, const char* format_str,
           ...) override;

  void LogStream(Aws::Utils::Logging::LogLevel level, const char* tag,
                 const Aws::OStringStream& message_stream) override;

  void Flush() override;
};

}  // namespace aws
}  // namespace util

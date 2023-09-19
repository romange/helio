// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/aws.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#include "util/aws/logger.h"

namespace util {
namespace aws {

namespace {

// Required by both Init and Shutdown so make static.
static Aws::SDKOptions options;

}  // namespace

void Init() {
  // Add a glog based logger. The logger handles discarding logs by level so
  // use trace logging to get all logs from AWS then filter in the logger.
  options.loggingOptions.logger_create_fn =
      []() -> std::shared_ptr<Aws::Utils::Logging::LogSystemInterface> {
    return Aws::MakeShared<Logger>("helio");
  };
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;

  Aws::InitAPI(options);
}

void Shutdown() {
  Aws::ShutdownAPI(options);
}

}  // namespace aws
}  // namespace util

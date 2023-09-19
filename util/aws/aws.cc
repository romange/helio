// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/aws.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#include "util/aws/http_client_factory.h"
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

  // Disable starting the background IO event loop thread which we don't need.
  options.ioOptions.clientBootstrap_create_fn =
      []() -> std::shared_ptr<Aws::Crt::Io::ClientBootstrap> { return nullptr; };

  // We must use non-blocking network IO so use our own fiber based HTTP client.
  options.httpOptions.httpClientFactory_create_fn =
      []() -> std::shared_ptr<Aws::Http::HttpClientFactory> {
    return Aws::MakeShared<HttpClientFactory>("helio");
  };

  Aws::InitAPI(options);
}

void Shutdown() {
  Aws::ShutdownAPI(options);
}

}  // namespace aws
}  // namespace util

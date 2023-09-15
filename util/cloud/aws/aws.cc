// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/aws/aws.h"

#include <aws/core/Aws.h>

#include "util/cloud/aws/http_client_factory.h"

namespace util {
namespace cloud {
namespace aws {

namespace {

// Required by both Init and Shutdown so make static.
static Aws::SDKOptions options;

}  // namespace

void Init() {
  // TODO(andydunstall): Configure logging. Use a custom glog based logger?
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;

  // Disable starting the background IO event loop thread which we don't need.
  options.ioOptions.clientBootstrap_create_fn =
      []() -> std::shared_ptr<Aws::Crt::Io::ClientBootstrap> { return nullptr; };

  // We must use non-blocking network IO so use our own fiber based HTTP client.
  options.httpOptions.httpClientFactory_create_fn =
      []() -> std::shared_ptr<Aws::Http::HttpClientFactory> {
    return Aws::MakeShared<util::cloud::aws::HttpClientFactory>("helio");
  };

  // We don't use cURL so don't initialise or cleanup.
  options.httpOptions.initAndCleanupCurl = false;

  Aws::InitAPI(options);
}

void Shutdown() {
  Aws::ShutdownAPI(options);
}

}  // namespace aws
}  // namespace cloud
}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/aws.h"

#include <aws/core/Aws.h>

namespace util {
namespace aws {

namespace {

// Required by both Init and Shutdown so make static.
static Aws::SDKOptions options;

}  // namespace

void Init() {
  Aws::InitAPI(options);
}

void Shutdown() {
  Aws::ShutdownAPI(options);
}

}  // namespace aws
}  // namespace util

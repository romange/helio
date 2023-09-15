// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

namespace util {
namespace cloud {
namespace aws {

// Initialises the AWS library. This must be called before using any AWS
// services.
void Init();

void Shutdown();

}  // namespace aws
}  // namespace cloud
}  // namespace util

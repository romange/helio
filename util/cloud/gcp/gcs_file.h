// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include "io/file.h"
#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/http/https_client_pool.h"

namespace util {

namespace cloud {

static constexpr size_t kDefaultGCPPartSize = 1ULL << 23;  // 8MB.

io::Result<io::WriteFile*> OpenWriteGcsFile(const std::string& bucket, const std::string& key,
                                            GCPCredsProvider* creds_provider,
                                            http::ClientPool* pool,
                                            size_t part_size = kDefaultGCPPartSize);

}  // namespace cloud
}  // namespace util
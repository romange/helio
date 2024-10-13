// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include "io/file.h"
#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/http/https_client_pool.h"

namespace util {

namespace cloud {

static constexpr size_t kDefaultGCPPartSize = 1ULL << 23;  // 8MB.

struct GcsFileOptions {
  GCPCredsProvider* creds_provider = nullptr;
  http::ClientPool* pool = nullptr;
  bool pool_owned = false;  // true if file assumes ownership of the pool.
};

struct GcsWriteFileOptions : public GcsFileOptions {
  size_t part_size = kDefaultGCPPartSize;
};

using GcsReadFileOptions = GcsFileOptions;

io::Result<io::WriteFile*> OpenWriteGcsFile(const std::string& bucket, const std::string& key,
                                            const GcsWriteFileOptions& opts);

io::Result<io::ReadonlyFile*> OpenReadGcsFile(const std::string& bucket, const std::string& key,
                                              const GcsReadFileOptions& opts);

}  // namespace cloud
}  // namespace util
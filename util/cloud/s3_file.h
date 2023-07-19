// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "io/file.h"
#include "util/cloud/aws.h"
#include "util/http/http_client.h"

namespace util {
namespace cloud {

io::Result<io::ReadonlyFile*> OpenS3ReadFile(
    std::string_view region, std::string_view path, const AWS& aws, http::Client* client,
    const io::ReadonlyFile::Options& opts = io::ReadonlyFile::Options{});

// Takes ownership over client.
io::Result<io::WriteFile*> OpenS3WriteFile(std::string_view region, std::string_view key_path,
                                           const AWS& aws, http::Client* client);

}  // namespace cloud
}  // namespace util

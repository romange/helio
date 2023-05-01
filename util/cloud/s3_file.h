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
    std::string_view region, std::string_view path, AWS* aws, http::Client* client,
    const io::ReadonlyFile::Options& opts = io::ReadonlyFile::Options{});

io::Result<io::WriteFile*> OpenS3WriteFile(std::string_view region, std::string_view key_path,
                                           AWS* aws, http::Client* client);

}  // namespace cloud
}  // namespace util
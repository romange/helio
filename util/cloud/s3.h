// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <system_error>

#include "io/io.h"
#include "util/http/http_client.h"

namespace util {
namespace cloud {
class AWS;

using ListBucketsResult = io::Result<std::vector<std::string>>;
ListBucketsResult ListS3Buckets(const AWS& aws, http::Client* http_client);

}  // namespace cloud

}  // namespace util

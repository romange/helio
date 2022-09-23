// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <system_error>

namespace util {

// dest_ip must be at least INET_ADDRSTRLEN because currently we
// resolve only IPv4 addresses.
// returns 0 if resolution succeeded, negative error otherwise.
// wait_ms - sets deadline to resolve, 0 if infinite.
std::error_code DnsResolve(const char* dns, uint32_t wait_ms, char dest_ip[]);

}  // namespace util

// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <system_error>

namespace util {
namespace fb2 {

class ProactorBase;
// Resolve addresss - either ipv4 or ipv6. dest_ip must be able to hold INET6_ADDRSTRLEN
std::error_code DnsResolve(const std::string& host, uint32_t wait_ms, char dest_ip[],
                           ProactorBase* proactor);

void InitDnsResolver(uint32_t timeout_ms);
std::error_code DnsResolve(const std::string& host, char dest_ip[]);

}  // namespace fb2
}  // namespace util

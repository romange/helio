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
std::error_code DnsResolve(std::string host, uint32_t wait_ms, char dest_ip[],
                           ProactorBase* proactor);

}  // namespace fb2
}  // namespace util

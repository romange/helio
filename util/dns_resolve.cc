// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/dns_resolve.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "base/logging.h"
#include "util/fibers/fibers_ext.h"

namespace util {

using namespace std;

/// TODO: to reimplement it with c-ares because getaddrinfo_a is not supported by musl and
/// older glibc versions.
/// check out in c-ares: ares_init, ares_gethostbyname,etc.
///
error_code DnsResolve(const char* dns, uint32_t wait_ms, char dest_ip[]) {
  // TODO: to implemet deadline semantics with wait_ms. currently ignored.
  struct addrinfo hints;
  struct addrinfo *servinfo = nullptr;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_ALL;

  static_assert(INET_ADDRSTRLEN < INET6_ADDRSTRLEN, "");

  int res = getaddrinfo(dns, NULL, &hints, &servinfo);
  if (res != 0) {
    LOG(ERROR) << "Error calling getaddrinfo " << gai_strerror(res);
    return make_error_code(errc::address_not_available);
  }


  res = EAI_FAMILY;
  for (addrinfo* p = servinfo; p != NULL; p = p->ai_next) {
    if (p->ai_family == AF_INET) {
      struct sockaddr_in* ipv4 = (struct sockaddr_in*)p->ai_addr;
      const char* inet_res = inet_ntop(p->ai_family, &ipv4->sin_addr, dest_ip, INET6_ADDRSTRLEN);
      CHECK_NOTNULL(inet_res);
      res = 0;
      break;
    }
    LOG(WARNING) << "Only IPv4 is supported";
  }

  freeaddrinfo(servinfo);

  return res == 0 ? error_code{} : make_error_code(errc::address_family_not_supported);
}

}  // namespace util

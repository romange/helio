// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/utils.h"

namespace util::cloud {
namespace detail {

using namespace std;
namespace h2 = boost::beast::http;

EmptyRequestImpl::EmptyRequestImpl(h2::verb req_verb, std::string_view url)
    : req_{req_verb, boost::beast::string_view{url.data(), url.size()}, 11} {
}

std::error_code EmptyRequestImpl::Send(http::Client* client) {
  return client->Send(req_);
}

std::error_code DynamicBodyRequestImpl::Send(http::Client* client) {
  return client->Send(req_);
}

constexpr unsigned kTcpKeepAliveInterval = 30;

error_code EnableKeepAlive(int fd) {
  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  val = kTcpKeepAliveInterval;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  val = kTcpKeepAliveInterval;
#ifdef __APPLE__
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }
#else
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }
#endif

  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  return std::error_code{};
}

}  // namespace detail
}  // namespace util::cloud

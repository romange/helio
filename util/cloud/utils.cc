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

}  // namespace detail
}  // namespace util::cloud

// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcp_utils.h"

#include <absl/strings/str_cat.h>


#include "base/logging.h"
#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/http/http_client.h"

namespace util::cloud {
using namespace std;

namespace h2 = boost::beast::http;
const char GCS_API_DOMAIN[] = "storage.googleapis.com";

string AuthHeader(string_view access_token) {
  return absl::StrCat("Bearer ", access_token);
}

namespace detail {

EmptyRequestImpl CreateGCPEmptyRequest(boost::beast::http::verb req_verb, std::string_view url,
                                       const std::string_view access_token) {
  EmptyRequestImpl res(req_verb, url);
  res.SetHeader(h2::field::host, GCS_API_DOMAIN);
  res.SetHeader(h2::field::authorization, AuthHeader(access_token));
  return res;
}

}  // namespace detail
}  // namespace util::cloud

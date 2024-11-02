// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include "base/logging.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/utils.h"
#include "util/http/http_client.h"
#include "util/http/http_common.h"

#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

namespace util {
namespace cloud::azure {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

const char kVersion[] = "2025-01-05";


void HMAC(absl::string_view key, absl::string_view msg, uint8_t dest[32]) {
  // HMAC_xxx are deprecated since openssl 3.0
  // Ubuntu 20.04 uses openssl 1.1.
  HMAC_CTX* hmac = HMAC_CTX_new();

  CHECK_EQ(1, HMAC_Init_ex(hmac, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                           EVP_sha256(), NULL));

  CHECK_EQ(1, HMAC_Update(hmac, reinterpret_cast<const uint8_t*>(msg.data()), msg.size()));

  uint8_t* ptr = reinterpret_cast<uint8_t*>(dest);
  unsigned len = 32;
  CHECK_EQ(1, HMAC_Final(hmac, ptr, &len));
  HMAC_CTX_free(hmac);
  CHECK_EQ(len, 32u);
}

string ComputeSignature(string_view account, const boost::beast::http::header<true>& req_header,
                        string_view account_key) {
  string key_bin;
  CHECK(absl::Base64Unescape(account_key, &key_bin));

  // see here:
  // https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#blob-queue-and-file-services-shared-key-authorization
  string new_lines;
  for (unsigned i = 0; i < 12; ++i)
    absl::StrAppend(&new_lines, "\n");

  vector<pair<string_view, string_view>> x_head;
  for (const auto& h : req_header) {
    if (h.name_string().starts_with("x-ms-")) {
      x_head.emplace_back(detail::FromBoostSV(h.name_string()), detail::FromBoostSV(h.value()));
    }
  }
  sort(x_head.begin(), x_head.end());
  string to_sign = absl::StrCat("GET", new_lines);
  for (const auto& p : x_head) {
    absl::StrAppend(&to_sign, p.first, ":", p.second, "\n");
  }

  string_view target = detail::FromBoostSV(req_header.target());
  http::QueryParam qparams = http::ParseQuery(target);
  string_view path = qparams.first;
  DCHECK(absl::StartsWith(path, "/"));

  http::QueryArgs args = http::SplitQuery(qparams.second);
  sort(args.begin(), args.end());
  string query_canon;
  for (const auto& p : args) {
    absl::StrAppend(&query_canon, "\n", p.first, ":", p.second);
  }

  string canonic_resource = absl::StrCat("/", account, path, query_canon);
  VLOG(1) << "Canonical resource: " << absl::CEscape(canonic_resource);

  absl::StrAppend(&to_sign, canonic_resource);

  uint8_t dest[32];
  HMAC(key_bin, to_sign, dest);

  string signature = absl::Base64Escape(string_view{reinterpret_cast<char*>(dest), sizeof(dest)});
  return signature;
}

}  // namespace

error_code Credentials::Init(unsigned) {
  const char* name = getenv("AZURE_STORAGE_ACCOUNT");
  const char* secret_key = getenv("AZURE_STORAGE_KEY");
  if (!name || !secret_key) {
    return make_error_code(errc::permission_denied);
  }

  account_name_ = name;
  account_key_ = secret_key;

  return {};
}

void Credentials::Sign(detail::HttpRequestBase* req) const {
  const absl::TimeZone utc_tz = absl::UTCTimeZone();
  string date = absl::FormatTime("%a, %d %b %Y %H:%M:%S GMT", absl::Now(), utc_tz);
  req->SetHeader("x-ms-date", date);
  req->SetHeader("x-ms-version", kVersion);

  string signature = ComputeSignature(account_name_, req->GetHeaders(), account_key_);
  req->SetHeader("Authorization", absl::StrCat("SharedKey ", account_name_, ":", signature));
}

std::error_code Credentials::RefreshToken() {
  // TBD
  return {};
}


}  // namespace cloud::azure
}  // namespace util
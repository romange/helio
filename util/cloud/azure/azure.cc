// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>

#include <boost/beast/http/string_body.hpp>
#include <pugixml.hpp>

#include "base/logging.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/utils.h"
#include "util/http/http_client.h"
#include "util/http/https_client_pool.h"

#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

namespace util {
namespace cloud::azure {

using namespace std;
namespace h2 = boost::beast::http;

error_code CredsProvider::Init() {
  const char* name = getenv("AZURE_STORAGE_ACCOUNT");
  const char* secret_key = getenv("AZURE_STORAGE_KEY");
  if (!name || !secret_key) {
    return make_error_code(errc::permission_denied);
  }

  account_name_ = name;
  account_key_ = secret_key;

  return {};
}

auto UnexpectedError(errc code) {
  return nonstd::make_unexpected(make_error_code(code));
}

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

string Sign(string_view account, const boost::beast::http::header<true>& req_header,
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
  string canonic_resource = absl::StrCat("/", account, "/\n", "comp:list");
  absl::StrAppend(&to_sign, canonic_resource);

  uint8_t dest[32];
  HMAC(key_bin, to_sign, dest);

  string signature = absl::Base64Escape(string_view{reinterpret_cast<char*>(dest), sizeof(dest)});
  return signature;
}

io::Result<vector<string>> ParseXmlListBuckets(string_view xml_resp) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_buffer(xml_resp.data(), xml_resp.size());

  if (!result) {
    LOG(ERROR) << "Could not parse xml response " << result.description();
    return UnexpectedError(errc::bad_message);
  }

  pugi::xml_node root = doc.child("EnumerationResults");
  if (root.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find root node " << xml_resp;
    return UnexpectedError(errc::bad_message);
  }

  pugi::xml_node buckets = root.child("Containers");
  if (buckets.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find buckets node " << xml_resp;
    return UnexpectedError(errc::bad_message);
  }

  vector<string> res;
  for (pugi::xml_node bucket = buckets.child("Container"); bucket; bucket = bucket.next_sibling()) {
    res.push_back(bucket.child_value("Name"));
  }
  return res;
}

error_code CredsProvider::ListContainers(function<void(ContainerItem)> cb) {
  SSL_CTX* ctx = util::http::TlsClient::CreateSslContext();
  absl::Cleanup cleanup([ctx] { util::http::TlsClient::FreeContext(ctx); });

  fb2::ProactorBase* pb = fb2::ProactorBase::me();
  CHECK(pb);
  string endpoint = account_name_ + ".blob.core.windows.net";
  unique_ptr<http::ClientPool> pool(new http::ClientPool(endpoint, ctx, pb));
  pool->set_connect_timeout(2000);

  auto client = pool->GetHandle();
  CHECK(client);
  detail::EmptyRequestImpl req(h2::verb::get, "/?comp=list");
  const absl::TimeZone utc_tz = absl::UTCTimeZone();
  string date = absl::FormatTime("%a, %d %b %Y %H:%M:%S GMT", absl::Now(), utc_tz);
  req.SetHeader("x-ms-date", date);
  const char kVersion[] = "2025-01-05";
  req.SetHeader("x-ms-version", kVersion);

  string signature = Sign(account_name_, req.GetHeaders(), account_key_);
  req.SetHeader("Authorization", absl::StrCat("SharedKey ", account_name_, ":", signature));
  req.SetHeader(h2::field::host, endpoint);
  req.SetHeader(h2::field::accept_encoding, "gzip, deflate");

  DVLOG(1) << "Request: " << req.GetHeaders();
  auto ec = req.Send(client->get());
  CHECK(!ec) << ec;

  h2::response_parser<h2::empty_body> parser;
  ec = client->get()->ReadHeader(&parser);
  CHECK(!ec) << ec;
  DVLOG(1) << "Response: " << parser.get().base();
  h2::response_parser<h2::string_body> resp(std::move(parser));
  client->get()->Recv(&resp);

  auto msg = resp.release();
  DVLOG(1) << "Body: " << msg.body();
  auto res = ParseXmlListBuckets(msg.body());
  if (!res) {
    return res.error();
  }

  for (const auto& b : *res) {
    cb(b);
  }
  return {};
}

}  // namespace cloud::azure
}  // namespace util
// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/beast/http/string_body.hpp>
#include <pugixml.hpp>

#include "base/logging.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/utils.h"
#include "util/http/http_client.h"
#include "util/http/http_common.h"
#include "util/http/https_client_pool.h"

#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

namespace util {
namespace cloud::azure {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

const char kVersion[] = "2025-01-05";

auto UnexpectedError(errc code) {
  return nonstd::make_unexpected(make_error_code(code));
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

/*
<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://stagingv2dataplane.blob.core.windows.net/" ContainerName="mycontainer">
	<Blobs>
		<Blob>
			<Name>my/path.txt</Name>
			<Properties>
				<Creation-Time>Mon, 07 Oct 2024 11:09:55 GMT</Creation-Time>
				<Last-Modified>Mon, 07 Oct 2024 11:09:55 GMT</Last-Modified>
				<Etag>0x8DCE6C092FD371</Etag>
				<Content-Length>35115002</Content-Length>
				<Content-Type>...</Content-Type>
				<Content-Encoding />
				<Content-Language />
				<Content-CRC64 />
				<Content-MD5>OH10A4F0MqW0HIWHW2sEMg==</Content-MD5>
				<Cache-Control />
				<Content-Disposition />
				<BlobType>BlockBlob</BlobType>
				<AccessTier>Hot</AccessTier>
				<AccessTierInferred>true</AccessTierInferred>
				<LeaseStatus>unlocked</LeaseStatus>
				<LeaseState>available</LeaseState>
				<ServerEncrypted>true</ServerEncrypted>
			</Properties>
			<OrMetadata />
		</Blob>
    .....
*/
io::Result<vector<string>> ParseXmlListBlobs(string_view xml_resp) {
    pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_buffer(xml_resp.data(), xml_resp.size());

  if (!result) {
    LOG(ERROR) << "Could not parse xml response " << result.description();
    return UnexpectedError(errc::bad_message);
  }

  pugi::xml_node root = doc.child("EnumerationResults");
  if (root.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find root node ";
    return UnexpectedError(errc::bad_message);
  }

  pugi::xml_node blobs = root.child("Blobs");
  if (blobs.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find Blobs node ";
    return UnexpectedError(errc::bad_message);
  }

  vector<string> res;
  for (pugi::xml_node bucket = blobs.child("Blob"); bucket; bucket = bucket.next_sibling()) {
    res.push_back(bucket.child_value("Name"));
  }
  return res;
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

detail::EmptyRequestImpl FillRequest(string_view endpoint, string_view url, CredsProvider* creds) {
  detail::EmptyRequestImpl req(h2::verb::get, url);
  const absl::TimeZone utc_tz = absl::UTCTimeZone();
  string date = absl::FormatTime("%a, %d %b %Y %H:%M:%S GMT", absl::Now(), utc_tz);
  req.SetHeader("x-ms-date", date);
  req.SetHeader("x-ms-version", kVersion);

  const string& account_name = creds->account_name();
  string signature = Sign(account_name, req.GetHeaders(), creds->account_key());
  req.SetHeader("Authorization", absl::StrCat("SharedKey ", account_name, ":", signature));
  req.SetHeader(h2::field::host, endpoint);
  req.SetHeader(h2::field::accept_encoding, "gzip, deflate");

  return req;
}

unique_ptr<http::ClientPool> CreatePool(const string& endpoint, SSL_CTX* ctx,
                                        fb2::ProactorBase* pb) {
  CHECK(pb);
  unique_ptr<http::ClientPool> pool(new http::ClientPool(endpoint, ctx, pb));
  pool->set_connect_timeout(2000);
  return pool;
}

}  // namespace

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

error_code Storage::ListContainers(function<void(const ContainerItem&)> cb) {
  SSL_CTX* ctx = http::TlsClient::CreateSslContext();
  absl::Cleanup cleanup([ctx] { http::TlsClient::FreeContext(ctx); });

  string endpoint = creds_->account_name() + ".blob.core.windows.net";
  unique_ptr<http::ClientPool> pool = CreatePool(endpoint, ctx, fb2::ProactorBase::me());

  auto client = pool->GetHandle();
  CHECK(client);
  detail::EmptyRequestImpl req = FillRequest(endpoint, "/?comp=list", creds_);

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

error_code Storage::List(string_view container, function<void(const ObjectItem&)> cb) {
  SSL_CTX* ctx = http::TlsClient::CreateSslContext();
  absl::Cleanup cleanup([ctx] { http::TlsClient::FreeContext(ctx); });

  string endpoint = creds_->account_name() + ".blob.core.windows.net";
  unique_ptr<http::ClientPool> pool = CreatePool(endpoint, ctx, fb2::ProactorBase::me());

  auto client = pool->GetHandle();
  CHECK(client);
  string url = absl::StrCat("/", container, "?restype=container&comp=list");
  detail::EmptyRequestImpl req = FillRequest(endpoint, url, creds_);

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
  auto blobs = ParseXmlListBlobs(msg.body());
  if (!blobs)
    return blobs.error();

  for (const auto& b : *blobs) {
    cb(b);
  }

  return {};
}

}  // namespace cloud::azure
}  // namespace util
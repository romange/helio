// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/azure/storage.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <boost/beast/http/string_body.hpp>
#include <pugixml.hpp>

#include "base/logging.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/utils.h"
#include "util/http/http_client.h"
#include "util/http/https_client_pool.h"

namespace util {
namespace cloud::azure {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

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
<EnumerationResults ServiceEndpoint="https://stagingv2dataplane.blob.core.windows.net/"
ContainerName="mycontainer"> <Blobs> <Blob> <Name>my/path.txt</Name> <Properties>
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

unique_ptr<http::ClientPool> CreatePool(const string& endpoint, SSL_CTX* ctx,
                                        fb2::ProactorBase* pb) {
  CHECK(pb);
  unique_ptr<http::ClientPool> pool(new http::ClientPool(endpoint, ctx, pb));
  pool->set_connect_timeout(2000);
  pool->SetOnConnect([](int fd) {
    auto ec = detail::EnableKeepAlive(fd);
    LOG_IF(WARNING, ec) << "Error setting keep alive " << ec.message() << " " << fd;
  });

  return pool;
}

detail::EmptyRequestImpl FillRequest(string_view endpoint, string_view url, Credentials* provider) {
  detail::EmptyRequestImpl req(h2::verb::get, url);
  req.SetHeader(h2::field::host, endpoint);
  req.SetHeader(h2::field::accept_encoding, "gzip, deflate");
  req.SetHeader(h2::field::accept, "*/*");

  provider->Sign(&req);

  return req;
}

}  // namespace

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

error_code Storage::List(string_view container, unsigned max_results,
                         function<void(const ObjectItem&)> cb) {
  SSL_CTX* ctx = http::TlsClient::CreateSslContext();
  absl::Cleanup cleanup([ctx] { http::TlsClient::FreeContext(ctx); });

  string endpoint = creds_->account_name() + ".blob.core.windows.net";
  unique_ptr<http::ClientPool> pool = CreatePool(endpoint, ctx, fb2::ProactorBase::me());

  auto client = pool->GetHandle();
  CHECK(client);
  string url = absl::StrCat("/", container, "?restype=container&comp=list");
  absl::StrAppend(&url, "&maxresults=", max_results);
  detail::EmptyRequestImpl req = FillRequest(endpoint, url, creds_);

  DVLOG(1) << "Request: " << req.GetHeaders();
  auto ec = req.Send(client->get());
  CHECK(!ec) << ec;

  h2::response_parser<h2::empty_body> parser;
  ec = client->get()->ReadHeader(&parser);
  CHECK(!ec) << ec;
  DVLOG(1) << "Response: " << parser.get().base();
  h2::response_parser<h2::string_body> resp(std::move(parser));
  auto be = client->get()->Recv(&resp);
  if (be)
    return be;

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
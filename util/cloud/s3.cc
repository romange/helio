// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/s3.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <pugixml.hpp>

#include "base/logging.h"
#include "util/cloud/aws.h"
#include "util/cloud/s3_file.h"
#include "util/fibers/proactor_base.h"
#include "util/http/encoding.h"

namespace util {
namespace cloud {

using namespace std;
namespace h2 = boost::beast::http;
using nonstd::make_unexpected;

// Max number of keys in AWS response.
const unsigned kAwsMaxKeys = 1000;

inline std::string_view std_sv(const ::boost::beast::string_view s) {
  return std::string_view{s.data(), s.size()};
}

bool IsAwsEndpoint(string_view endpoint) {
  return absl::EndsWith(endpoint, ".amazonaws.com");
}

namespace xml {

ListBucketsResult ParseXmlListBuckets(string_view xml_resp) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_buffer(xml_resp.data(), xml_resp.size());
  if (!result) {
    LOG(ERROR) << "Could not parse xml response " << result.description();
    return make_unexpected(make_error_code(errc::bad_message));
  }

  pugi::xml_node root = doc.child("ListAllMyBucketsResult");
  if (root.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find root node " << xml_resp;
    return make_unexpected(make_error_code(errc::bad_message));
  }

  pugi::xml_node buckets = root.child("Buckets");
  if (buckets.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find buckets node " << xml_resp;
    return make_unexpected(make_error_code(errc::bad_message));
  }

  vector<string> res;
  for (pugi::xml_node bucket = buckets.child("Bucket"); bucket; bucket = bucket.next_sibling()) {
    res.emplace_back(bucket.child("Name").text().get());
  }

  return res;
}

ListObjectsResult ParseListObj(string_view xml_resp, S3Bucket::ListObjectCb cb) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_buffer(xml_resp.data(), xml_resp.size());
  if (!result) {
    LOG(ERROR) << "Could not parse xml response " << result.description();
    return make_unexpected(make_error_code(errc::bad_message));
  }

  string_view last_key;

  pugi::xml_node root = doc.child("ListBucketResult");
  if (root.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find root node " << xml_resp;
    return make_unexpected(make_error_code(errc::bad_message));
  }

  // text() provides a convenient interface to avoid checking for potentially missing
  // fields and rely on the defaults.
  bool truncated = root.child("IsTruncated").text().as_bool();
  for (pugi::xml_node contents = root.child("Contents"); contents;
       contents = contents.next_sibling("Contents")) {
    size_t sz = contents.child("Size").text().as_ullong();
    string_view key = contents.child("Key").text().get();
    if (!key.empty())
      cb(sz, key);
    last_key = key;
  }

  return truncated ? std::string{last_key} : "";
}

}  // namespace xml

ListBucketsResult ListS3Buckets(AWS* aws, http::Client* http_client) {
  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  req.set(h2::field::host, http_client->host());
  h2::response<h2::string_body> resp;

  AwsSignKey skey = aws->GetSignKey(aws->connection_data().region);
  auto ec = aws->SendRequest(http_client, &skey, &req, &resp);
  if (ec) {
    return make_unexpected(ec);
  }

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "http error: " << resp;
    return make_unexpected(make_error_code(errc::inappropriate_io_control_operation));
  }

  VLOG(1) << "ListS3Buckets: " << resp;

  return xml::ParseXmlListBuckets(resp.body());
}

S3Bucket::S3Bucket(const AWS& aws, string_view bucket, string_view region)
    : aws_(aws), bucket_(bucket), region_(region) {
  CHECK(!bucket.empty());

  if (region.empty()) {
    region = aws_.connection_data().region;
    if (region.empty()) {
      region = "us-east-1";
    }
  }
  skey_ = aws.GetSignKey(region);
}

S3Bucket S3Bucket::FromEndpoint(const AWS& aws, string_view endpoint, string_view bucket) {
  S3Bucket res(aws, bucket);
  res.endpoint_ = endpoint;
  return res;
}

std::error_code S3Bucket::Connect(uint32_t ms) {
  ProactorBase* pb = ProactorBase::me();
  CHECK(pb);

  http_client_.reset(new http::Client{pb});
  http_client_->AssignOnConnect([](int fd) {
    int val = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) < 0)
      return;

    val = 20;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0)
      return;

    val = 60;
#ifdef __APPLE__
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &val, sizeof(val)) < 0)
      return;
#else
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0)
      return;
#endif

    val = 3;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0)
      return;
  });

  http_client_->set_connect_timeout_ms(ms);

  return ConnectInternal();
}

ListObjectsResult S3Bucket::ListObjects(string_view bucket_path, ListObjectCb cb,
                                        std::string_view marker, unsigned max_keys) {
  CHECK_LE(max_keys, kAwsMaxKeys);

  string host = http_client_->host();
  std::string path;

  // Build full request path.
  if (IsAwsEndpoint(host)) {
    path.append("/?");
  } else {
    path.append("/").append(bucket_).append("?");
  }

  if (bucket_path != "")
    path += absl::StrCat("prefix=", util::http::UrlEncode(bucket_path), "&");

  if (marker != "")
    path += absl::StrCat("marker=", util::http::UrlEncode(marker), "&");

  if (max_keys != kAwsMaxKeys)
    path += absl::StrCat("max-keys=", max_keys, "&");

  CHECK(path.back() == '?' || path.back() == '&');
  path.pop_back();

  // Send request.
  h2::request<h2::empty_body> req{h2::verb::get, path, 11};
  req.set(h2::field::host, host);

  h2::response<h2::string_body> resp;

  error_code ec = aws_.SendRequest(http_client_.get(), &skey_, &req, &resp);
  if (ec)
    return make_unexpected(ec);

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "http error: " << resp;
    return make_unexpected(make_error_code(errc::connection_refused));
  }

  if (!absl::StartsWith(resp.body(), "<?xml"))
    return make_unexpected(make_error_code(errc::bad_message));

  // Sometimes s3 response contains multiple xml documents. xml2lib does not know how to parse them
  // or I did not find the way to do it. So I just check for another marker.
  string_view xml(resp.body());
  auto pos = xml.find("<?xml", 6);
  if (pos != string_view::npos) {
    VLOG(1) << "Removing preliminary xml " << xml.substr(0, pos);
    xml.remove_prefix(pos);
  }
  VLOG(1) << "ObjListResp: " << xml;
  return xml::ParseListObj(xml, std::move(cb));
}

error_code S3Bucket::ListAllObjects(string_view bucket_path, ListObjectCb cb) {
  std::string marker = "";
  unsigned max_keys = kAwsMaxKeys;
  do {
    auto res = ListObjects(bucket_path, cb, marker, max_keys);
    if (!res)
      return res.error();

    marker = res.value();
  } while (!marker.empty());

  return std::error_code{};
}

io::Result<io::ReadonlyFile*> S3Bucket::OpenReadFile(string_view path,
                                                     const io::ReadonlyFile::Options& opts) {
  string host = http_client_->host();
  string full_path{path};
  if (IsAwsEndpoint(host)) {
  } else {
    full_path = absl::StrCat(bucket_, "/", path);
  }
  return OpenS3ReadFile(region_, full_path, aws_, http_client_.get(), opts);
}

io::Result<io::WriteFile*> S3Bucket::OpenWriteFile(std::string_view path) {
  string host = http_client_->host();
  string full_path{path};
  if (IsAwsEndpoint(host)) {
  } else {
    full_path = absl::StrCat(bucket_, "/", path);
  }

  unique_ptr http_client = std::move(http_client_);
  error_code ec = Connect(http_client->connect_timeout_ms());
  if (ec)
    return make_unexpected(ec);

  return OpenS3WriteFile(region_, full_path, aws_, http_client.release());
}

string S3Bucket::GetHost() const {
  if (!endpoint_.empty())
    return endpoint_;

  // fallback to default aws endpoint.
  if (region_.empty())
    return "s3.amazonaws.com";
  return absl::StrCat("s3.", region_, ".amazonaws.com");
}

error_code S3Bucket::ConnectInternal() {
  string host = GetHost();
  auto pos = host.rfind(':');
  string port;

  if (pos != string::npos) {
    port = host.substr(pos + 1);
    host = host.substr(0, pos);
  } else {
    port = "80";
  }

  bool is_aws = IsAwsEndpoint(host);
  if (is_aws)
    host = absl::StrCat(bucket_, ".", host);

  VLOG(1) << "Connecting to " << host << ":" << port;
  auto ec = http_client_->Connect(host, port);
  if (ec)
    return ec;

  if (region_.empty()) {
    ec = DeriveRegion();
  }

  return ec;
}

error_code S3Bucket::DeriveRegion() {
  h2::request<h2::empty_body> req(h2::verb::head, "/", 11);
  req.set(h2::field::host, http_client_->host());
  bool is_aws = IsAwsEndpoint(http_client_->host());

  h2::response_parser<h2::string_body> parser;

  if (is_aws) {
    parser.skip(true);  // for HEAD requests we do not get the body.
  } else {
    string url = absl::StrCat("/", bucket_, "?location=");
    req.target(url);

    // TODO: can we keep HEAD for other providers?
    req.method(h2::verb::get);
  }

  skey_.Sign(AWS::kEmptySig, &req);
  error_code ec = http_client_->Send(req);
  if (ec)
    return ec;

  ec = http_client_->ReadHeader(&parser);
  if (ec)
    return ec;

  h2::header<false, h2::fields>& header = parser.get();

  // I deliberately do not check for http status. AWS can return 400 or 403 and it still reports
  // the region.
  VLOG(1) << "LocationResp: " << header;
  auto src = header["x-amz-bucket-region"];
  if (src.empty()) {
    LOG(ERROR) << "x-amz-bucket-region is absent in response: " << header;
    return make_error_code(errc::bad_message);
  }

  region_ = std::string(src);
  skey_ = aws_.GetSignKey(region_);
  if (header[h2::field::connection] == "close") {
    ec = http_client_->Reconnect();
    if (ec)
      return ec;
  } else if (!parser.is_done()) {
    // Drain http response.
    ec = http_client_->Recv(&parser);
  }

  return ec;
}

}  // namespace cloud
}  // namespace util

// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/aws/s3_storage.h"

#include <absl/functional/function_ref.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <optional>
#include <pugixml.hpp>

#include "base/logging.h"
#include "strings/escaping.h"
#include "util/cloud/utils.h"
#include "util/fibers/proactor_base.h"
#include "util/http/https_client_pool.h"

namespace util {
namespace cloud::aws {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

constexpr unsigned kSendRetries = 3;

auto UnexpectedError(errc code) {
  return nonstd::make_unexpected(make_error_code(code));
}

unique_ptr<http::ClientPool> CreatePool(const string& endpoint, SSL_CTX* ctx, fb2::ProactorBase* pb,
                                        unsigned connect_ms) {
  CHECK(pb);
  unique_ptr<http::ClientPool> pool(new http::ClientPool(endpoint, ctx, pb));
  pool->set_connect_timeout(connect_ms);
  pool->SetOnConnect([](int fd) {
    auto ec = detail::EnableKeepAlive(fd);
    LOG_IF(WARNING, ec) << "Error setting keep alive " << ec.message() << " " << fd;
  });
  return pool;
}

detail::EmptyRequestImpl FillRequest(string_view endpoint, string_view url,
                                     CredentialsProvider* provider) {
  detail::EmptyRequestImpl req(h2::verb::get, url);
  req.SetHeader(h2::field::host, endpoint);
  provider->Sign(&req);
  return req;
}

// Parses S3 ListAllMyBucketsResult XML response.
io::Result<vector<string>> ParseXmlListBuckets(string_view xml) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_buffer(xml.data(), xml.size());
  if (!result) {
    LOG(ERROR) << "aws: failed to parse ListBuckets XML: " << result.description();
    return UnexpectedError(errc::bad_message);
  }

  pugi::xml_node root = doc.child("ListAllMyBucketsResult");
  if (root.type() != pugi::node_element) {
    LOG(ERROR) << "aws: ListBuckets: missing ListAllMyBucketsResult node";
    return UnexpectedError(errc::bad_message);
  }

  pugi::xml_node buckets = root.child("Buckets");
  if (buckets.type() != pugi::node_element) {
    LOG(ERROR) << "aws: ListBuckets: missing Buckets node";
    return UnexpectedError(errc::bad_message);
  }

  vector<string> res;
  for (pugi::xml_node bucket = buckets.child("Bucket"); bucket;
       bucket = bucket.next_sibling("Bucket")) {
    res.push_back(bucket.child_value("Name"));
  }
  return res;
}

// Parses S3 ListObjectsV2 XML response, calling cb for each item.
// Sets *is_truncated and *next_token for pagination.
error_code ParseXmlListObjects(string_view xml, bool* is_truncated, string* next_token,
                               absl::FunctionRef<void(const StorageListItem&)> cb) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_buffer(xml.data(), xml.size());
  if (!result) {
    LOG(ERROR) << "aws: failed to parse ListObjects XML: " << result.description();
    return make_error_code(errc::bad_message);
  }

  pugi::xml_node root = doc.child("ListBucketResult");
  if (root.type() != pugi::node_element) {
    LOG(ERROR) << "aws: ListObjects: missing ListBucketResult node\n" << xml;
    return make_error_code(errc::bad_message);
  }

  *is_truncated = string_view(root.child_value("IsTruncated")) == "true";
  *next_token = root.child_value("NextContinuationToken");

  VLOG(2) << "aws: ListObjects: is_truncated=" << *is_truncated << " next_token=" << *next_token;

  for (pugi::xml_node content = root.child("Contents"); content;
       content = content.next_sibling("Contents")) {
    StorageListItem item;
    item.key = content.child_value("Key");
    item.size = content.child("Size").text().as_ullong();

    string_view lmstr = content.child("LastModified").text().as_string();
    if (!lmstr.empty()) {
      absl::Time t;
      string err;
      if (absl::ParseTime("%Y-%m-%dT%H:%M:%E*SZ", lmstr, &t, &err)) {
        item.mtime_ns = absl::ToUnixNanos(t);
      } else {
        VLOG(1) << "aws: failed to parse LastModified '" << lmstr << "': " << err;
      }
    }
    cb(item);
  }

  // Common prefixes (virtual directories when delimiter is set).
  for (pugi::xml_node prefix_node = root.child("CommonPrefixes"); prefix_node;
       prefix_node = prefix_node.next_sibling("CommonPrefixes")) {
    StorageListItem item;
    item.key = prefix_node.child_value("Prefix");
    item.is_prefix = true;
    cb(item);
  }

  return {};
}

class ReadFile final : public io::ReadonlyFile {
 public:
  ReadFile(string bucket, string key, ReadFileOptions opts, unique_ptr<http::ClientPool> pool)
      : bucket_(std::move(bucket)), key_(std::move(key)), opts_(opts), pool_(std::move(pool)) {
  }

  error_code Close() override {
    return {};
  }

  io::SizeOrError Read(size_t offset, const iovec* v, uint32_t len) override;

  size_t Size() const override {
    return size_;
  }

  int Handle() const override {
    return -1;
  }

  error_code InitRead();

 private:
  const string bucket_, key_;
  const ReadFileOptions opts_;

  using Parser = h2::response_parser<h2::buffer_body>;
  std::optional<Parser> parser_;

  unique_ptr<http::ClientPool> pool_;
  http::ClientPool::ClientHandle client_handle_;

  size_t size_ = 0, offs_ = 0;
};

error_code ReadFile::InitRead() {
  string endpoint = opts_.creds_provider->ServiceEndpoint();
  string url = absl::StrCat("/", bucket_, "/", key_);
  detail::EmptyRequestImpl req = FillRequest(endpoint, url, opts_.creds_provider);

  RobustSender sender(pool_.get(), opts_.creds_provider);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(kSendRetries, &req, &send_res));

  auto parser_ptr = std::move(send_res.eb_parser);
  const auto& head_resp = parser_ptr->get();
  auto it = head_resp.find(h2::field::content_length);
  if (it == head_resp.end() || !absl::SimpleAtoi(detail::FromBoostSV(it->value()), &size_)) {
    LOG(ERROR) << "aws: ReadFile: missing content-length for " << bucket_ << "/" << key_;
    return make_error_code(errc::bad_message);
  }

  client_handle_.swap(send_res.client_handle);
  parser_.emplace(std::move(*parser_ptr));
  return {};
}

io::SizeOrError ReadFile::Read(size_t offset, const iovec* v, uint32_t len) {
  DCHECK_EQ(offset, offs_);  // Does not support non-sequential reads.
  size_t total = 0;
  for (uint32_t i = 0; i < len; ++i) {
    auto& body = parser_->get().body();
    body.data = reinterpret_cast<char*>(v[i].iov_base);
    body.size = v[i].iov_len;

    auto boost_ec = client_handle_->Recv(&parser_.value());

    size_t read = v[i].iov_len - body.size;
    total += read;
    offs_ += read;

    if (boost_ec == h2::error::partial_message) {
      LOG(ERROR) << "aws: ReadFile: partial message after " << read << " bytes";
      return nonstd::make_unexpected(make_error_code(errc::connection_aborted));
    }
    if (boost_ec && boost_ec != h2::error::need_buffer) {
      LOG(ERROR) << "aws: ReadFile: recv error: " << boost_ec.message();
      return nonstd::make_unexpected(boost_ec);
    }
    VLOG(1) << "aws: ReadFile: read " << read << "/" << v[i].iov_len << " offs=" << offs_ << "/"
            << size_;
  }
  return total;
}

}  // namespace

S3Storage::S3Storage(AwsCredsProvider* creds, SSL_CTX* ssl_cntx, fb2::ProactorBase* pb)
    : creds_(creds), ssl_cntx_(ssl_cntx) {
  pool_ = CreatePool(creds_->ServiceEndpoint(), ssl_cntx_, pb, creds_->connect_ms());
}

S3Storage::~S3Storage() = default;

error_code S3Storage::ListBuckets(function<void(const BucketItem&)> cb) {
  string endpoint = creds_->ServiceEndpoint();
  detail::EmptyRequestImpl req = FillRequest(endpoint, "/", creds_);

  RobustSender sender(pool_.get(), creds_);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(kSendRetries, &req, &send_res));

  h2::response_parser<h2::string_body> resp(std::move(*send_res.eb_parser));
  RETURN_ERROR(send_res.client_handle->Recv(&resp));

  auto msg = resp.release();
  DVLOG(2) << "aws: ListBuckets response: " << msg.body();

  auto res = ParseXmlListBuckets(msg.body());
  if (!res) {
    return res.error();
  }

  for (const auto& b : *res) {
    cb(b);
  }
  return {};
}

error_code S3Storage::List(string_view bucket, string_view prefix, bool recursive,
                           unsigned max_results, function<void(const ListItem&)> cb) {
  CHECK(!bucket.empty());

  string endpoint = creds_->ServiceEndpoint();

  // Build initial URL: /{bucket}?list-type=2&max-keys=N[&prefix=...][&delimiter=%2F]
  string base_url = absl::StrCat("/", bucket, "?list-type=2&max-keys=", max_results);
  if (!prefix.empty()) {
    absl::StrAppend(&base_url, "&prefix=");
    strings::AppendUrlEncoded(prefix, &base_url);
  }
  if (!recursive) {
    absl::StrAppend(&base_url, "&delimiter=%2F");
  }

  string url = base_url;
  RobustSender sender(pool_.get(), creds_);

  while (true) {
    detail::EmptyRequestImpl req = FillRequest(endpoint, url, creds_);

    RobustSender::SenderResult send_res;
    RETURN_ERROR(sender.Send(kSendRetries, &req, &send_res));

    h2::response_parser<h2::string_body> resp(std::move(*send_res.eb_parser));
    RETURN_ERROR(send_res.client_handle->Recv(&resp));

    auto msg = resp.release();
    DVLOG(3) << "aws: List response: " << msg.body();

    bool is_truncated = false;
    string next_token;
    RETURN_ERROR(ParseXmlListObjects(msg.body(), &is_truncated, &next_token, cb));

    if (!is_truncated || next_token.empty()) {
      break;
    }

    // Build next page URL with continuation token.
    url = base_url;
    absl::StrAppend(&url, "&continuation-token=");
    strings::AppendUrlEncoded(next_token, &url);
  }

  return {};
}

io::Result<io::ReadonlyFile*> OpenReadFile(const std::string& bucket, const std::string& key,
                                           const ReadFileOptions& opts) {
  DCHECK(opts.creds_provider);
  string endpoint = opts.creds_provider->ServiceEndpoint();
  auto pool = CreatePool(endpoint, opts.ssl_cntx, fb2::ProactorBase::me(),
                         opts.creds_provider->connect_ms());
  auto file = std::make_unique<ReadFile>(bucket, key, opts, std::move(pool));
  if (auto ec = file->InitRead(); ec)
    return nonstd::make_unexpected(ec);
  return file.release();
}

}  // namespace cloud::aws
}  // namespace util

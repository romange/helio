// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/aws/s3_storage.h"

#include <absl/strings/ascii.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <boost/asio/buffer.hpp>
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

struct BucketAddressing {
  string endpoint;     // host[:port] used for connect + Host header.
  bool virtual_hosted;  // true => bucket.endpoint, false => endpoint/bucket/...
};

bool SupportsVirtualHostedStyle(string_view endpoint) {
  if (endpoint.empty()) {
    return false;
  }

  string_view host = endpoint;

  // IPv6 literals and multi-colon endpoints should use path-style.
  if (host.front() == '[' || host.find(':') != host.rfind(':')) {
    return false;
  }

  // Strip single :port suffix.
  size_t colon_pos = host.find(':');
  if (colon_pos != string_view::npos) {
    host = host.substr(0, colon_pos);
  }

  string host_lc = absl::AsciiStrToLower(string(host));
  return absl::EndsWith(host_lc, ".amazonaws.com") ||
         absl::EndsWith(host_lc, ".amazonaws.com.cn");
}

BucketAddressing ResolveBucketAddressing(CredentialsProvider* provider, string_view bucket) {
  CHECK(provider);
  string endpoint = provider->ServiceEndpoint();
  if (SupportsVirtualHostedStyle(endpoint)) {
    return {absl::StrCat(bucket, ".", endpoint), true};
  }
  return {std::move(endpoint), false};
}

string BucketObjectPath(const BucketAddressing& addressing, string_view bucket, string_view key) {
  if (addressing.virtual_hosted) {
    return absl::StrCat("/", key);
  }
  return absl::StrCat("/", bucket, "/", key);
}

string BucketQueryPath(const BucketAddressing& addressing, string_view bucket, string_view query) {
  if (addressing.virtual_hosted) {
    return absl::StrCat("/?", query);
  }
  return absl::StrCat("/", bucket, "?", query);
}

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
  BucketAddressing addressing = ResolveBucketAddressing(opts_.creds_provider, bucket_);
  string url = BucketObjectPath(addressing, bucket_, key_);
  detail::EmptyRequestImpl req = FillRequest(addressing.endpoint, url, opts_.creds_provider);

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

constexpr size_t kS3DefaultPartSize = 8UL << 20;  // 8MB

// Parses the UploadId from a CreateMultipartUpload XML response.
io::Result<string> ParseXmlUploadId(string_view xml) {
  pugi::xml_document doc;
  pugi::xml_parse_result res = doc.load_buffer(xml.data(), xml.size());
  if (!res) {
    LOG(ERROR) << "aws: CreateMultipartUpload: failed to parse XML: " << res.description();
    return UnexpectedError(errc::bad_message);
  }
  string upload_id = doc.child("InitiateMultipartUploadResult").child_value("UploadId");
  if (upload_id.empty()) {
    LOG(ERROR) << "aws: CreateMultipartUpload: missing UploadId\n" << xml;
    return UnexpectedError(errc::bad_message);
  }
  return upload_id;
}

class WriteFile : public detail::AbstractStorageFile {
 public:
  WriteFile(string bucket, string key, WriteFileOptions opts, string upload_id,
            unique_ptr<http::ClientPool> pool)
      : detail::AbstractStorageFile(key, kS3DefaultPartSize),
        bucket_(std::move(bucket)),
        upload_id_(std::move(upload_id)),
        opts_(opts),
        pool_(std::move(pool)) {
  }

  error_code Close() override;

 private:
  error_code Upload() override;

  const string bucket_;
  const string upload_id_;
  unsigned part_num_ = 1;
  vector<pair<unsigned, string>> parts_;  // (part_num, etag)
  WriteFileOptions opts_;
  unique_ptr<http::ClientPool> pool_;
};

error_code WriteFile::Upload() {
  size_t body_size = body_mb_.size();
  CHECK_GT(body_size, 0u);

  BucketAddressing addressing = ResolveBucketAddressing(opts_.creds_provider, bucket_);
  string url = BucketObjectPath(addressing, bucket_, create_file_name());
  absl::StrAppend(&url, "?partNumber=", part_num_, "&uploadId=");
  strings::AppendUrlEncoded(upload_id_, &url);

  detail::DynamicBodyRequestImpl req(url, h2::verb::put);
  req.SetBody(std::move(body_mb_));
  req.SetHeader(h2::field::host, addressing.endpoint);
  req.SetHeader("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
  req.Finalize();
  opts_.creds_provider->Sign(&req);

  RobustSender sender(pool_.get(), opts_.creds_provider);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(kSendRetries, &req, &send_res));

  auto it = send_res.eb_parser->get().find(h2::field::etag);
  if (it == send_res.eb_parser->get().end()) {
    LOG(ERROR) << "aws: UploadPart " << part_num_ << ": missing ETag in response";
    return make_error_code(errc::bad_message);
  }
  string etag(detail::FromBoostSV(it->value()));
  VLOG(1) << "aws: UploadPart " << part_num_ << " size=" << body_size << " etag=" << etag;
  parts_.emplace_back(part_num_++, std::move(etag));
  return {};
}

error_code WriteFile::Close() {
  if (body_mb_.size() > 0) {
    RETURN_ERROR(Upload());
  }

  // Build CompleteMultipartUpload XML body.
  string xml = "<CompleteMultipartUpload>";
  for (const auto& [num, etag] : parts_) {
    absl::StrAppend(&xml, "<Part><PartNumber>", num, "</PartNumber><ETag>", etag,
                    "</ETag></Part>");
  }
  xml += "</CompleteMultipartUpload>";

  boost::beast::multi_buffer xml_mb;
  auto buf = xml_mb.prepare(xml.size());
  boost::asio::buffer_copy(buf, boost::asio::buffer(xml));
  xml_mb.commit(xml.size());

  BucketAddressing addressing = ResolveBucketAddressing(opts_.creds_provider, bucket_);
  string url = BucketObjectPath(addressing, bucket_, create_file_name());
  absl::StrAppend(&url, "?uploadId=");
  strings::AppendUrlEncoded(upload_id_, &url);

  detail::DynamicBodyRequestImpl req(url, h2::verb::post);
  req.SetBody(std::move(xml_mb));
  req.SetHeader(h2::field::host, addressing.endpoint);
  req.SetHeader(h2::field::content_type, "application/xml");
  req.SetHeader("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
  req.Finalize();
  opts_.creds_provider->Sign(&req);

  RobustSender sender(pool_.get(), opts_.creds_provider);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(kSendRetries, &req, &send_res));

  h2::response_parser<h2::string_body> resp(std::move(*send_res.eb_parser));
  RETURN_ERROR(send_res.client_handle->Recv(&resp));
  VLOG(1) << "aws: CompleteMultipartUpload: " << resp.get().body();
  return {};
}

}  // namespace

S3Storage::S3Storage(AwsCredsProvider* creds, SSL_CTX* ssl_cntx, fb2::ProactorBase* pb)
    : creds_(creds), ssl_cntx_(ssl_cntx), pb_(pb) {
  pool_ = CreatePool(creds_->ServiceEndpoint(), ssl_cntx_, pb, creds_->connect_ms());
}

S3Storage::~S3Storage() = default;

string S3Storage::BucketEndpoint(string_view bucket) const {
  return ResolveBucketAddressing(creds_, bucket).endpoint;
}

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

  BucketAddressing addressing = ResolveBucketAddressing(creds_, bucket);
  auto pool = CreatePool(addressing.endpoint, ssl_cntx_, pb_, creds_->connect_ms());

  string query = absl::StrCat("list-type=2&max-keys=", max_results);
  if (!prefix.empty()) {
    absl::StrAppend(&query, "&prefix=");
    strings::AppendUrlEncoded(prefix, &query);
  }
  if (!recursive) {
    absl::StrAppend(&query, "&delimiter=%2F");
  }

  string base_url = BucketQueryPath(addressing, bucket, query);

  string url = base_url;
  RobustSender sender(pool.get(), creds_);

  while (true) {
    detail::EmptyRequestImpl req = FillRequest(addressing.endpoint, url, creds_);

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

io::Result<io::WriteFile*> OpenWriteFile(const std::string& bucket, const std::string& key,
                                         const WriteFileOptions& opts) {
  DCHECK(opts.creds_provider);
  BucketAddressing addressing = ResolveBucketAddressing(opts.creds_provider, bucket);
  auto pool = CreatePool(addressing.endpoint, opts.ssl_cntx, fb2::ProactorBase::me(),
                         opts.creds_provider->connect_ms());

  // Initiate multipart upload.
  string url = BucketObjectPath(addressing, bucket, key);
  absl::StrAppend(&url, "?uploads");
  detail::EmptyRequestImpl req(h2::verb::post, url);
  req.SetHeader(h2::field::host, addressing.endpoint);
  req.Finalize();
  opts.creds_provider->Sign(&req);

  RobustSender sender(pool.get(), opts.creds_provider);
  RobustSender::SenderResult send_res;
  if (auto ec = sender.Send(kSendRetries, &req, &send_res); ec)
    return nonstd::make_unexpected(ec);

  h2::response_parser<h2::string_body> resp(std::move(*send_res.eb_parser));
  if (auto ec = send_res.client_handle->Recv(&resp); ec)
    return nonstd::make_unexpected(ec);

  auto upload_id = ParseXmlUploadId(resp.get().body());
  if (!upload_id)
    return nonstd::make_unexpected(upload_id.error());

  VLOG(1) << "aws: CreateMultipartUpload: upload_id=" << *upload_id;
  return new WriteFile(bucket, key, opts, std::move(*upload_id), std::move(pool));
}

io::Result<io::ReadonlyFile*> OpenReadFile(const std::string& bucket, const std::string& key,
                                           const ReadFileOptions& opts) {
  DCHECK(opts.creds_provider);
  BucketAddressing addressing = ResolveBucketAddressing(opts.creds_provider, bucket);
  auto pool = CreatePool(addressing.endpoint, opts.ssl_cntx, fb2::ProactorBase::me(),
                         opts.creds_provider->connect_ms());
  auto file = std::make_unique<ReadFile>(bucket, key, opts, std::move(pool));
  if (auto ec = file->InitRead(); ec)
    return nonstd::make_unexpected(ec);
  return file.release();
}

}  // namespace cloud::aws
}  // namespace util

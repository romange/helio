// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/azure/storage.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/asio/buffer.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <pugixml.hpp>

#include "base/logging.h"
#include "strings/escaping.h"
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
io::Result<vector<StorageListItem>> ParseXmlListBlobs(string_view xml_resp) {
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

  pugi::xml_node blobs = root.child("Blobs");
  if (blobs.type() != pugi::node_element) {
    LOG(ERROR) << "Could not find Blobs node ";
    return UnexpectedError(errc::bad_message);
  }
  VLOG(2) << "ListBlobs Response: " << xml_resp;
  vector<Storage::ListItem> res;
  for (pugi::xml_node blob = blobs.child("Blob"); blob; blob = blob.next_sibling()) {
    Storage::ListItem item;
    item.key = blob.child_value("Name");
    pugi::xml_node prop = blob.child("Properties");
    if (!prop) {
      LOG(ERROR) << "Could not find Properties node ";
      continue;
    }
    item.size = prop.child("Content-Length").text().as_ullong();
    string_view lmstr = prop.child("Last-Modified").text().as_string();
    item.mtime_ns = 0;

    absl::Time time;
    string err;
    const char kRfc1123[] = "%a, %d %b %Y %H:%M:%S";
    lmstr = absl::StripSuffix(lmstr, " GMT");

    if (absl::ParseTime(kRfc1123, lmstr, &time, &err)) {
      item.mtime_ns = absl::ToUnixNanos(time);
    } else {
      LOG(ERROR) << "Failed to parse time: " << lmstr << " " << err;
    }
    res.push_back(item);
  }
  for (pugi::xml_node blob = blobs.child("BlobPrefix"); blob; blob = blob.next_sibling()) {
    Storage::ListItem item;
    item.key = blob.child_value("Name");
    res.push_back(item);
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

class ReadFile final : public io::ReadonlyFile {
 public:
  ReadFile(string read_obj_url, ReadFileOptions opts) : read_obj_url_(read_obj_url), opts_(opts) {
  }

  virtual ~ReadFile();

  error_code Close() {
    return {};
  }

  io::SizeOrError Read(size_t offset, const iovec* v, uint32_t len);

  size_t Size() const {
    return size_;
  }

  int Handle() const {
    return -1;
  };

 private:
  error_code InitRead();

  const string read_obj_url_;
  ReadFileOptions opts_;

  using Parser = h2::response_parser<h2::buffer_body>;
  std::optional<Parser> parser_;

  unique_ptr<http::ClientPool> pool_;  // must be before client_handle_.
  http::ClientPool::ClientHandle client_handle_;

  size_t size_ = 0, offs_ = 0;
};

// File handle that writes to Azure.
//
// This uses multipart uploads, where it will buffer upto the configured part
// size before uploading.
class WriteFile : public detail::AbstractStorageFile {
 public:
  WriteFile(string_view container, string_view key, const WriteFileOptions& opts);
  ~WriteFile();

  // Closes the object and completes the multipart upload. Therefore the object
  // will not be uploaded unless Close is called.
  error_code Close() override;

 private:
  error_code Upload() override;

  using UploadRequest = detail::DynamicBodyRequestImpl;
  using UploadBlockListRequest = detail::DynamicBodyRequestImpl;

  unique_ptr<UploadRequest> PrepareUploadBlockReq();
  unique_ptr<UploadBlockListRequest> PrepareBlockListReq();

  unique_ptr<http::ClientPool> pool_;  // must be before client_handle_.
  string target_;
  unsigned block_id_ = 1;
  WriteFileOptions opts_;
};

ReadFile::~ReadFile() {
}

error_code ReadFile::InitRead() {
  string endpoint = opts_.creds_provider->GetEndpoint();
  pool_ = CreatePool(endpoint, opts_.ssl_cntx, fb2::ProactorBase::me());

  detail::EmptyRequestImpl req = FillRequest(endpoint, read_obj_url_, opts_.creds_provider);
  RobustSender sender(pool_.get(), opts_.creds_provider);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(3, &req, &send_res));

  auto parser_ptr = std::move(send_res.eb_parser);
  const auto& head_resp = parser_ptr->get();
  auto it = head_resp.find(h2::field::content_length);
  if (it == head_resp.end() || !absl::SimpleAtoi(detail::FromBoostSV(it->value()), &size_)) {
    LOG(ERROR) << "Could not find content-length in " << head_resp;
    return make_error_code(errc::connection_refused);
  }

  client_handle_.swap(send_res.client_handle);
  parser_.emplace(std::move(*parser_ptr));

  return {};
}

io::SizeOrError ReadFile::Read(size_t offset, const iovec* v, uint32_t len) {
  // We do a trick. Instead of pulling a file chunk by chunk, we fetch everything at once.
  // But then we pull from a socket iteratively, assuming that we read the data fast enough.
  // the "offset" argument is ignored as this file only reads sequentially.
  if (!client_handle_) {
    error_code ec = InitRead();
    if (ec) {
      return nonstd::make_unexpected(ec);
    }
  }

  size_t total = 0;
  for (uint32_t i = 0; i < len; ++i) {
    auto& body = parser_->get().body();
    body.data = reinterpret_cast<char*>(v[i].iov_base);
    body.size = v[i].iov_len;

    auto boost_ec = client_handle_->Recv(&parser_.value());

    // body.size diminishes after the call.
    size_t read = v[i].iov_len - body.size;
    total += read;
    offs_ += read;
    if (boost_ec == h2::error::partial_message) {
      LOG(ERROR) << "Partial message, " << read << " bytes read, tbd ";
      return nonstd::make_unexpected(make_error_code(errc::connection_aborted));
    }

    if (boost_ec && boost_ec != h2::error::need_buffer) {
      LOG(ERROR) << "Error reading from azure: " << boost_ec.message();
      return nonstd::make_unexpected(boost_ec);
    }
    CHECK(!boost_ec || boost_ec == h2::error::need_buffer);
    VLOG(1) << "Read " << read << "/" << v[i].iov_len << " bytes from " << read_obj_url_;

    if (body.size != 0u) {
      // We have not filled the whole buffer, which means we reached EOF.
      // Verify we read everything.
      if (offs_ != size_) {
        LOG(ERROR) << "Read " << offs_ << " bytes, expected " << size_;
        return nonstd::make_unexpected(make_error_code(errc::message_size));
      }
    }
  }

  return total;
}

WriteFile::WriteFile(string_view container, string_view key, const WriteFileOptions& opts)
    : detail::AbstractStorageFile(key, 1UL << 23), opts_(opts) {
  string endpoint = opts_.creds_provider->GetEndpoint();
  pool_ = CreatePool(endpoint, opts_.ssl_cntx, fb2::ProactorBase::me());
  target_ = absl::StrCat("/", container, "/", key);
}

WriteFile::~WriteFile() {
}

error_code WriteFile::Close() {
  if (body_mb_.size() > 0) {
    RETURN_ERROR(Upload());
  }
  DCHECK_EQ(body_mb_.size(), 0u);
  auto req = PrepareBlockListReq();

  error_code res;
  RobustSender sender(pool_.get(), opts_.creds_provider);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(3, req.get(), &send_res));

  auto parser_ptr = std::move(send_res.eb_parser);
  const auto& resp_msg = parser_ptr->get();
  VLOG(1) << "Close response: " << resp_msg;

  return {};
}

error_code WriteFile::Upload() {
  size_t body_size = body_mb_.size();
  CHECK_GT(body_size, 0u);

  auto req = PrepareUploadBlockReq();

  error_code res;
  RobustSender sender(pool_.get(), opts_.creds_provider);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(3, req.get(), &send_res));

  auto parser_ptr = std::move(send_res.eb_parser);
  const auto& resp_msg = parser_ptr->get();
  VLOG(1) << "Upload response: " << resp_msg;

  return {};
}

auto WriteFile::PrepareUploadBlockReq() -> unique_ptr<UploadRequest> {
  string url =
      absl::StrCat(target_, "?comp=block&blockid=", absl::Dec(block_id_++, absl::kZeroPad4));
  unique_ptr<UploadRequest> upload_req(new UploadRequest(url, h2::verb::put));

  upload_req->SetBody(std::move(body_mb_));

  upload_req->SetHeader(h2::field::host, opts_.creds_provider->GetEndpoint());
  upload_req->Finalize();
  opts_.creds_provider->Sign(upload_req.get());

  return upload_req;
}

auto WriteFile::PrepareBlockListReq() -> unique_ptr<UploadBlockListRequest> {
  string url = absl::StrCat(target_, "?comp=blocklist");
  unique_ptr<UploadBlockListRequest> upload_req(new UploadBlockListRequest(url, h2::verb::put));

  boost::beast::multi_buffer mb;

  string body = R"(<?xml version="1.0" encoding="utf-8"?><BlockList>)";

  for (unsigned i = 1; i < block_id_; ++i) {
    absl::StrAppend(&body, "\n<Uncommitted>", absl::Dec(i, absl::kZeroPad4), "</Uncommitted>");
  }
  absl::StrAppend(&body, "\n</BlockList>\n");

  auto buf_list = mb.prepare(body.size());
  size_t res = boost::asio::buffer_copy(buf_list, boost::asio::buffer(body));
  DCHECK_EQ(res, body.size());
  mb.commit(body.size());

  upload_req->SetBody(std::move(mb));

  upload_req->SetHeader(h2::field::host, opts_.creds_provider->GetEndpoint());
  upload_req->Finalize();
  opts_.creds_provider->Sign(upload_req.get());

  return upload_req;
}

}  // namespace

error_code Storage::ListContainers(function<void(const ContainerItem&)> cb) {
  SSL_CTX* ctx = http::TlsClient::CreateSslContext();
  absl::Cleanup cleanup([ctx] { http::TlsClient::FreeContext(ctx); });

  string endpoint = creds_->GetEndpoint();
  unique_ptr<http::ClientPool> pool = CreatePool(endpoint, ctx, fb2::ProactorBase::me());

  detail::EmptyRequestImpl req = FillRequest(endpoint, "/?comp=list", creds_);
  RobustSender sender(pool.get(), creds_);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(3, &req, &send_res));

  h2::response_parser<h2::string_body> resp(std::move(*send_res.eb_parser));
  RETURN_ERROR(send_res.client_handle->Recv(&resp));

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

error_code Storage::List(string_view container, std::string_view prefix, bool recursive,
                         unsigned max_results, function<void(const ListItem&)> cb) {
  SSL_CTX* ctx = http::TlsClient::CreateSslContext();
  absl::Cleanup cleanup([ctx] { http::TlsClient::FreeContext(ctx); });

  string endpoint = creds_->GetEndpoint();
  unique_ptr<http::ClientPool> pool = CreatePool(endpoint, ctx, fb2::ProactorBase::me());

  string url = absl::StrCat("/", container, "?restype=container&comp=list");
  absl::StrAppend(&url, "&maxresults=", max_results);
  if (!prefix.empty()) {
    absl::StrAppend(&url, "&prefix=");
    strings::AppendUrlEncoded(prefix, &url);
  }
  if (!recursive) {
    absl::StrAppend(&url, "&delimiter=%2f");
  }

  detail::EmptyRequestImpl req = FillRequest(endpoint, url, creds_);
  RobustSender sender(pool.get(), creds_);
  RobustSender::SenderResult send_res;
  RETURN_ERROR(sender.Send(3, &req, &send_res));

  h2::response_parser<h2::string_body> resp(std::move(*send_res.eb_parser));
  RETURN_ERROR(send_res.client_handle->Recv(&resp));

  auto msg = resp.release();
  auto blobs = ParseXmlListBlobs(msg.body());
  if (!blobs)
    return blobs.error();

  for (const auto& b : *blobs) {
    cb(b);
  }

  return {};
}

string BuildGetObjUrl(const string& container, const string& key) {
  string url = absl::StrCat("/", container, "/", key);
  return url;
}

io::Result<io::ReadonlyFile*> OpenReadFile(const std::string& container, const std::string& key,
                                           const ReadFileOptions& opts) {
  DCHECK(opts.creds_provider && opts.ssl_cntx);
  string url = BuildGetObjUrl(container, key);
  return new ReadFile(url, opts);
}

io::Result<io::WriteFile*> OpenWriteFile(const std::string& container, const std::string& key,
                                         const WriteFileOptions& opts) {
  DCHECK(opts.creds_provider && opts.ssl_cntx);

  return new WriteFile(container, key, opts);
}

}  // namespace cloud::azure
}  // namespace util
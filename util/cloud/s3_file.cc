// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/cloud/s3_file.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <libxml/xpath.h>

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/dynamic_body.hpp>

#include "base/logging.h"
#include "util/http/http_common.h"

using namespace std;

namespace h2 = boost::beast::http;

namespace util {
namespace cloud {

namespace {

// AWS requires at least 5MB part size. We use 8MB.
constexpr size_t kMaxPartSize = 1ULL << 23;

inline void SetRange(size_t from, size_t to, h2::fields* flds) {
  string tmp = absl::StrCat("bytes=", from, "-");
  if (to < kuint64max) {
    absl::StrAppend(&tmp, to - 1);
  }
  flds->set(h2::field::range, std::move(tmp));
}

inline string_view ToSv(const boost::string_view s) {
  return string_view{s.data(), s.size()};
}

std::ostream& operator<<(std::ostream& os, const h2::response<h2::buffer_body>& msg) {
  os << msg.reason() << std::endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << std::endl;
  }
  os << "-------------------------";

  return os;
}

error_code DrainResponse(http::Client* client, h2::response_parser<h2::buffer_body>* parser) {
  char resp[512];
  auto& body = parser->get().body();
  while (!parser->is_done()) {
    body.data = resp;
    body.size = sizeof(resp);

    http::Client::BoostError ec = client->Recv(parser);
    if (ec && ec != h2::error::need_buffer) {
      return ec;
    }
  }
  return error_code{};
}

inline xmlDocPtr XmlRead(string_view xml) {
  return xmlReadMemory(xml.data(), xml.size(), NULL, NULL, XML_PARSE_COMPACT | XML_PARSE_NOBLANKS);
}

inline const char* as_char(const xmlChar* var) {
  return reinterpret_cast<const char*>(var);
}

error_code ParseXmlStartUpload(std::string_view xml_resp, string* upload_id) {
  xmlDocPtr doc = XmlRead(xml_resp);
  if (!doc)
    return make_error_code(errc::bad_message);

  absl::Cleanup xml_free{[doc]() { xmlFreeDoc(doc); }};

  xmlNodePtr root = xmlDocGetRootElement(doc);

  string_view rname = as_char(root->name);
  if (rname != "InitiateMultipartUploadResult"sv) {
    return make_error_code(errc::bad_message);
  }

  for (xmlNodePtr child = root->children; child; child = child->next) {
    if (child->type == XML_ELEMENT_NODE) {
      xmlNodePtr grand = child->children;
      if (!strcmp(as_char(child->name), "UploadId")) {
        if (!grand || grand->type != XML_TEXT_NODE)
          return make_error_code(errc::bad_message);
        upload_id->assign(as_char(grand->content));
        break;
      }
    }
  }

  return error_code{};
}

class S3ReadFile : public io::ReadonlyFile {
 public:
  // does not own pool object, only wraps it with ReadonlyFile interface.
  S3ReadFile(const AWS& aws, string read_obj_url, http::Client* client)
      : aws_(aws), client_(client), read_obj_url_(std::move(read_obj_url)) {
  }

  virtual ~S3ReadFile() final;

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  io::Result<size_t> Read(size_t offset, const iovec* v, uint32_t len) final;

  std::error_code Open(std::string_view region);

  // releases the system handle for this file.
  std::error_code Close() final;

  size_t Size() const final {
    return size_;
  }

  int Handle() const final {
    return -1;
  }

 private:
  AWS::HttpParser* parser() {
    return &parser_;
  }

  const AWS& aws_;
  http::Client* client_;

  const string read_obj_url_;

  AWS::HttpParser parser_;
  size_t size_ = 0, offs_ = 0;
  AwsSignKey sign_key_;
};

class S3WriteFile : public io::WriteFile {
 public:
  /**
   * @brief Construct a new S3 Write File object.
   *
   * @param name - aka "s3://somebucket/path_to_obj"
   * @param aws - initialized AWS object.
   * @param pool - https connection pool connected to google api server.
   */
  S3WriteFile(std::string_view name, string upload_id, AwsSignKey skey, const AWS& aws,
              http::Client* client);

  error_code Close() final;

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

 private:
  size_t FillBody(const uint8* buffer, size_t length);
  error_code Upload();

  AwsSignKey skey_;
  const AWS& aws_;

  string upload_id_;
  size_t uploaded_ = 0;
  unique_ptr<http::Client> client_;
  boost::beast::multi_buffer body_mb_;
  std::vector<string> parts_;
};

S3ReadFile::~S3ReadFile() {
  Close();
}

error_code S3ReadFile::Open(string_view region) {
  string url = absl::StrCat("/", read_obj_url_);
  h2::request<h2::empty_body> req{h2::verb::get, url, 11};
  req.set(h2::field::host, client_->host());

  if (offs_)
    SetRange(offs_, kuint64max, &req);

  VLOG(1) << "Unsigned request: " << req;
  sign_key_ = aws_.GetSignKey(region);
  error_code ec = aws_.Handshake(client_, &sign_key_, &req, &parser_);
  if (ec) {
    return ec;
  }

  const auto& msg = parser_.get();
  VLOG(1) << "HeaderResp: " << msg.result_int() << " " << msg;

  if (msg.result() == h2::status::not_found) {
    ec = DrainResponse(client_, &parser_);
    if (ec)
      return ec;
    return make_error_code(errc::no_such_file_or_directory);
  }

  if (msg.result() == h2::status::bad_request) {
    return make_error_code(errc::bad_message);
  }

  CHECK(parser_.keep_alive()) << "TBD";

  auto content_len_it = msg.find(h2::field::content_length);
  if (content_len_it != msg.end()) {
    size_t content_sz = 0;
    CHECK(absl::SimpleAtoi(ToSv(content_len_it->value()), &content_sz));

    if (size_) {
      CHECK_EQ(size_, content_sz + offs_) << "File size has changed underneath during reopen";
    } else {
      size_ = content_sz;
    }
  }

  return ec;
}

error_code S3ReadFile::Close() {
  return error_code{};
}

io::Result<size_t> S3ReadFile::Read(size_t offset, const iovec* v, uint32_t len) {
  if (offset != offs_) {
    return nonstd::make_unexpected(make_error_code(errc::invalid_argument));
  }

  // We can not cache parser() into local var because Open() below recreates the parser instance.
  if (parser_.is_done()) {
    return 0;
  }

  size_t index = 0;
  size_t read_sofar = 0;

  while (index < len) {
    // We keep body references inside the loop because Open() that might be called here,
    // will recreate the parser from the point the connections disconnected.
    auto& body = parser()->get().body();
    auto& left_available = body.size;
    body.data = v[index].iov_base;
    left_available = v[index].iov_len;

    boost::system::error_code ec = client_->Recv(parser());  // decreases left_available.
    size_t http_read = v[index].iov_len - left_available;

    if (!ec || ec == h2::error::need_buffer) {  // Success
      DVLOG(2) << "Read " << http_read << " bytes from " << offset << " with capacity "
               << v[index].iov_len << "ec: " << ec;

      CHECK(left_available == 0 || !ec);

      // This check does not happen. See here why: https://github.com/boostorg/beast/issues/1662
      // DCHECK_EQ(sz_read, http_read) << " " << range.size() << "/" << left_available;
      offs_ += http_read;
      read_sofar += http_read;
      ++index;

      continue;
    }

    if (ec == h2::error::partial_message) {
      offs_ += http_read;
      VLOG(1) << "Got partial_message";

      // advance the destination buffer as well.
      read_sofar += http_read;
      break;
    }

    LOG(ERROR) << "ec: " << ec << "/" << ec.message() << " at " << offset << "/" << size_;
    return nonstd::make_unexpected(ec);
  }

  return read_sofar;
}

S3WriteFile::S3WriteFile(string_view name, string upload_id, AwsSignKey skey, const AWS& aws,
                         http::Client* client)
    : WriteFile(name), skey_(std::move(skey)), aws_(aws), upload_id_(move(upload_id)),
      client_(client), body_mb_(kMaxPartSize) {
}

error_code S3WriteFile::Close() {
  error_code ec = Upload();
  if (ec) {
    return ec;
  }

  if (parts_.empty())
    return ec;

  string url("/");
  url.append(create_file_name_);

  // Signed params must look like key/value pairs. Instead of handling key-only params
  // in the signing code we just pass empty value here.
  absl::StrAppend(&url, "?uploadId=", upload_id_);

  h2::request<h2::string_body> req{h2::verb::post, url, 11};
  h2::response<h2::string_body> resp;

  req.set(h2::field::content_type, http::kXmlMime);
  req.set(h2::field::host, client_->host());

  auto& body = req.body();
  body = R"(<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">)";
  body.append("\n");

  for (size_t i = 0; i < parts_.size(); ++i) {
    absl::StrAppend(&body, "<Part><ETag>\"", parts_[i], "\"</ETag><PartNumber>", i + 1);
    absl::StrAppend(&body, "</PartNumber></Part>\n");
  }
  body.append("</CompleteMultipartUpload>");

  req.prepare_payload();

  skey_.Sign(string_view{AWS::kUnsignedPayloadSig}, &req);

  ec = client_->Send(req, &resp);

  if (ec) {
    return ec;
  }

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "S3WriteFile::Close: " << req << "/ " << resp;

    return make_error_code(errc::io_error);
  }
  parts_.clear();

  return ec;
}

io::Result<size_t> S3WriteFile::WriteSome(const iovec* v, uint32_t len) {
  size_t total = 0;
  for (size_t i = 0; i < len; ++i) {
    size_t len = v[i].iov_len;
    const uint8_t* buffer = reinterpret_cast<const uint8_t*>(v[i].iov_base);

    while (len) {
      size_t written = FillBody(buffer, len);
      total += written;
      len -= written;
      buffer += written;
      if (body_mb_.size() == body_mb_.max_size()) {
        error_code ec = Upload();
        if (ec)
          return nonstd::make_unexpected(ec);
      }
    }
  }

  return total;
}

size_t S3WriteFile::FillBody(const uint8* buffer, size_t length) {
  size_t prepare_size = std::min(length, body_mb_.max_size() - body_mb_.size());
  auto mbs = body_mb_.prepare(prepare_size);
  size_t offs = 0;
  for (auto mb : mbs) {
    memcpy(mb.data(), buffer + offs, mb.size());
    offs += mb.size();
  }
  CHECK_EQ(offs, prepare_size);
  body_mb_.commit(prepare_size);

  return offs;
}

error_code S3WriteFile::Upload() {
  size_t body_size = body_mb_.size();
  if (body_size == 0)
    return error_code{};

  string url("/");

  // TODO: To figure out why SHA256 is so slow.
  // detail::Sha256String(body_mb_, sha256);
  absl::StrAppend(&url, create_file_name_);

  absl::StrAppend(&url, "?uploadId=", upload_id_);
  absl::StrAppend(&url, "&partNumber=", parts_.size() + 1);

  h2::request<h2::dynamic_body> req{h2::verb::put, url, 11};
  h2::response<h2::string_body> resp;

  req.set(h2::field::content_type, http::kBinMime);
  req.set(h2::field::host, client_->host());
  req.body() = std::move(body_mb_);
  req.prepare_payload();

  skey_.Sign(string_view{AWS::kUnsignedPayloadSig}, &req);
  VLOG(1) << "UploadReq: " << req.base();
  error_code ec = client_->Send(req, &resp);
  if (ec)
    return ec;

  VLOG(1) << "UploadResp: " << resp;

  auto it = resp.find(h2::field::etag);
  if (it != resp.end())
    return make_error_code(errc::io_error);

  VLOG(1) << "Etag: " << it->value();
  auto sv = it->value();
  if (sv.size() <= 2) {
    return make_error_code(errc::io_error);
  }
  sv.remove_prefix(1);
  sv.remove_suffix(1);
  parts_.emplace_back(sv.to_string());

  if (resp[h2::field::connection] == "close") {
    ec = client_->Reconnect();
    if (ec)
      return ec;
  }
  return ec;
}

}  // namespace

io::Result<io::ReadonlyFile*> OpenS3ReadFile(std::string_view region, string_view path,
                                             const AWS& aws, http::Client* client,
                                             const io::ReadonlyFile::Options& opts) {
  CHECK(opts.sequential && client);
  VLOG(1) << "OpenS3ReadFile: " << path;

  string_view bucket, obj_path;

  string read_obj_url{path};
  unique_ptr<S3ReadFile> fl(new S3ReadFile(aws, std::move(read_obj_url), client));

  auto ec = fl->Open(region);
  if (ec)
    return nonstd::make_unexpected(ec);

  return fl.release();
}

io::Result<io::WriteFile*> OpenS3WriteFile(string_view region, string_view key_path, const AWS& aws,
                                           http::Client* client) {
  string url("/");
  url.append(key_path).append("?uploads=");

  // Signed params must look like key/value pairs. Instead of handling key-only params
  // in the signing code we just pass empty value here.

  h2::request<h2::empty_body> req{h2::verb::post, url, 11};
  h2::response<h2::string_body> resp;

  req.set(h2::field::host, client->host());
  auto sign_key = aws.GetSignKey(region);
  unique_ptr<http::Client> bucket_client{client};

  auto ec = aws.SendRequest(client, &sign_key, &req, &resp);
  if (ec)
    return nonstd::make_unexpected(ec);

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "OpenS3WriteFile Error: " << resp;

    return nonstd::make_unexpected(make_error_code(errc::io_error));
  }

  string upload_id;
  ec = ParseXmlStartUpload(resp.body(), &upload_id);
  if (ec)
    return nonstd::make_unexpected(ec);

  VLOG(1) << "OpenS3WriteFile: " << req << "/" << resp << "\nUploadId: " << upload_id;

  return new S3WriteFile{key_path, move(upload_id), move(sign_key), aws, bucket_client.release()};
}

}  // namespace cloud
}  // namespace util
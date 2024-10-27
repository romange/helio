// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcs_file.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>  // for operator<<

#include "base/flags.h"
#include "base/logging.h"
#include "strings/escaping.h"
#include "util/cloud/gcp/gcp_utils.h"
#include "util/http/http_client.h"
#include "util/http/http_common.h"

ABSL_FLAG(bool, gcs_dry_upload, false, "");

namespace util {

namespace cloud {
using namespace std;
namespace h2 = boost::beast::http;
using boost::beast::multi_buffer;

namespace {

//! [from, to) limited range out of total. If total is < 0 then it's unknown.
string ContentRangeHeader(size_t from, size_t to, ssize_t total) {
  DCHECK_LE(from, to);
  string tmp{"bytes "};

  if (from < to) {                                  // common case.
    absl::StrAppend(&tmp, from, "-", to - 1, "/");  // content-range is inclusive.
    if (total >= 0) {
      absl::StrAppend(&tmp, total);
    } else {
      tmp.push_back('*');
    }
  } else {
    // We can write empty ranges only when we finalize the file and total is known.
    DCHECK_GE(total, 0);
    absl::StrAppend(&tmp, "*/", total);
  }

  return tmp;
}

string BuildGetObjUrl(absl::string_view bucket, absl::string_view obj_path) {
  string read_obj_url{"/storage/v1/b/"};
  absl::StrAppend(&read_obj_url, bucket, "/o/");
  strings::AppendUrlEncoded(obj_path, &read_obj_url);
  absl::StrAppend(&read_obj_url, "?alt=media");

  return read_obj_url;
}

inline void SetRange(size_t from, size_t to, h2::fields* flds) {
  string tmp = absl::StrCat("bytes=", from, "-");
  if (to < kuint64max) {
    absl::StrAppend(&tmp, to - 1);
  }
  flds->set(h2::field::range, std::move(tmp));
}

// File handle that writes to GCS.
//
// This uses multipart uploads, where it will buffer upto the configured part
// size before uploading.
class GcsWriteFile : public io::WriteFile {
 public:
  // Writes bytes to the GCS object. This will either buffer internally or
  // write a part to GCS.
  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override;

  // Closes the object and completes the multipart upload. Therefore the object
  // will not be uploaded unless Close is called.
  error_code Close() override;

  GcsWriteFile(const string_view key, string_view upload_id, const GcsWriteFileOptions& opts);
  ~GcsWriteFile();

 private:
  error_code FillBuf(const uint8* buffer, size_t length);
  error_code Upload();

  using UploadRequest = detail::DynamicBodyRequestImpl;
  unique_ptr<UploadRequest> PrepareRequest(size_t to, ssize_t total);

  string upload_id_;
  multi_buffer body_mb_;
  size_t uploaded_ = 0;
  GcsWriteFileOptions opts_;
};

class GcsReadFile : public io::ReadonlyFile {
 public:
  // does not own gcs object, only wraps it with ReadonlyFile interface.
  GcsReadFile(string read_obj_url, const GcsReadFileOptions& opts)
      : read_obj_url_(read_obj_url), opts_(opts) {
  }

  virtual ~GcsReadFile() final;

  error_code Close() final {
    return {};
  }

  io::SizeOrError Read(size_t offset, const iovec* v, uint32_t len) final;

  size_t Size() const final {
    return size_;
  }

  int Handle() const final {
    return -1;
  };

 private:
  const string read_obj_url_;

  using Parser = h2::response_parser<h2::buffer_body>;
  std::optional<Parser> parser_;

  http::ClientPool::ClientHandle client_handle_;
  const GcsReadFileOptions opts_;

  size_t size_ = 0, offs_ = 0;
};

/********************************************************************************************
                                Implementation
******************************************************************************************/

GcsWriteFile::GcsWriteFile(string_view key, string_view upload_id, const GcsWriteFileOptions& opts)
    : io::WriteFile(key), upload_id_(upload_id), body_mb_(opts.part_size), opts_(opts) {
}

GcsWriteFile::~GcsWriteFile() {
  if (opts_.pool_owned) {
    delete opts_.pool;
  }
}

io::Result<size_t> GcsWriteFile::WriteSome(const iovec* v, uint32_t len) {
  size_t total = 0;
  for (uint32_t i = 0; i < len; ++i) {
    RETURN_UNEXPECTED(FillBuf(reinterpret_cast<const uint8_t*>(v->iov_base), v->iov_len));
    total += v->iov_len;
  }
  return total;
}

error_code GcsWriteFile::Close() {
  size_t to = uploaded_ + body_mb_.size();
  auto req = PrepareRequest(to, to);

  string body;
  if (!absl::GetFlag(FLAGS_gcs_dry_upload)) {
    RobustSender sender(opts_.pool, opts_.creds_provider);

    RobustSender::SenderResult send_res;
    error_code ec = sender.Send(3, req.get(), &send_res);
    if (ec) {
      LOG(ERROR) << "Error closing GCS file " << create_file_name() << " for request: \n"
                 << req->GetHeaders() << ", status " << ec.message();
      return ec;
    }
    h2::response_parser<h2::string_body> resp(std::move(*send_res.eb_parser));
    auto client_handle = std::move(send_res.client_handle);
    RETURN_ERROR(client_handle->Recv(&resp));

    body = std::move(resp.get().body());

    /*
      body is in a json reponse with all the metadata of the object
      {
          "kind": "storage#object",
          "id": "mybucket/roman/bar_0/1720889888465538",
          "selfLink": "https://www.googleapis.com/storage/v1/b/mybucket/o/roman%2Fbar_0",
          "mediaLink":
        "https://storage.googleapis.com/download/storage/v1/b/mybucket/o/roman%2Fbar_0?generation=1720889888465538&alt=media",
          "name": "roman/bar_0",
          "bucket": "mybucket",
          "generation": "1720889888465538",
          "metageneration": "1",
          "storageClass": "STANDARD",
          "size": "27270144",
          "md5Hash": "O/P7e3k8qxRQaomHhn0H9Q==",
          "crc32c": "8s2ltw==",
          "etag": "CILF/7O+pIcDEAE=",
          "timeCreated": "2024-07-13T16:58:08.476Z",
          "updated": "2024-07-13T16:58:08.476Z",
          "timeStorageClassUpdated": "2024-07-13T16:58:08.476Z"
      }

    */
  }

  VLOG(1) << "Closed file " << req->GetHeaders() << "\n" << body;

  return {};
}

error_code GcsWriteFile::FillBuf(const uint8* buffer, size_t length) {
  while (length >= body_mb_.max_size() - body_mb_.size()) {
    size_t prepare_size = body_mb_.max_size() - body_mb_.size();
    auto mbs = body_mb_.prepare(prepare_size);
    size_t offs = 0;
    for (auto mb : mbs) {
      memcpy(mb.data(), buffer + offs, mb.size());
      offs += mb.size();
    }
    DCHECK_EQ(offs, prepare_size);
    body_mb_.commit(prepare_size);

    auto ec = Upload();
    if (ec)
      return ec;

    length -= prepare_size;
    buffer += prepare_size;
  }

  if (length) {
    auto mbs = body_mb_.prepare(length);
    for (auto mb : mbs) {
      memcpy(mb.data(), buffer, mb.size());
      buffer += mb.size();
    }
    body_mb_.commit(length);
  }
  return {};
}

error_code GcsWriteFile::Upload() {
  size_t body_size = body_mb_.size();
  CHECK_GT(body_size, 0u);
  CHECK_EQ(0u, body_size % (1U << 18)) << body_size;  // Must be multiple of 256KB.

  size_t to = uploaded_ + body_size;

  auto req = PrepareRequest(to, -1);

  error_code res;
  if (!absl::GetFlag(FLAGS_gcs_dry_upload)) {
    RobustSender sender(opts_.pool, opts_.creds_provider);
    RobustSender::SenderResult send_res;
    RETURN_ERROR(sender.Send(3, req.get(), &send_res));

    VLOG(1) << "Uploaded range " << uploaded_ << "/" << to << " for " << upload_id_;
    auto parser_ptr = std::move(send_res.eb_parser);
    const auto& resp_msg = parser_ptr->get();
    auto it = resp_msg.find(h2::field::range);
    CHECK(it != resp_msg.end()) << resp_msg;

    string_view range = detail::FromBoostSV(it->value());
    CHECK(absl::ConsumePrefix(&range, "bytes="));
    size_t pos = range.find('-');
    CHECK_LT(pos, range.size());
    size_t uploaded_pos = 0;
    CHECK(absl::SimpleAtoi(range.substr(pos + 1), &uploaded_pos));
    CHECK_EQ(uploaded_pos + 1, to);
  }

  uploaded_ = to;
  return {};
}

auto GcsWriteFile::PrepareRequest(size_t to, ssize_t total) -> unique_ptr<UploadRequest> {
  unique_ptr<UploadRequest> upload_req(new UploadRequest(upload_id_));

  upload_req->SetBody(std::move(body_mb_));
  upload_req->SetHeader(h2::field::content_range, ContentRangeHeader(uploaded_, to, total));
  upload_req->SetHeader(h2::field::content_type, http::kBinMime);
  upload_req->Finalize();

  return upload_req;
}

GcsReadFile::~GcsReadFile() {
  // Important to release it before we delete the pool.
  client_handle_.reset();

  if (opts_.pool_owned) {
    delete opts_.pool;
  }
}

io::SizeOrError GcsReadFile::Read(size_t offset, const iovec* v, uint32_t len) {
  // We do a trick. Instead of pulling a file chunk by chunk, we fetch everything at once.
  // But then we pull from a socket iteratively, assuming that we read the data fast enough.
  // the "offset" argument is ignored as this file only reads sequentially.
  if (!client_handle_) {
    string token = opts_.creds_provider->access_token();
    detail::EmptyRequestImpl empty_req(h2::verb::get, read_obj_url_, token);

    RobustSender sender(opts_.pool, opts_.creds_provider);
    RobustSender::SenderResult send_res;
    RETURN_UNEXPECTED(sender.Send(3, &empty_req, &send_res));

    client_handle_ = std::move(send_res.client_handle);
    parser_.emplace(std::move(*send_res.eb_parser));
    const auto& headers = parser_->get();
    auto it = headers.find(h2::field::content_length);
    if (it == headers.end() || !absl::SimpleAtoi(detail::FromBoostSV(it->value()), &size_)) {
      LOG(ERROR) << "Could not find content-length in " << headers.base();
      return nonstd::make_unexpected(make_error_code(errc::connection_refused));
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
      LOG(ERROR) << "Error reading from GCS: " << boost_ec.message();
      return nonstd::make_unexpected(boost_ec);
    }
    CHECK(!boost_ec || boost_ec == h2::error::need_buffer);
    VLOG(1) << "Read " << read << "/" << v[i].iov_len << " bytes from " << read_obj_url_;
    // We either read everything that can fit the buffers or we reached EOF.
    CHECK(body.size == 0u || offs_ == size_);
  }

  return total;
}

}  // namespace

io::Result<io::WriteFile*> OpenWriteGcsFile(const string& bucket, const string& key,
                                            const GcsWriteFileOptions& opts) {
  CHECK(opts.creds_provider);
  CHECK(opts.pool);

  string url = "/upload/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?uploadType=resumable&name=");
  strings::AppendUrlEncoded(key, &url);
  string token = opts.creds_provider->access_token();
  detail::EmptyRequestImpl empty_req(h2::verb::post, url, token);
  empty_req.Finalize();  // it's post request so it's required.

  RobustSender sender(opts.pool, opts.creds_provider);
  RobustSender::SenderResult send_res;
  error_code ec = sender.Send(3, &empty_req, &send_res);
  if (ec) {
    if (opts.pool_owned) {
      delete opts.pool;
    }
    return nonstd::make_unexpected(ec);
  }

  auto parser_ptr = std::move(send_res.eb_parser);
  const auto& headers = parser_ptr->get();
  auto it = headers.find(h2::field::location);
  if (it == headers.end()) {
    LOG(ERROR) << "Could not find location in " << headers;
    if (opts.pool_owned) {
      delete opts.pool;
    }
    return nonstd::make_unexpected(make_error_code(errc::connection_refused));
  }

  return new GcsWriteFile(key, detail::FromBoostSV(it->value()), opts);
}

io::Result<io::ReadonlyFile*> OpenReadGcsFile(const std::string& bucket, const std::string& key,
                                              const GcsReadFileOptions& opts) {
  string url = BuildGetObjUrl(bucket, key);
  return new GcsReadFile(url, opts);
}

}  // namespace cloud
}  // namespace util

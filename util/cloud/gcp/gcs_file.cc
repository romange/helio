// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcs_file.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "base/flags.h"
#include "base/logging.h"
#include "strings/escaping.h"
#include "util/cloud/gcp/gcp_utils.h"
#include "util/http/http_common.h"

ABSL_FLAG(bool, gcs_dry_upload, false, "");

namespace util {

namespace cloud {
using namespace std;
namespace h2 = boost::beast::http;
using boost::beast::multi_buffer;
using HeaderParserPtr = RobustSender::HeaderParserPtr;

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

  GcsWriteFile(const string_view key, string_view upload_id, size_t part_size,
               http::ClientPool* pool, GCPCredsProvider* creds_provider);

 private:
  error_code FillBuf(const uint8* buffer, size_t length);
  error_code Upload();

  using UploadRequest = detail::DynamicBodyRequestImpl;
  unique_ptr<UploadRequest> PrepareRequest(size_t to, ssize_t total);

  string upload_id_;
  multi_buffer body_mb_;
  size_t uploaded_ = 0;
  http::ClientPool* pool_;
  GCPCredsProvider* creds_provider_;
};

GcsWriteFile::GcsWriteFile(string_view key, string_view upload_id, size_t part_size,
                           http::ClientPool* pool, GCPCredsProvider* creds_provider)
    : io::WriteFile(key), upload_id_(upload_id), body_mb_(part_size), pool_(pool),
      creds_provider_(creds_provider) {
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
    RobustSender sender(3, creds_provider_);
    auto client_handle = pool_->GetHandle();
    io::Result<HeaderParserPtr> res = sender.Send(client_handle.get(), req.get());
    if (!res) {
      LOG(ERROR) << "Error closing GCS file " << create_file_name() << " for request: \n"
                 << req->GetHeaders() << ", status " << res.error().message();
      return res.error();
    }
    HeaderParserPtr head_parser = std::move(*res);
    h2::response_parser<h2::string_body> resp(std::move(*head_parser));
    auto ec = client_handle->Recv(&resp);
    if (ec)
      return ec;
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
    // TODO: RobustSender must access the entire pool, not just a single client.
    RobustSender sender(3, creds_provider_);
    auto client_handle = pool_->GetHandle();
    io::Result<HeaderParserPtr> res = sender.Send(client_handle.get(), req.get());
    if (!res)
      return res.error();

    VLOG(1) << "Uploaded range " << uploaded_ << "/" << to << " for " << upload_id_;
    HeaderParserPtr parser_ptr = std::move(*res);
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

}  // namespace

io::Result<io::WriteFile*> OpenWriteGcsFile(const string& bucket, const string& key,
                                            GCPCredsProvider* creds_provider,
                                            http::ClientPool* pool, size_t part_size) {
  string url = "/upload/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?uploadType=resumable&name=");
  strings::AppendUrlEncoded(key, &url);
  string token = creds_provider->access_token();
  detail::EmptyRequestImpl empty_req(h2::verb::post, url, token);
  empty_req.Finalize();  // it's post request so it's required.

  RobustSender sender(3, creds_provider);
  auto client_handle = pool->GetHandle();
  io::Result<HeaderParserPtr> res = sender.Send(client_handle.get(), &empty_req);
  if (!res) {
    return nonstd::make_unexpected(res.error());
  }

  HeaderParserPtr parser_ptr = std::move(*res);
  const auto& headers = parser_ptr->get();
  auto it = headers.find(h2::field::location);
  if (it == headers.end()) {
    LOG(ERROR) << "Could not find location in " << headers;
    return nonstd::make_unexpected(make_error_code(errc::connection_refused));
  }

  return new GcsWriteFile(key, detail::FromBoostSV(it->value()), part_size, pool, creds_provider);
}

}  // namespace cloud
}  // namespace util

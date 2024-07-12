// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcs_file.h"

#include <absl/strings/str_cat.h>

#include <boost/beast/http/empty_body.hpp>

#include "strings/escaping.h"
#include "util/cloud/gcp/gcp_utils.h"

namespace util {

namespace cloud {
using namespace std;
namespace h2 = boost::beast::http;

namespace {


}  // namespace

io::Result<GcsWriteFile*> GcsWriteFile::Open(const string& bucket, const string& key,
                                             GCPCredsProvider* creds_provider,
                                             http::ClientPool* pool, size_t part_size) {
  string url = "/upload/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?uploadType=resumable&name=");
  strings::AppendUrlEncoded(key, &url);
  string token = creds_provider->access_token();
  auto req = PrepareRequest(h2::verb::post, url, token);
  string upload_id;
#if 0
  ApiSenderDynamicBody sender("start_write", gce, pool);
  auto res = sender.SendGeneric(3, std::move(req));
  if (!res.ok())
    return res.status;

  const auto& resp = sender.parser()->get();

  // HttpsClientPool::ClientHandle handle = std::move(res.obj);

  auto it = resp.find(h2::field::location);
  if (it == resp.end()) {
    return Status(StatusCode::PARSE_ERROR, "Can not find location header");
  }
  string upload_id = string(it->value());


#endif

  return new GcsWriteFile(key, upload_id, part_size, pool);
}

}  // namespace cloud
}  // namespace util

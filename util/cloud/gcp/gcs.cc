// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcs.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <rapidjson/document.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "base/flags.h"
#include "base/logging.h"
#include "io/file.h"
#include "io/file_util.h"
#include "io/line_reader.h"
#include "strings/escaping.h"
#include "util/cloud/gcp/gcp_utils.h"

using namespace std;
namespace h2 = boost::beast::http;
namespace rj = rapidjson;

ABSL_FLAG(string, gcs_auth_token, "", "");

namespace util {
namespace cloud {

namespace {

auto Unexpected(std::errc code) {
  return nonstd::make_unexpected(make_error_code(code));
}

const char kInstanceTokenUrl[] = "/computeMetadata/v1/instance/service-accounts/default/token";


io::Result<string> ExpandFilePath(string_view path) {
  io::Result<io::StatShortVec> res = io::StatFiles(path);

  if (!res) {
    return nonstd::make_unexpected(res.error());
  }

  if (res->empty()) {
    VLOG(1) << "Could not find " << path;
    return Unexpected(errc::no_such_file_or_directory);
  }
  return res->front().name;
}

std::error_code LoadGCPConfig(string* account_id, string* project_id) {
  io::Result<string> path = ExpandFilePath("~/.config/gcloud/configurations/config_default");
  if (!path) {
    return path.error();
  }

  io::Result<string> config = io::ReadFileToString(*path);
  if (!config) {
    return config.error();
  }

  io::BytesSource bs(*config);
  io::LineReader reader(&bs, DO_NOT_TAKE_OWNERSHIP, 11);
  string scratch;
  string_view line;
  while (reader.Next(&line, &scratch)) {
    vector<string_view> vals = absl::StrSplit(line, "=");
    if (vals.size() != 2)
      continue;
    for (auto& v : vals) {
      v = absl::StripAsciiWhitespace(v);
    }
    if (vals[0] == "account") {
      *account_id = string(vals[1]);
    } else if (vals[0] == "project") {
      *project_id = string(vals[1]);
    }
  }

  return {};
}

std::error_code ParseADC(string_view adc_file, string* client_id, string* client_secret,
                         string* refresh_token) {
  io::Result<string> adc = io::ReadFileToString(adc_file);
  if (!adc) {
    return adc.error();
  }

  rj::Document adc_doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  adc_doc.ParseInsitu<kFlags>(&adc->front());

  if (adc_doc.HasParseError()) {
    return make_error_code(errc::protocol_error);
  }

  for (auto it = adc_doc.MemberBegin(); it != adc_doc.MemberEnd(); ++it) {
    if (it->name == "client_id") {
      *client_id = it->value.GetString();
    } else if (it->name == "client_secret") {
      *client_secret = it->value.GetString();
    } else if (it->name == "refresh_token") {
      *refresh_token = it->value.GetString();
    }
  }

  return {};
}

// token, expire_in (seconds)
using TokenTtl = pair<string, unsigned>;

io::Result<TokenTtl> ParseTokenResponse(std::string&& response) {
  VLOG(2) << "Refresh Token response: " << response;

  rj::Document doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  doc.ParseInsitu<kFlags>(&response.front());

  if (doc.HasParseError()) {
    return Unexpected(errc::bad_message);
  }

  TokenTtl result;
  auto it = doc.FindMember("token_type");
  if (it == doc.MemberEnd() || string_view{it->value.GetString()} != "Bearer"sv) {
    return Unexpected(errc::bad_message);
  }

  it = doc.FindMember("access_token");
  if (it == doc.MemberEnd()) {
    return Unexpected(errc::bad_message);
  }
  result.first = it->value.GetString();
  it = doc.FindMember("expires_in");
  if (it == doc.MemberEnd() || !it->value.IsUint()) {
    return Unexpected(errc::bad_message);
  }
  result.second = it->value.GetUint();

  return result;
}

constexpr unsigned kTcpKeepAliveInterval = 30;

error_code EnableKeepAlive(int fd) {
  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  val = kTcpKeepAliveInterval;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  val = kTcpKeepAliveInterval;
#ifdef __APPLE__
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }
#else
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }
#endif

  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  return std::error_code{};
}

error_code ConfigureMetadataClient(fb2::ProactorBase* pb, http::Client* client) {
  client->set_connect_timeout_ms(1000);
  static const char kMetaDataHost[] = "metadata.google.internal";
  return client->Connect(kMetaDataHost, "80");
}

error_code ReadGCPConfigFromMetadata(fb2::ProactorBase* pb, string* account_id, string* project_id,
                                     TokenTtl* token) {
  http::Client client(pb);
  RETURN_ERROR(ConfigureMetadataClient(pb, &client));

  const char kEmailUrl[] = "/computeMetadata/v1/instance/service-accounts/default/email";
  h2::request<h2::empty_body> req{h2::verb::get, kEmailUrl, 11};
  req.set("Metadata-Flavor", "Google");

  h2::response<h2::string_body> resp;
  RETURN_ERROR(client.Send(req, &resp));
  if (resp.result() != h2::status::ok) {
    LOG(WARNING) << "Http error: " << string(resp.reason()) << ", Body: ", resp.body();
    return make_error_code(errc::permission_denied);
  }

  *account_id = std::move(resp.body());
  resp.clear();

  const char kProjectIdUrl[] = "/computeMetadata/v1/project/project-id";
  req.target(kProjectIdUrl);
  RETURN_ERROR(client.Send(req, &resp));
  if (resp.result() != h2::status::ok) {
    LOG(WARNING) << "Http error: " << string(resp.reason()) << ", Body: ", resp.body();
    return make_error_code(errc::permission_denied);
  }

  *project_id = std::move(resp.body());
  resp.clear();

  req.target(kInstanceTokenUrl);
  RETURN_ERROR(client.Send(req, &resp));
  if (resp.result() != h2::status::ok) {
    LOG(WARNING) << "Http error: " << string(resp.reason()) << ", Body: ", resp.body();
    return make_error_code(errc::permission_denied);
  }
  io::Result<TokenTtl> token_res = ParseTokenResponse(std::move(resp.body()));
  if (!token_res)
    return token_res.error();
  *token = std::move(*token_res);

  return {};
}

#define FETCH_ARRAY_MEMBER(val)                \
  if (!(val).IsArray())                        \
    return make_error_code(errc::bad_message); \
  auto array = val.GetArray()

}  // namespace

error_code GCPCredsProvider::Init(unsigned connect_ms, fb2::ProactorBase* pb) {
  CHECK_GT(connect_ms, 0u);

  io::Result<string> root_path = ExpandFilePath("~/.config/gcloud");
  if (!root_path) {
    return root_path.error();
  }

  bool is_cloud_env = false;
  string gce_file = absl::StrCat(*root_path, "/gce");

  VLOG(1) << "Reading from " << gce_file;

  io::Result<string> gce_file_str = io::ReadFileToString(gce_file);

  if (gce_file_str && *gce_file_str == "True") {
    is_cloud_env = true;
  }

  connect_ms_ = connect_ms;

  if (is_cloud_env) {
    use_instance_metadata_ = true;
    VLOG(1) << "Reading from instance metadata";
    TokenTtl token_ttl;
    RETURN_ERROR(ReadGCPConfigFromMetadata(pb, &account_id_, &project_id_, &token_ttl));

    string inject_token = absl::GetFlag(FLAGS_gcs_auth_token);
    if (!inject_token.empty()) {
      token_ttl.first = inject_token;
    }
    folly::RWSpinLock::WriteHolder lock(lock_);
    access_token_ = token_ttl.first;
    expire_time_.store(time(nullptr) + token_ttl.second, std::memory_order_release);
  } else {
    RETURN_ERROR(LoadGCPConfig(&account_id_, &project_id_));

    if (account_id_.empty() || project_id_.empty()) {
      LOG(WARNING) << "gcloud config file is not valid";
      return make_error_code(errc::not_supported);
    }
    string adc_file = absl::StrCat(*root_path, "/legacy_credentials/", account_id_, "/adc.json");
    VLOG(1) << "ADC file: " << adc_file;
    RETURN_ERROR(ParseADC(adc_file, &client_id_, &client_secret_, &refresh_token_));

    if (client_id_.empty() || client_secret_.empty() || refresh_token_.empty()) {
      LOG(WARNING) << "Bad ADC file " << adc_file;
      return make_error_code(errc::bad_message);
    }

    // At this point we should have all the data to get an access token.
    RETURN_ERROR(RefreshToken(pb));
  }

  return {};
}

error_code GCPCredsProvider::RefreshToken(fb2::ProactorBase* pb) {
  h2::response<h2::string_body> resp;

  if (use_instance_metadata_) {
    http::Client client(pb);
    RETURN_ERROR(ConfigureMetadataClient(pb, &client));

    h2::request<h2::empty_body> req{h2::verb::get, kInstanceTokenUrl, 11};
    req.set("Metadata-Flavor", "Google");
    RETURN_ERROR(client.Send(req, &resp));
  } else {
    constexpr char kDomain[] = "oauth2.googleapis.com";

    http::TlsClient https_client(pb);
    https_client.set_connect_timeout_ms(connect_ms_);
    SSL_CTX* context = http::TlsClient::CreateSslContext();
    error_code ec = https_client.Connect(kDomain, "443", context);
    http::TlsClient::FreeContext(context);

    if (ec) {
      VLOG(1) << "Could not connect to " << kDomain;
      return ec;
    }
    h2::request<h2::string_body> req{h2::verb::post, "/token", 11};
    req.set(h2::field::host, kDomain);
    req.set(h2::field::content_type, "application/x-www-form-urlencoded");

    string& body = req.body();
    body = absl::StrCat("grant_type=refresh_token&client_secret=", client_secret_,
                        "&refresh_token=", refresh_token_);
    absl::StrAppend(&body, "&client_id=", client_id_);
    req.prepare_payload();
    VLOG(1) << "Req: " << req;

    RETURN_ERROR(https_client.Send(req, &resp));

    if (resp.result() != h2::status::ok) {
      LOG(WARNING) << "Http error: " << string(resp.reason()) << ", Body: ", resp.body();
      return make_error_code(errc::permission_denied);
    }
  }

  io::Result<TokenTtl> token = ParseTokenResponse(std::move(resp.body()));
  if (!token)
    return token.error();

  folly::RWSpinLock::WriteHolder lock(lock_);
  access_token_ = token->first;
  expire_time_.store(time(nullptr) + token->second, std::memory_order_release);

  return {};
}

unique_ptr<http::ClientPool> GCS::CreateApiConnectionPool(SSL_CTX* ssl_ctx, fb2::ProactorBase* pb) {
  unique_ptr<http::ClientPool> res(new http::ClientPool(GCS_API_DOMAIN, ssl_ctx, pb));
  res->SetOnConnect([](int fd) {
    auto ec = EnableKeepAlive(fd);
    LOG_IF(WARNING, ec) << "Error setting keep alive " << ec.message() << " " << fd;
  });
  return res;
}

GCS::GCS(GCPCredsProvider* provider, SSL_CTX* ssl_cntx, fb2::ProactorBase* pb)
    : creds_provider_(*provider), ssl_ctx_(ssl_cntx) {
  client_pool_ = CreateApiConnectionPool(ssl_ctx_, pb);
  // TODO: to make it configurable.
  client_pool_->set_connect_timeout(2000);
}

GCS::~GCS() {
}

error_code GCS::ListBuckets(ListBucketCb cb) {
  string url = absl::StrCat("/storage/v1/b?project=", creds_provider_.project_id());
  absl::StrAppend(&url, "&maxResults=50&fields=items/id,nextPageToken");

  detail::EmptyRequestImpl empty_req(h2::verb::get, url, creds_provider_.access_token());

  rj::Document doc;

  RobustSender sender(client_pool_.get(), &creds_provider_);

  while (true) {
    RobustSender::SenderResult result;
    RETURN_ERROR(sender.Send(2, &empty_req, &result));

    h2::response_parser<h2::string_body> resp(std::move(*result.eb_parser));
    auto client = std::move(result.client_handle);

    RETURN_ERROR(client->Recv(&resp));

    auto msg = resp.release();

    VLOG(2) << "ListResponse: " << msg.body();

    doc.ParseInsitu(&msg.body().front());
    if (doc.HasParseError()) {
      return make_error_code(errc::bad_message);
    }

    auto it = doc.FindMember("items");
    if (it == doc.MemberEnd())
      break;

    FETCH_ARRAY_MEMBER(it->value);

    for (size_t i = 0; i < array.Size(); ++i) {
      const auto& item = array[i];
      auto it = item.FindMember("id");
      if (it != item.MemberEnd()) {
        cb(string_view{it->value.GetString(), it->value.GetStringLength()});
      }
    }

    it = doc.FindMember("nextPageToken");
    if (it == doc.MemberEnd()) {
      break;
    }
    absl::string_view page_token{it->value.GetString(), it->value.GetStringLength()};
    empty_req.SetUrl(absl::StrCat(url, "&pageToken=", page_token));
  }
  return {};
}

error_code GCS::List(string_view bucket, string_view prefix, bool recursive, ListObjectCb cb) {
  CHECK(!bucket.empty());

  string url = "/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?maxResults=200&prefix=");
  strings::AppendUrlEncoded(prefix, &url);
  if (!recursive) {
    absl::StrAppend(&url, "&delimiter=%2f");
  }

  detail::EmptyRequestImpl empty_req(h2::verb::get, url, creds_provider_.access_token());

  rj::Document doc;
  RobustSender sender(client_pool_.get(), &creds_provider_);
  while (true) {
    RobustSender::SenderResult parse_res;
    RETURN_ERROR(sender.Send(2, &empty_req, &parse_res));

    h2::response_parser<h2::string_body> resp(std::move(*parse_res.eb_parser));
    auto client = std::move(parse_res.client_handle);

    RETURN_ERROR(client->Recv(&resp));

    auto msg = resp.release();

    doc.ParseInsitu(&msg.body().front());
    if (doc.HasParseError()) {
      return make_error_code(errc::bad_message);
    }

    auto it = doc.FindMember("items");
    if (it != doc.MemberEnd()) {
      FETCH_ARRAY_MEMBER(it->value);

      for (size_t i = 0; i < array.Size(); ++i) {
        const auto& item = array[i];
        auto it = item.FindMember("name");
        CHECK(it != item.MemberEnd());
        absl::string_view key_name(it->value.GetString(), it->value.GetStringLength());
        it = item.FindMember("size");
        CHECK(it != item.MemberEnd());
        absl::string_view sz_str(it->value.GetString(), it->value.GetStringLength());
        size_t item_size = 0;
        CHECK(absl::SimpleAtoi(sz_str, &item_size));
        cb(ObjectItem{item_size, key_name, false});
      }
    }
    it = doc.FindMember("prefixes");
    if (it != doc.MemberEnd()) {
      FETCH_ARRAY_MEMBER(it->value);
      for (size_t i = 0; i < array.Size(); ++i) {
        const auto& item = array[i];
        absl::string_view str(item.GetString(), item.GetStringLength());
        cb(ObjectItem{0, str, true});
      }
    }

    it = doc.FindMember("nextPageToken");
    if (it == doc.MemberEnd()) {
      break;
    }
    absl::string_view page_token{it->value.GetString(), it->value.GetStringLength()};
    empty_req.SetUrl(absl::StrCat(url, "&pageToken=", page_token));
  }
  return {};
}

}  // namespace cloud
}  // namespace util
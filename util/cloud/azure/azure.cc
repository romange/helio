// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <rapidjson/document.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <ctime>
#include <unordered_map>

#include "base/logging.h"
#include "strings/escaping.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/utils.h"
#include "util/fibers/proactor_base.h"
#include "util/http/http_client.h"
#include "util/http/http_common.h"

#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

namespace util {
namespace cloud::azure {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

const char kVersion[] = "2025-01-05";
const char kImdsHost[] = "169.254.169.254";
const char kImdsTokenPath[] = "/metadata/identity/oauth2/token";
const char kImdsApiVersion[] = "2018-02-01";
const char kStorageResource[] = "https://storage.azure.com/";
const char kDefaultEndpointSuffix[] = "core.windows.net";

string GetEnvOrEmpty(const char* name) {
  const char* value = getenv(name);
  return (value && *value) ? value : "";
}

void HMAC(absl::string_view key, absl::string_view msg, uint8_t dest[32]) {
  // HMAC_xxx are deprecated since openssl 3.0
  // Ubuntu 20.04 uses openssl 1.1.
  HMAC_CTX* hmac = HMAC_CTX_new();

  CHECK_EQ(1, HMAC_Init_ex(hmac, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                           EVP_sha256(), NULL));

  CHECK_EQ(1, HMAC_Update(hmac, reinterpret_cast<const uint8_t*>(msg.data()), msg.size()));

  uint8_t* ptr = reinterpret_cast<uint8_t*>(dest);
  unsigned len = 32;
  CHECK_EQ(1, HMAC_Final(hmac, ptr, &len));
  HMAC_CTX_free(hmac);
  CHECK_EQ(len, 32u);
}

string ComputeSignature(string_view account, h2::verb verb, const h2::header<true>& req_header,
                        string_view account_key) {
  string key_bin;
  CHECK(absl::Base64Unescape(account_key, &key_bin));

  vector<pair<string_view, string_view>> x_head;
  for (const auto& h : req_header) {
    if (h.name_string().starts_with("x-ms-")) {
      x_head.emplace_back(detail::FromBoostSV(h.name_string()), detail::FromBoostSV(h.value()));
    }
  }
  sort(x_head.begin(), x_head.end());
  string_view verb_str = detail::FromBoostSV(h2::to_string(verb));

  auto it = req_header.find(h2::field::content_length);
  string content_length;
  if (it != req_header.end() && it->value() != "0") {
    absl::StrAppend(&content_length, detail::FromBoostSV(it->value()), "\n");
  } else {
    content_length = "\n";
  }

  // see here:
  // https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#blob-queue-and-file-services-shared-key-authorization
  string new_lines;
  for (unsigned i = 0; i < 8; ++i)
    absl::StrAppend(&new_lines, "\n");

  string to_sign = absl::StrCat(verb_str, "\n\n\n", content_length, new_lines);
  for (const auto& p : x_head) {
    absl::StrAppend(&to_sign, p.first, ":", p.second, "\n");
  }

  string_view target = detail::FromBoostSV(req_header.target());
  http::QueryParam qparams = http::ParseQuery(target);
  string_view path = qparams.first;
  DCHECK(absl::StartsWith(path, "/"));

  http::QueryArgs args = http::SplitQuery(qparams.second);
  sort(args.begin(), args.end());
  string query_canon;
  for (const auto& p : args) {
    string val;
    if (!strings::AppendUrlDecoded(p.second, &val)) {
      val = p.second;
    }
    absl::StrAppend(&query_canon, "\n", p.first, ":", val);
  }

  string canonic_resource = absl::StrCat("/", account, path, query_canon);
  VLOG(1) << "Canonical resource: " << absl::CEscape(canonic_resource);

  absl::StrAppend(&to_sign, canonic_resource);

  uint8_t dest[32];
  HMAC(key_bin, to_sign, dest);

  string signature = absl::Base64Escape(string_view{reinterpret_cast<char*>(dest), sizeof(dest)});
  return signature;
}

using ConnMap = unordered_map<string, string>;

string LowerCopy(string_view sv) {
  string out(sv);
  absl::AsciiStrToLower(&out);
  return out;
}

ConnMap ParseConnectionStringKV(string_view connection_string) {
  ConnMap result;
  for (string_view part : absl::StrSplit(connection_string, ';')) {
    part = absl::StripAsciiWhitespace(part);
    if (part.empty()) {
      continue;
    }
    pair<string_view, string_view> kv = absl::StrSplit(part, absl::MaxSplits('=', 1));
    string_view key = absl::StripAsciiWhitespace(kv.first);
    if (key.empty()) {
      continue;
    }
    string value = string(absl::StripAsciiWhitespace(kv.second));
    result.emplace(LowerCopy(key), std::move(value));
  }
  return result;
}

string_view MapGet(const ConnMap& map, string_view key) {
  auto it = map.find(LowerCopy(key));
  if (it == map.end()) {
    return {};
  }
  return it->second;
}

bool ParseEndpointHost(string_view endpoint, string* host) {
  endpoint = absl::StripAsciiWhitespace(endpoint);
  if (endpoint.empty()) {
    return false;
  }

  ParsedHttpUrl parsed = ParseHttpUrl(endpoint);
  if (parsed.host.empty()) {
    return false;
  }

  bool default_port = parsed.port == (parsed.is_https ? "443" : "80");
  *host = default_port ? std::move(parsed.host) : absl::StrCat(parsed.host, ":", parsed.port);
  return true;
}

string InferAccountNameFromHost(string_view host) {
  auto pos = host.find(".blob.");
  if (pos == string_view::npos || pos == 0) {
    return "";
  }
  return string(host.substr(0, pos));
}

bool TargetHasSasSignature(string_view target) {
  http::QueryParam qparams = http::ParseQuery(target);
  http::QueryArgs args = http::SplitQuery(qparams.second);
  for (const auto& [key, val] : args) {
    if (absl::EqualsIgnoreCase(key, "sig") && !val.empty()) {
      return true;
    }
  }
  return false;
}

string AddSasToTarget(string_view target, string_view sas_query) {
  if (sas_query.empty() || TargetHasSasSignature(target)) {
    return string(target);
  }
  if (target.find('?') == string_view::npos) {
    return absl::StrCat(target, "?", sas_query);
  }
  return absl::StrCat(target, "&", sas_query);
}

io::Result<pair<string, unsigned>> ParseImdsTokenResponse(string&& response) {
  rapidjson::Document doc;
  doc.ParseInsitu<rapidjson::kParseCommentsFlag | rapidjson::kParseTrailingCommasFlag>(
      &response.front());
  if (doc.HasParseError()) {
    LOG(ERROR) << "azure: failed to parse managed identity response";
    return nonstd::make_unexpected(make_error_code(errc::bad_message));
  }

  auto token_it = doc.FindMember("access_token");
  if (token_it == doc.MemberEnd() || !token_it->value.IsString()) {
    LOG(ERROR) << "azure: managed identity response missing access_token";
    return nonstd::make_unexpected(make_error_code(errc::bad_message));
  }

  auto exp_it = doc.FindMember("expires_in");
  if (exp_it == doc.MemberEnd()) {
    LOG(ERROR) << "azure: managed identity response missing expires_in";
    return nonstd::make_unexpected(make_error_code(errc::bad_message));
  }

  unsigned expires_in = 0;
  if (exp_it->value.IsUint()) {
    expires_in = exp_it->value.GetUint();
  } else if (exp_it->value.IsString() && absl::SimpleAtoi(exp_it->value.GetString(), &expires_in)) {
  } else {
    return nonstd::make_unexpected(make_error_code(errc::bad_message));
  }

  return make_pair(string(token_it->value.GetString()), expires_in);
}

error_code ResolveServiceEndpointFromEnv(string_view account_name, string* service_endpoint,
                                         string* inferred_account_name) {
  if (const char* ep = getenv("AZURE_STORAGE_BLOB_ENDPOINT"); ep && *ep) {
    if (!ParseEndpointHost(ep, service_endpoint)) {
      return make_error_code(errc::bad_message);
    }
  } else if (const char* url = getenv("AZURE_STORAGE_ACCOUNT_URL"); url && *url) {
    if (!ParseEndpointHost(url, service_endpoint)) {
      return make_error_code(errc::bad_message);
    }
  } else if (!account_name.empty()) {
    *service_endpoint = absl::StrCat(account_name, ".blob.", kDefaultEndpointSuffix);
  } else {
    return make_error_code(errc::not_supported);
  }

  *inferred_account_name = InferAccountNameFromHost(*service_endpoint);
  return {};
}

}  // namespace

error_code Credentials::Init(unsigned connect_ms) {
  connect_ms_ = connect_ms ? connect_ms : 1000;

  account_name_.clear();
  account_key_.clear();
  service_endpoint_.clear();
  sas_query_.clear();
  source_ = CredSource::kNone;
  auth_mode_ = AuthMode::kNone;
  managed_identity_client_id_.clear();

  managed_identity_client_id_ = GetEnvOrEmpty("AZURE_CLIENT_ID");

  vector<string> errors;
  auto try_source = [&](string_view name, auto fn) -> bool {
    error_code ec = (this->*fn)();
    if (!ec) {
      LOG_FIRST_N(INFO, 1) << "azure: loaded credentials; provider=" << name
                           << " account=" << account_name_;
      return true;
    }
    if (ec != errc::not_supported) {
      errors.push_back(absl::StrCat(name, "=", ec.message()));
      VLOG(1) << "azure: credential source " << name << " failed: " << ec.message();
    }
    return false;
  };

  if (try_source("connection-string", &Credentials::TryConnectionString)) {
    return {};
  }
  if (try_source("environment", &Credentials::TryEnvSharedKey)) {
    return {};
  }
  if (try_source("managed-identity", &Credentials::TryManagedIdentity)) {
    return {};
  }

  if (!errors.empty()) {
    LOG(ERROR) << "azure: no credential provider found, attempts: " << absl::StrJoin(errors, "; ");
  } else {
    LOG(ERROR) << "azure: no credential provider found";
  }
  return make_error_code(errc::permission_denied);
}

string Credentials::ServiceEndpoint() const {
  if (!service_endpoint_.empty()) {
    return service_endpoint_;
  }
  return absl::StrCat(account_name_, ".blob.", kDefaultEndpointSuffix);
}

error_code Credentials::TryConnectionString() {
  const char* raw = getenv("AZURE_STORAGE_CONNECTION_STRING");
  if (!raw || !*raw) {
    return make_error_code(errc::not_supported);
  }

  ConnMap params = ParseConnectionStringKV(raw);
  if (params.empty()) {
    return make_error_code(errc::bad_message);
  }
  if (absl::EqualsIgnoreCase(MapGet(params, "UseDevelopmentStorage"), "true")) {
    LOG(WARNING) << "azure: UseDevelopmentStorage connection strings are not supported";
    return make_error_code(errc::not_supported);
  }

  string account_name(MapGet(params, "AccountName"));
  string account_key(MapGet(params, "AccountKey"));
  string sas = NormalizeSasQuery(MapGet(params, "SharedAccessSignature"));

  string endpoint;
  string_view blob_endpoint = MapGet(params, "BlobEndpoint");
  if (!blob_endpoint.empty()) {
    if (!ParseEndpointHost(blob_endpoint, &endpoint)) {
      return make_error_code(errc::bad_message);
    }
  } else {
    string endpoint_suffix(MapGet(params, "EndpointSuffix"));
    if (endpoint_suffix.empty()) {
      endpoint_suffix = kDefaultEndpointSuffix;
    }
    if (account_name.empty()) {
      return make_error_code(errc::bad_message);
    }
    endpoint = absl::StrCat(account_name, ".blob.", endpoint_suffix);
  }

  if (!account_key.empty()) {
    if (account_name.empty()) {
      account_name = InferAccountNameFromHost(endpoint);
    }
    if (account_name.empty()) {
      return make_error_code(errc::bad_message);
    }

    SetSharedKey(CredSource::kConnectionString, std::move(account_name), std::move(account_key),
                 std::move(endpoint));
    return {};
  }

  if (!sas.empty()) {
    SetSas(CredSource::kConnectionString, std::move(account_name), std::move(endpoint),
           std::move(sas));
    return {};
  }

  return make_error_code(errc::bad_message);
}

error_code Credentials::TryEnvSharedKey() {
  string account = GetEnvOrEmpty("AZURE_STORAGE_ACCOUNT");
  string key = GetEnvOrEmpty("AZURE_STORAGE_KEY");
  string sas = NormalizeSasQuery(GetEnvOrEmpty("AZURE_STORAGE_SAS_TOKEN"));

  string endpoint;
  string inferred_account;
  error_code ep_ec = ResolveServiceEndpointFromEnv(account, &endpoint, &inferred_account);

  if (!key.empty()) {
    if (account.empty()) {
      account = inferred_account;
    }
    if (account.empty() || ep_ec) {
      return ep_ec ? ep_ec : make_error_code(errc::bad_message);
    }

    SetSharedKey(CredSource::kEnv, std::move(account), std::move(key), std::move(endpoint));
    return {};
  }

  if (!sas.empty()) {
    if (ep_ec) {
      return ep_ec;
    }

    SetSas(CredSource::kEnv, std::move(account), std::move(endpoint), std::move(sas));
    return {};
  }

  return make_error_code(errc::not_supported);
}

error_code Credentials::TryManagedIdentity() {
  string account = GetEnvOrEmpty("AZURE_STORAGE_ACCOUNT");
  string endpoint;
  string inferred_account;

  RETURN_ERROR(ResolveServiceEndpointFromEnv(account, &endpoint, &inferred_account));
  if (account.empty()) {
    account = std::move(inferred_account);
  }

  fb2::ProactorBase* pb = fb2::ProactorBase::me();
  CHECK(pb);

  http::Client client(pb);
  client.set_connect_timeout_ms(connect_ms_);

  RETURN_ERROR(client.Connect(kImdsHost, "80"));

  string path = absl::StrCat(kImdsTokenPath, "?api-version=", kImdsApiVersion, "&resource=");
  strings::AppendUrlEncoded(kStorageResource, &path);
  if (!managed_identity_client_id_.empty()) {
    absl::StrAppend(&path, "&client_id=");
    strings::AppendUrlEncoded(managed_identity_client_id_, &path);
  }

  h2::request<h2::empty_body> req{h2::verb::get, path, 11};
  req.set("Metadata", "true");
  req.set(h2::field::host, kImdsHost);

  h2::response<h2::string_body> resp;
  RETURN_ERROR(client.Send(req, &resp));
  if (resp.result() != h2::status::ok) {
    VLOG(1) << "azure: managed identity response " << resp.result_int() << ": " << resp.body();
    return make_error_code(errc::permission_denied);
  }

  auto token_ttl = ParseImdsTokenResponse(std::move(resp.body()));
  if (!token_ttl) {
    return token_ttl.error();
  }

  SetBearer(std::move(account), std::move(endpoint), std::move(token_ttl->first),
            token_ttl->second);
  return {};
}

void Credentials::SetSharedKey(CredSource src, string account, string key, string endpoint) {
  account_name_ = std::move(account);
  account_key_ = std::move(key);
  service_endpoint_ = std::move(endpoint);
  sas_query_.clear();
  source_ = src;
  auth_mode_ = AuthMode::kSharedKey;
  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    access_token_.clear();
  }
  expire_time_.store(0, std::memory_order_release);
}

void Credentials::SetSas(CredSource src, string account, string endpoint, string sas) {
  account_name_ = std::move(account);
  account_key_.clear();
  service_endpoint_ = std::move(endpoint);
  sas_query_ = std::move(sas);
  source_ = src;
  auth_mode_ = AuthMode::kSas;
  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    access_token_.clear();
  }
  expire_time_.store(0, std::memory_order_release);
}

void Credentials::SetBearer(string account, string endpoint, string token, unsigned ttl) {
  VLOG(1) << "azure: obtained access token " << token << ", account=" << account
          << ", endpoint=" << endpoint << ", expires in " << ttl << " seconds";
  account_name_ = std::move(account);
  account_key_.clear();
  service_endpoint_ = std::move(endpoint);
  sas_query_.clear();
  source_ = CredSource::kManagedIdentity;
  auth_mode_ = AuthMode::kBearer;
  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    access_token_ = std::move(token);
  }
  const time_t now = time(nullptr);
  expire_time_.store(now + (ttl > 60 ? ttl - 60 : ttl), std::memory_order_release);
}

string Credentials::NormalizeSasQuery(string_view query) {
  query = absl::StripAsciiWhitespace(query);
  if (!query.empty() && query.front() == '?') {
    query.remove_prefix(1);
  }
  return string(query);
}

void Credentials::Sign(detail::HttpRequestBase* req) const {
  const absl::TimeZone utc_tz = absl::UTCTimeZone();
  string date = absl::FormatTime("%a, %d %b %Y %H:%M:%S GMT", absl::Now(), utc_tz);
  req->SetHeader("x-ms-date", date);
  req->SetHeader("x-ms-version", kVersion);

  switch (auth_mode_) {
    case AuthMode::kSharedKey: {
      string signature =
          ComputeSignature(account_name_, req->GetMethod(), req->GetHeaders(), account_key_);
      req->SetHeader("Authorization", absl::StrCat("SharedKey ", account_name_, ":", signature));
      break;
    }
    case AuthMode::kBearer: {
      string token;
      {
        folly::RWSpinLock::ReadHolder lock(lock_);
        token = access_token_;
      }
      req->SetHeader("Authorization", absl::StrCat("Bearer ", token));
      break;
    }
    case AuthMode::kSas: {
      string target = AddSasToTarget(req->GetTarget(), sas_query_);
      req->SetTarget(target);
      break;
    }
    case AuthMode::kNone:
      break;
  }
}

std::error_code Credentials::RefreshToken() {
  if (source_ == CredSource::kManagedIdentity) {
    return TryManagedIdentity();
  }
  return {};
}

}  // namespace cloud::azure
}  // namespace util
// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/aws/aws_creds_provider.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <rapidjson/document.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <pugixml.hpp>

#include "base/logging.h"
#include "io/file_util.h"
#include "strings/escaping.h"
#include "util/cloud/utils.h"
#include "util/fibers/proactor_base.h"
#include "util/http/http_client.h"
#include "util/http/http_common.h"

namespace util {
namespace cloud::aws {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

// SHA256("") — used as payload hash for all GET requests.
constexpr string_view kEmptyBodyHash =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

string HexEncode(const uint8_t* data, size_t len) {
  static const char kHex[] = "0123456789abcdef";
  string result;
  result.reserve(len * 2);
  for (size_t i = 0; i < len; ++i) {
    result.push_back(kHex[data[i] >> 4]);
    result.push_back(kHex[data[i] & 0xf]);
  }
  return result;
}

void HmacSha256(string_view key, string_view msg, uint8_t dest[32]) {
  unsigned int len = 32;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const uint8_t*>(msg.data()), msg.size(), dest, &len);
}

string Sha256Hex(string_view input) {
  uint8_t hash[EVP_MAX_MD_SIZE];
  unsigned len = 0;
  EVP_Digest(input.data(), input.size(), hash, &len, EVP_sha256(), nullptr);
  return HexEncode(hash, len);
}

// Percent-encodes a path per AWS SigV4 rules: encode each segment individually,
// preserving '/' separators between segments.
string CanonicalUri(string_view path) {
  if (path.empty() || path == "/") {
    return "/";
  }
  vector<string_view> segments = absl::StrSplit(path, '/');
  string result;
  result.reserve(path.size() + 16);
  for (size_t i = 0; i < segments.size(); ++i) {
    if (i > 0)
      result.push_back('/');
    strings::AppendUrlEncoded(segments[i], &result);
  }
  return result;
}

// The query string is already percent-encoded in the URL. Per SigV4, the canonical query string
// must have each param name and value URI-encoded. Since the URL already contains encoded values,
// we just sort and reassemble without re-encoding to avoid double-encoding (e.g. %2F -> %252F).
string CanonicalQueryString(string_view query) {
  if (query.empty()) {
    return "";
  }
  http::QueryArgs args = http::SplitQuery(query);
  sort(args.begin(), args.end());

  string result;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0)
      result.push_back('&');
    absl::StrAppend(&result, args[i].first, "=", args[i].second);
  }
  return result;
}

// Parses credentials JSON from IMDS or container endpoint:
// {"AccessKeyId":"...","SecretAccessKey":"...","Token":"...","Expiration":"2024-01-01T10:00:00Z"}
error_code ParseJsonCredentials(string_view json, AwsCredentials* out) {
  rapidjson::Document doc;
  // ParseInsitu modifies the buffer; use a mutable copy.
  string json_copy(json);
  doc.ParseInsitu(&json_copy.front());

  if (doc.HasParseError()) {
    LOG(ERROR) << "aws: failed to parse credentials JSON";
    return make_error_code(errc::bad_message);
  }

  auto get_str = [&](const char* key, string* dest) -> bool {
    auto it = doc.FindMember(key);
    if (it == doc.MemberEnd() || !it->value.IsString())
      return false;
    *dest = it->value.GetString();
    return true;
  };

  if (!get_str("AccessKeyId", &out->access_key_id) ||
      !get_str("SecretAccessKey", &out->secret_access_key)) {
    LOG(ERROR) << "aws: credentials JSON missing AccessKeyId or SecretAccessKey";
    return make_error_code(errc::bad_message);
  }

  // Token is optional for non-temporary credentials.
  get_str("Token", &out->session_token);

  string expiration;
  if (get_str("Expiration", &expiration)) {
    absl::Time t;
    string err;
    if (absl::ParseTime("%Y-%m-%dT%H:%M:%E*SZ", expiration, &t, &err)) {
      out->expiry = absl::ToTimeT(t);
    } else {
      LOG(WARNING) << "aws: failed to parse expiration '" << expiration << "': " << err;
    }
  }
  return {};
}

// Parses STS AssumeRoleWithWebIdentity XML response.
error_code ParseStsXml(string_view xml, AwsCredentials* out) {
  pugi::xml_document doc;
  pugi::xml_parse_result res = doc.load_buffer(xml.data(), xml.size());
  if (!res) {
    LOG(ERROR) << "aws: failed to parse STS XML: " << res.description() << "\n" << xml;
    return make_error_code(errc::bad_message);
  }

  // Response: AssumeRoleWithWebIdentityResponse > ... > Credentials > {fields}
  pugi::xml_node creds =
      doc.find_node([](pugi::xml_node n) { return strcmp(n.name(), "Credentials") == 0; });
  if (!creds) {
    LOG(ERROR) << "aws: STS response missing Credentials node\n" << xml;
    return make_error_code(errc::bad_message);
  }

  out->access_key_id = creds.child_value("AccessKeyId");
  out->secret_access_key = creds.child_value("SecretAccessKey");
  out->session_token = creds.child_value("SessionToken");

  if (out->access_key_id.empty() || out->secret_access_key.empty()) {
    LOG(ERROR) << "aws: STS Credentials missing AccessKeyId or SecretAccessKey\n" << xml;
    return make_error_code(errc::bad_message);
  }

  string_view expiration = creds.child_value("Expiration");
  if (!expiration.empty()) {
    absl::Time t;
    string err;
    if (absl::ParseTime("%Y-%m-%dT%H:%M:%E*SZ", expiration, &t, &err)) {
      out->expiry = absl::ToTimeT(t);
    } else {
      LOG(WARNING) << "aws: failed to parse STS expiration '" << expiration << "': " << err;
    }
  }
  return {};
}

// ssl_ctx: non-null => TLS connection; null => plain HTTP.
io::Result<string> FetchUrl(string_view host, string_view port, string_view path, h2::verb method,
                            const vector<pair<string, string>>& headers, string_view body,
                            unsigned connect_ms, SSL_CTX* ssl_ctx) {
  fb2::ProactorBase* pb = fb2::ProactorBase::me();
  CHECK(pb);

  auto build_and_send = [&](auto& client) -> io::Result<string> {
    h2::request<h2::string_body> req{method, boost::string_view{path.data(), path.size()}, 11};
    req.set(h2::field::host, boost::string_view{host.data(), host.size()});
    for (const auto& h : headers) {
      req.set(boost::string_view{h.first.data(), h.first.size()},
              boost::string_view{h.second.data(), h.second.size()});
    }
    if (!body.empty()) {
      req.body() = string(body);
    }
    req.prepare_payload();

    h2::response<h2::string_body> resp;
    if (auto ec = client.Send(req, &resp); ec)
      return nonstd::make_unexpected(ec);
    if (resp.result() != h2::status::ok) {
      LOG(WARNING) << "aws: FetchUrl " << host << path << " returned " << resp.result_int();
      VLOG(1) << "aws: FetchUrl error body: " << resp.body();
      return nonstd::make_unexpected(make_error_code(errc::permission_denied));
    }
    return std::move(resp.body());
  };

  if (ssl_ctx) {
    http::TlsClient client(pb);
    client.set_connect_timeout_ms(connect_ms);
    auto ec = client.Connect(host, port, ssl_ctx);
    if (ec) {
      VLOG(1) << "aws: FetchUrl TLS connect failed to " << host << ":" << port << ": "
              << ec.message();
      return nonstd::make_unexpected(ec);
    }
    return build_and_send(client);
  } else {
    http::Client client(pb);
    client.set_connect_timeout_ms(connect_ms);
    auto ec = client.Connect(host, port);
    if (ec) {
      VLOG(1) << "aws: FetchUrl connect failed to " << host << ":" << port << ": " << ec.message();
      return nonstd::make_unexpected(ec);
    }
    return build_and_send(client);
  }
}

}  // namespace

AwsCredsProvider::AwsCredsProvider(string region) : region_(std::move(region)) {
}

AwsCredsProvider::~AwsCredsProvider() {
  if (ssl_ctx_)
    http::TlsClient::FreeContext(ssl_ctx_);
}

SSL_CTX* AwsCredsProvider::GetSslCtx() {
  if (!ssl_ctx_)
    ssl_ctx_ = http::TlsClient::CreateSslContext();
  return ssl_ctx_;
}

error_code AwsCredsProvider::Init(unsigned connect_ms) {
  connect_ms_ = connect_ms;

  // Discover region if not provided.
  if (region_.empty()) {
    if (const char* r = getenv("AWS_DEFAULT_REGION")) {
      region_ = r;
    } else if (const char* r = getenv("AWS_REGION")) {
      region_ = r;
    } else {
      // Try ~/.aws/config — uses [default] or [profile name] sections.
      const char* config_file_env = getenv("AWS_CONFIG_FILE");
      string config_path;
      if (config_file_env) {
        config_path = config_file_env;
      } else {
        const char* home = getenv("HOME");
        config_path = home ? absl::StrCat(home, "/.aws/config") : "/.aws/config";
      }
      if (auto contents = io::ReadFileToString(config_path)) {
        const char* profile_env = getenv("AWS_PROFILE");
        string profile = profile_env ? profile_env : "default";
        // Config file uses [default] for default profile, [profile name] for others.
        string section = (profile == "default") ? "default" : absl::StrCat("profile ", profile);
        ParseIniSection(*contents, section, [&](string_view key, string_view val) {
          if (key == "region" && region_.empty()) {
            region_ = string(val);
          }
        });
      }
      if (region_.empty()) {
        region_ = "us-east-1";
      }
    }
  }

  if (auto ec = TryEnvironment(); !ec)
    return {};
  if (auto ec = TryProfileFile(); !ec)
    return {};
  if (auto ec = TryWebIdentity(); !ec)
    return {};
  if (auto ec = TryContainerCreds(); !ec)
    return {};

  const char* disabled = getenv("AWS_EC2_METADATA_DISABLED");
  if (!disabled || absl::AsciiStrToLower(disabled) != "true") {
    if (auto ec = TryIMDS(); !ec)
      return {};
  } else {
    LOG(INFO) << "aws: EC2 metadata disabled";
  }

  LOG(ERROR) << "aws: no credential provider found";
  return make_error_code(errc::permission_denied);
}

error_code AwsCredsProvider::TryEnvironment() {
  const char* key_id = getenv("AWS_ACCESS_KEY_ID");
  const char* secret = getenv("AWS_SECRET_ACCESS_KEY");
  if (!key_id || !secret || !*key_id || !*secret) {
    return make_error_code(errc::not_supported);
  }

  AwsCredentials creds;
  creds.access_key_id = key_id;
  creds.secret_access_key = secret;
  if (const char* tok = getenv("AWS_SESSION_TOKEN")) {
    creds.session_token = tok;
  }

  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    creds_ = std::move(creds);
  }
  source_ = CredSource::kEnv;
  LOG_FIRST_N(INFO, 1) << "aws: loaded credentials; provider=environment";
  return {};
}

error_code AwsCredsProvider::TryProfileFile() {
  const char* creds_file_env = getenv("AWS_SHARED_CREDENTIALS_FILE");
  string creds_path;
  if (creds_file_env) {
    creds_path = creds_file_env;
  } else {
    const char* home = getenv("HOME");
    creds_path = home ? absl::StrCat(home, "/.aws/credentials") : "/.aws/credentials";
  }

  auto contents = io::ReadFileToString(creds_path);
  if (!contents) {
    VLOG(1) << "aws: profile file not found: " << creds_path;
    return make_error_code(errc::not_supported);
  }

  const char* profile_env = getenv("AWS_PROFILE");
  string profile = profile_env ? profile_env : "default";

  AwsCredentials creds;
  ParseIniSection(*contents, profile, [&](string_view key, string_view val) {
    if (key == "aws_access_key_id") {
      creds.access_key_id = string(val);
    } else if (key == "aws_secret_access_key") {
      creds.secret_access_key = string(val);
    } else if (key == "aws_session_token") {
      creds.session_token = string(val);
    }
  });

  if (creds.empty()) {
    VLOG(1) << "aws: profile [" << profile << "] not found or empty in " << creds_path;
    return make_error_code(errc::not_supported);
  }

  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    creds_ = std::move(creds);
  }
  source_ = CredSource::kProfile;
  LOG_FIRST_N(INFO, 1) << "aws: loaded credentials; provider=profile file";
  return {};
}

error_code AwsCredsProvider::TryWebIdentity() {
  const char* role_arn = getenv("AWS_ROLE_ARN");
  const char* token_file = getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
  if (!role_arn || !*role_arn || !token_file || !*token_file) {
    return make_error_code(errc::not_supported);
  }

  auto token_res = io::ReadFileToString(token_file);
  if (!token_res) {
    LOG(WARNING) << "aws: failed to read web identity token file " << token_file;
    return make_error_code(errc::not_supported);
  }

  string token(absl::StripAsciiWhitespace(*token_res));
  string body = "Action=AssumeRoleWithWebIdentity&RoleArn=";
  strings::AppendUrlEncoded(role_arn, &body);
  body += "&WebIdentityToken=";
  strings::AppendUrlEncoded(token, &body);
  body += "&RoleSessionName=helio&Version=2011-06-15";

  auto resp = FetchUrl("sts.amazonaws.com", "443", "/", h2::verb::post,
                       {{"content-type", "application/x-www-form-urlencoded"}}, body, connect_ms_,
                       GetSslCtx());
  if (!resp)
    return resp.error();

  AwsCredentials creds;
  RETURN_ERROR(ParseStsXml(*resp, &creds));

  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    creds_ = std::move(creds);
  }
  role_arn_ = role_arn;
  web_identity_token_file_ = token_file;
  source_ = CredSource::kWebIdentity;
  LOG_FIRST_N(INFO, 1) << "aws: loaded credentials; provider=web-identity";
  return {};
}

error_code AwsCredsProvider::TryContainerCreds() {
  string uri;
  const char* relative = getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
  const char* absolute = getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI");

  if (relative && *relative) {
    uri = absl::StrCat("http://169.254.170.2", relative);
  } else if (absolute && *absolute) {
    uri = absolute;
  } else {
    return make_error_code(errc::not_supported);
  }

  // Read optional bearer token.
  string auth_token;
  if (const char* tok = getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN")) {
    auth_token = tok;
  } else if (const char* tok_file = getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")) {
    auto tok_res = io::ReadFileToString(tok_file);
    if (tok_res) {
      auth_token = absl::StripAsciiWhitespace(*tok_res);
    }
  }

  container_uri_ = uri;

  // Determine if http or https.
  vector<pair<string, string>> headers;
  if (!auth_token.empty()) {
    headers.push_back({"Authorization", absl::StrCat("Bearer ", auth_token)});
  }

  auto [host, port, path, is_https] = ParseHttpUrl(uri);
  auto resp = FetchUrl(host, port, path, h2::verb::get, headers, "", connect_ms_,
                       is_https ? GetSslCtx() : nullptr);
  if (!resp)
    return resp.error();

  AwsCredentials creds;
  RETURN_ERROR(ParseJsonCredentials(*resp, &creds));

  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    creds_ = std::move(creds);
  }
  source_ = CredSource::kContainer;
  LOG_FIRST_N(INFO, 1) << "aws: loaded credentials; provider=container-credentials";
  return {};
}

error_code AwsCredsProvider::TryIMDS() {
  // IMDSv2: first obtain a session token.
  auto imds_token_res =
      FetchUrl("169.254.169.254", "80", "/latest/api/token", h2::verb::put,
               {{"X-aws-ec2-metadata-token-ttl-seconds", "21600"}}, "", connect_ms_, nullptr);
  if (!imds_token_res)
    return imds_token_res.error();
  string imds_token = string(absl::StripAsciiWhitespace(*imds_token_res));

  vector<pair<string, string>> token_hdr = {{"X-aws-ec2-metadata-token", imds_token}};

  // Get the IAM role name.
  auto role_name_res =
      FetchUrl("169.254.169.254", "80", "/latest/meta-data/iam/security-credentials/",
               h2::verb::get, token_hdr, "", connect_ms_, nullptr);
  if (!role_name_res)
    return role_name_res.error();
  string role_name_raw = std::move(*role_name_res);

  imds_role_ = string(absl::StripAsciiWhitespace(role_name_raw));
  if (imds_role_.empty()) {
    LOG(WARNING) << "aws: IMDS returned empty role name";
    return make_error_code(errc::not_supported);
  }

  // Get the credentials for the role.
  auto json = FetchUrl("169.254.169.254", "80",
                       absl::StrCat("/latest/meta-data/iam/security-credentials/", imds_role_),
                       h2::verb::get, token_hdr, "", connect_ms_, nullptr);
  if (!json)
    return json.error();

  AwsCredentials creds;
  RETURN_ERROR(ParseJsonCredentials(*json, &creds));

  {
    folly::RWSpinLock::WriteHolder lock(lock_);
    creds_ = std::move(creds);
  }
  source_ = CredSource::kIMDS;
  LOG_FIRST_N(INFO, 1) << "aws: loaded credentials; provider=ec2-metadata; role=" << imds_role_;
  return {};
}

error_code AwsCredsProvider::RefreshToken() {
  switch (source_) {
    case CredSource::kEnv:
    case CredSource::kProfile:
      return {};  // static credentials never expire
    case CredSource::kWebIdentity:
      return TryWebIdentity();
    case CredSource::kContainer:
      return TryContainerCreds();
    case CredSource::kIMDS:
      return TryIMDS();
    case CredSource::kNone:
      break;
  }
  return make_error_code(errc::permission_denied);
}

string AwsCredsProvider::ServiceEndpoint() const {
  if (const char* ep = getenv("AWS_S3_ENDPOINT"); ep && *ep) {
    auto [host, port, path, is_https] = ParseHttpUrl(ep);
    return port == (is_https ? "443" : "80") ? host : absl::StrCat(host, ":", port);
  }
  return absl::StrCat("s3.", region_, ".amazonaws.com");
}

void AwsCredsProvider::Sign(detail::HttpRequestBase* req) const {
  AwsCredentials creds;
  {
    folly::RWSpinLock::ReadHolder lock(lock_);
    creds = creds_;
  }

  const absl::TimeZone utc = absl::UTCTimeZone();
  absl::Time now = absl::Now();
  string datetime = absl::FormatTime("%Y%m%dT%H%M%SZ", now, utc);
  string date = absl::FormatTime("%Y%m%d", now, utc);

  // Use caller-supplied payload hash if already set (e.g. "UNSIGNED-PAYLOAD" for writes);
  // otherwise default to SHA256("") for unsigned GET requests.
  const auto& headers = req->GetHeaders();
  auto hash_it = headers.find("x-amz-content-sha256");
  string payload_hash = (hash_it != headers.end())
                            ? string(detail::FromBoostSV(hash_it->value()))
                            : string(kEmptyBodyHash);

  req->SetHeader("x-amz-date", datetime);
  req->SetHeader("x-amz-content-sha256", payload_hash);
  if (!creds.session_token.empty()) {
    req->SetHeader("x-amz-security-token", creds.session_token);
  }
  vector<pair<string, string>> signed_hdrs;

  for (const auto& field : headers) {
    string name = absl::AsciiStrToLower(string(detail::FromBoostSV(field.name_string())));
    if (name == "host" || name == "x-amz-date" || name == "x-amz-security-token" ||
        absl::StartsWith(name, "x-amz-")) {
      string val = string(absl::StripAsciiWhitespace(detail::FromBoostSV(field.value())));
      signed_hdrs.push_back({std::move(name), std::move(val)});
    }
  }
  sort(signed_hdrs.begin(), signed_hdrs.end());

  // Build canonical headers block (each line: "lowercase_name:value\n")
  // and signed headers string ("name1;name2;...").
  string canonical_headers;
  string signed_headers_str;
  for (const auto& h : signed_hdrs) {
    absl::StrAppend(&canonical_headers, h.first, ":", h.second, "\n");
    if (!signed_headers_str.empty())
      signed_headers_str.push_back(';');
    absl::StrAppend(&signed_headers_str, h.first);
  }

  // Extract path and query from the request target.
  string_view target = detail::FromBoostSV(headers.target());
  auto [path, query] = http::ParseQuery(target);
  string canonical_uri = CanonicalUri(path);
  string canonical_qs = CanonicalQueryString(query);

  // Build canonical request.
  string_view method_str = detail::FromBoostSV(h2::to_string(req->GetMethod()));
  string canonical_request = absl::StrCat(
      method_str, "\n",
      canonical_uri, "\n",
      canonical_qs, "\n",
      canonical_headers, "\n",  // canonical_headers already ends with \n; this adds blank line
      signed_headers_str, "\n",
      payload_hash);

  // Credential scope and string to sign.
  string credential_scope = absl::StrCat(date, "/", region_, "/s3/aws4_request");
  string string_to_sign = absl::StrCat("AWS4-HMAC-SHA256\n", datetime, "\n", credential_scope, "\n",
                                       Sha256Hex(canonical_request));

  // Derive signing key.
  uint8_t k_date[32], k_region[32], k_service[32], k_signing[32];
  HmacSha256(absl::StrCat("AWS4", creds.secret_access_key), date, k_date);
  HmacSha256(string_view(reinterpret_cast<const char*>(k_date), 32), region_, k_region);
  HmacSha256(string_view(reinterpret_cast<const char*>(k_region), 32), "s3", k_service);
  HmacSha256(string_view(reinterpret_cast<const char*>(k_service), 32), "aws4_request", k_signing);

  // Compute final signature.
  uint8_t sig_bytes[32];
  HmacSha256(string_view(reinterpret_cast<const char*>(k_signing), 32), string_to_sign, sig_bytes);
  string signature = HexEncode(sig_bytes, 32);

  // Set Authorization header.
  string auth =
      absl::StrCat("AWS4-HMAC-SHA256 Credential=", creds.access_key_id, "/", credential_scope,
                   ", SignedHeaders=", signed_headers_str, ", Signature=", signature);
  req->SetHeader(h2::field::authorization, auth);
}

}  // namespace cloud::aws
}  // namespace util

// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/aws.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <rapidjson/document.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>
#include <optional>

#include "base/logging.h"
#include "io/file.h"
#include "io/line_reader.h"
#include "util/proactor_base.h"

namespace util {
namespace cloud {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

const char* kExpiredTokenSentinel = "<Error><Code>ExpiredToken</Code>";

/*void EVPDigest(const ::boost::beast::multi_buffer& mb, unsigned char* md) {
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  CHECK_EQ(1, EVP_DigestInit_ex(ctx, EVP_sha256(), NULL));
  for (const auto& e : mb.cdata()) {
    CHECK_EQ(1, EVP_DigestUpdate(ctx, e.data(), e.size()));
  }
  unsigned temp;
  CHECK_EQ(1, EVP_DigestFinal_ex(ctx, md, &temp));
}*/

void Hexify(const uint8_t* str, size_t len, char* dest) {
  static constexpr char kHex[] = "0123456789abcdef";

  for (unsigned i = 0; i < len; ++i) {
    char c = str[i];
    *dest++ = kHex[(c >> 4) & 0xF];
    *dest++ = kHex[c & 0xF];
  }
  *dest = '\0';
}

/*void Sha256String(const ::boost::beast::multi_buffer& mb, char out[65]) {
  uint8_t hash[32];
  EVPDigest(mb, hash);

  Hexify(hash, sizeof(hash), out);
}*/

void Sha256String(string_view str, char out[65]) {
  uint8_t hash[32];
  unsigned temp;

  CHECK_EQ(1, EVP_Digest(str.data(), str.size(), hash, &temp, EVP_sha256(), NULL));

  Hexify(hash, sizeof(hash), out);
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

string DeriveSigKey(absl::string_view key, absl::string_view datestamp, absl::string_view region,
                    absl::string_view service) {
  uint8_t sign[32];
  HMAC_CTX* hmac = HMAC_CTX_new();
  unsigned len;

  string start_key{"AWS4"};
  string_view sign_key{reinterpret_cast<char*>(sign), sizeof(sign)};

  absl::StrAppend(&start_key, key);
  CHECK_EQ(1, HMAC_Init_ex(hmac, start_key.data(), start_key.size(), EVP_sha256(), NULL));
  CHECK_EQ(1,
           HMAC_Update(hmac, reinterpret_cast<const uint8_t*>(datestamp.data()), datestamp.size()));
  CHECK_EQ(1, HMAC_Final(hmac, sign, &len));

  CHECK_EQ(1, HMAC_Init_ex(hmac, sign_key.data(), sign_key.size(), EVP_sha256(), NULL));
  CHECK_EQ(1, HMAC_Update(hmac, reinterpret_cast<const uint8_t*>(region.data()), region.size()));
  CHECK_EQ(1, HMAC_Final(hmac, sign, &len));

  CHECK_EQ(1, HMAC_Init_ex(hmac, sign_key.data(), sign_key.size(), EVP_sha256(), NULL));
  CHECK_EQ(1, HMAC_Update(hmac, reinterpret_cast<const uint8_t*>(service.data()), service.size()));
  CHECK_EQ(1, HMAC_Final(hmac, sign, &len));

  const char* sr = "aws4_request";
  CHECK_EQ(1, HMAC_Init_ex(hmac, sign_key.data(), sign_key.size(), EVP_sha256(), NULL));
  CHECK_EQ(1, HMAC_Update(hmac, reinterpret_cast<const uint8_t*>(sr), strlen(sr)));
  CHECK_EQ(1, HMAC_Final(hmac, sign, &len));

  return string(sign_key);
}

inline std::string_view std_sv(const ::boost::beast::string_view s) {
  return std::string_view{s.data(), s.size()};
}

constexpr char kAlgo[] = "AWS4-HMAC-SHA256";

// Try reading AwsConnectionData from env.
std::optional<AwsConnectionData> GetConnectionDataFromEnv() {
  const char* access_key = getenv("AWS_ACCESS_KEY_ID");
  const char* secret_key = getenv("AWS_SECRET_ACCESS_KEY");
  const char* session_token = getenv("AWS_SESSION_TOKEN");
  const char* region = getenv("AWS_REGION");

  if (access_key && secret_key) {
    AwsConnectionData cd;
    cd.access_key = access_key;
    cd.secret_key = secret_key;
    if (session_token)
      cd.session_token = session_token;
    if (region)
      cd.region = region;
    return cd;
  }

  return std::nullopt;
}

// Get path from ENV if env_var is set or default path relative to user home.
std::optional<std::string> GetAlternativePath(std::string_view default_home_postfix,
                                              const char* env_var) {
  const char* path_override = getenv(env_var);
  if (path_override)
    return path_override;

  const char* home_folder = getenv("HOME");
  if (!home_folder)
    return std::nullopt;

  return absl::StrCat(home_folder, default_home_postfix);
}

std::optional<io::ini::Contents> ReadIniFile(std::string_view full_path) {
  auto file = io::OpenRead(full_path, io::ReadonlyFile::Options{});
  if (!file)
    return std::nullopt;

  io::FileSource file_source{file.value()};
  auto contents = ::io::ini::Parse(&file_source, Ownership::DO_NOT_TAKE_OWNERSHIP);
  if (!contents) {
    LOG(ERROR) << "Failed to parse ini file:" << full_path;
    return std::nullopt;
  }

  return contents.value();
}

// Try filling AwsConnectionData with data from config file.
void GetConfigFromFile(const char* profile, AwsConnectionData* cd) {
  auto full_path = GetAlternativePath("/.aws/config", "AWS_CONFIG_FILE");
  if (!full_path)
    return;

  auto contents = ReadIniFile(*full_path);
  if (!contents)
    return;

  auto it = contents->find(profile);
  if (it != contents->end()) {
    cd->region = it->second["region"];
  }
}

// Try reading AwsConnectionData from credentials file.
std::optional<AwsConnectionData> GetConnectionDataFromFile() {
  // Get credentials path.
  auto full_path = GetAlternativePath("/.aws/credentials", "AWS_SHARED_CREDENTIALS_FILE");
  if (!full_path)
    return std::nullopt;

  // Read credentials file.
  auto contents = ReadIniFile(*full_path);
  if (!contents)
    return std::nullopt;

  // Read profile data.
  const char* profile = getenv("AWS_PROFILE");
  if (profile == nullptr)
    profile = "default";

  auto it = contents->find(profile);
  if (it != contents->end()) {
    AwsConnectionData cd;
    cd.access_key = it->second["aws_access_key_id"];
    cd.secret_key = it->second["aws_secret_access_key"];
    cd.session_token = it->second["aws_session_token"];
    GetConfigFromFile(profile, &cd);
    return cd;
  }

  if (profile != "default"sv) {
    LOG(ERROR) << "Failed to find profile:" << profile << " in credentials";
  }
  return std::nullopt;
}

// Make simple GET request on path and return body.
std::optional<std::string> MakeGetRequest(boost::string_view path, http::Client* http_client) {
  h2::request<h2::empty_body> req{h2::verb::get, path, 11};
  h2::response<h2::string_body> resp;
  req.set(h2::field::host, http_client->host());

  std::error_code ec = http_client->Send(req, &resp);
  if (ec || resp.result() != h2::status::ok)
    return std::nullopt;

  return resp.body();
}

void GetConfigFromMetadata(http::Client* http_client, AwsConnectionData* cd) {
  const char* PATH = "/latest/dynamic/instance-identity/document";

  auto resp = MakeGetRequest(PATH, http_client);
  if (!resp)
    return;

  rapidjson::Document doc;
  doc.Parse(resp->c_str());
  if (doc.HasMember("region")) {
    cd->region = doc["region"].GetString();
  }
}

// Try getting AwsConnectionData from instance metadata.
std::optional<AwsConnectionData> GetConnectionDataFromMetadata(
    std::string_view hinted_role_name = ""sv) {
  ProactorBase* pb = ProactorBase::me();
  CHECK(pb);

  http::Client http_client{pb};
  error_code ec = http_client.Connect("169.254.169.254", "80");
  if (ec)
    return std::nullopt;

  const char* PATH = "/latest/meta-data/iam/security-credentials/";

  // Get role name if none provided.
  std::string role_name{hinted_role_name};
  if (role_name.empty()) {
    auto fetched_role = MakeGetRequest(PATH, &http_client);
    if (!fetched_role) {
      LOG(ERROR) << "Failed to get role name from metadata";
      return std::nullopt;
    }
    role_name = std::move(*fetched_role);
  }

  // Get credentials.
  std::string path = absl::StrCat(PATH, role_name);
  auto resp = MakeGetRequest(path, &http_client);
  if (!resp)
    return std::nullopt;

  rapidjson::Document doc;
  doc.Parse(resp->c_str());
  if (!doc.HasMember("AccessKeyId") || !doc.HasMember("SecretAccessKey"))
    return std::nullopt;

  AwsConnectionData cd;
  cd.access_key = doc["AccessKeyId"].GetString();
  cd.secret_key = doc["SecretAccessKey"].GetString();
  if (doc.HasMember("Token")) {
    cd.session_token = doc["Token"].GetString();
  }
  cd.role_name = role_name;
  GetConfigFromMetadata(&http_client, &cd);

  return cd;
}

AwsConnectionData GetConnectionData() {
  std::optional<AwsConnectionData> keys;

  keys = GetConnectionDataFromEnv();
  if (keys)
    return *keys;

  keys = GetConnectionDataFromFile();
  if (keys)
    return *keys;

  keys = GetConnectionDataFromMetadata();
  if (keys)
    return *keys;

  LOG(ERROR) << "Failed to find valid source for AWS connection data";
  return {};
}

void PopulateAwsConnectionData(const AwsConnectionData& src, AwsConnectionData* dest) {
  // don't overwrite region as it can be provided as a flag.
  std::string region = dest->region;

  *dest = src;

  if (!region.empty())
    dest->region = region;
}

// Return true if the response indicates an expired token.
bool IsExpiredSessionResponse(const h2::response<h2::string_body>& resp) {
  return resp.result() == h2::status::bad_request &&
         resp.body().find(kExpiredTokenSentinel) != std::string::npos;
}

}  // namespace

const char AWS::kEmptySig[] = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const char AWS::kUnsignedPayloadSig[] = "UNSIGNED-PAYLOAD";

string AWS::AuthHeader(string_view method, string_view headers, string_view target,
                       string_view content_sha256, string_view amz_date) const {
  CHECK(!connection_data_.access_key.empty());

  size_t pos = target.find('?');
  string_view url = target.substr(0, pos);
  string_view query_string;
  string canonical_querystring;

  if (pos != string::npos) {
    query_string = target.substr(pos + 1);

    // We must sign query string with params in alphabetical order
    vector<string_view> params = absl::StrSplit(query_string, "&", absl::SkipWhitespace{});
    sort(params.begin(), params.end());
    canonical_querystring = absl::StrJoin(params, "&");
  }

  string canonical_request = absl::StrCat(method, "\n", url, "\n", canonical_querystring, "\n");
  string signed_headers = "host;x-amz-content-sha256;x-amz-date";
  if (!connection_data_.session_token.empty()) {
    signed_headers += ";x-amz-security-token";
  }

  absl::StrAppend(&canonical_request, headers, "\n", signed_headers, "\n", content_sha256);
  VLOG(1) << "CanonicalRequest:\n" << canonical_request << "\n-------------------\n";

  char hexdigest[65];
  Sha256String(canonical_request, hexdigest);

  string string_to_sign =
      absl::StrCat(kAlgo, "\n", amz_date, "\n", credential_scope_, "\n", hexdigest);

  uint8_t signature[32];
  HMAC(sign_key_, string_to_sign, signature);
  Hexify(signature, sizeof(signature), hexdigest);

  string authorization_header =
      absl::StrCat(kAlgo, " Credential=", connection_data_.access_key, "/", credential_scope_,
                   ",SignedHeaders=", signed_headers, ",Signature=", hexdigest);

  return authorization_header;
}

void AWS::Sign(std::string_view payload_sig, HttpHeader* header) const {
  const absl::TimeZone utc_tz = absl::UTCTimeZone();

  // We show consider to pass it via argument to make the function test-friendly.
  // Must be recent (upto 900sec skew is allowed vs amazon servers).
  absl::Time tn = absl::Now();

  string amz_date = absl::FormatTime("%Y%m%dT%H%M00Z", tn, utc_tz);
  header->set("x-amz-date", amz_date);

  // older beast versions require passing beast::string_view
  header->set("x-amz-content-sha256",
              boost::beast::string_view{payload_sig.data(), payload_sig.size()});

  const std::string& session_token = connection_data_.session_token;
  if (!session_token.empty()) {
    header->set("x-amz-security-token", session_token);
  }

  /// The Canonical headers must include the following:
  ///
  ///   - HTTP host header.
  ///   - If the Content-Type header is present in the request, you must add it as well
  ///   - Any x-amz-* headers that you plan to include in your request must also be added.
  ///     For example, if you are using temporary security credentials, you need
  ///     to include x-amz-security-token in your request and add it to the canonical header list.

  // TODO: right now I hardcoded the list but if we need more flexible headers,
  // this code much change.
  string canonical_headers =
      absl::StrCat("host", ":", std_sv(header->find(h2::field::host)->value()), "\n");
  absl::StrAppend(&canonical_headers, "x-amz-content-sha256", ":", payload_sig, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-date", ":", amz_date, "\n");
  if (!session_token.empty()) {
    absl::StrAppend(&canonical_headers, "x-amz-security-token", ":", session_token, "\n");
  }

  string auth_header = AuthHeader(std_sv(header->method_string()), canonical_headers,
                                  std_sv(header->target()), payload_sig, amz_date);

  header->set(h2::field::authorization, auth_header);
}

std::error_code AWS::SendRequest(std::string_view payload_sig, http::Client* client,
                                 h2::request<h2::empty_body>* req,
                                 h2::response<h2::string_body>* resp) {
  Sign(payload_sig, req);

  VLOG(1) << "Req: " << *req;
  auto ec = client->Send(*req, resp);

  // Check if session_token expired on an aws connection, established via instance metadata.
  // In this case, try to update it, re-sign the request and re-try it.
  if (!ec && !connection_data_.role_name.empty() && IsExpiredSessionResponse(*resp)) {
    VLOG(1) << "Trying to update expired session token";

    auto updated_data = GetConnectionDataFromMetadata(connection_data_.role_name);
    if (!updated_data)
      return ec;

    PopulateAwsConnectionData(std::move(*updated_data), &connection_data_);
    SetScopeAndSignKey();

    // Re-connect client if needed.
    if ((*resp)[h2::field::connection] == "close") {
      auto ec = client->Connect(client->host(), "80");
      if (ec)
        return ec;
    }

    Sign(payload_sig, req);
    *resp = h2::response<h2::string_body>{};
    VLOG(1) << "Req: " << *req;
    return client->Send(*req, resp);
  }

  return ec;
}

error_code AWS::Init() {
  PopulateAwsConnectionData(GetConnectionData(), &connection_data_);

  if (connection_data_.access_key.empty()) {
    LOG(WARNING) << "Can not find AWS_ACCESS_KEY_ID";
    return make_error_code(errc::operation_not_permitted);
  }

  if (connection_data_.secret_key.empty()) {
    LOG(WARNING) << "Can not find AWS_SECRET_ACCESS_KEY";
    return make_error_code(errc::operation_not_permitted);
  }

  const absl::TimeZone utc_tz = absl::UTCTimeZone();

  // Must be recent (upto 900sec skew is allowed vs amazon servers).
  absl::Time tn = absl::Now();
  string amz_date = absl::FormatTime("%Y%m%d", tn, utc_tz);
  strcpy(date_str_, amz_date.c_str());

  SetScopeAndSignKey();

  return error_code{};
}

void AWS::UpdateRegion(std::string_view region) {
  connection_data_.region = region;
  SetScopeAndSignKey();
}

void AWS::SetScopeAndSignKey() {
  sign_key_ =
      DeriveSigKey(connection_data_.secret_key, date_str_, connection_data_.region, service_);
  credential_scope_ =
      absl::StrCat(date_str_, "/", connection_data_.region, "/", service_, "/", "aws4_request");
}

}  // namespace cloud
}  // namespace util

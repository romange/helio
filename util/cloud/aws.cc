// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/aws.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <rapidjson/document.h>
#include <absl/time/clock.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>

#include <optional>

#include "base/logging.h"
#include "util/http/http_client.h"
#include "util/proactor_base.h"
#include "io/file.h"
#include "io/line_reader.h"

namespace util {
namespace cloud {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

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

struct AwsKeys {
  std::string access_key, secret_key, session_token;
};

// Try reading AwsKeys from env.
std::optional<AwsKeys> GetKeysFromEnv() {
  const char* access_key = getenv("AWS_ACCESS_KEY_ID");
  const char* secret_key = getenv("AWS_SECRET_ACCESS_KEY");
  const char* session_token = getenv("AWS_SESSION_TOKEN");
  if (access_key && secret_key) {
    AwsKeys keys;
    keys.access_key = access_key;
    keys.secret_key = secret_key;
    if (session_token) {
      keys.session_token = session_token;
    }
    return keys;
  }
  return std::nullopt;
}

// Try reading AwsKeys from credentials file.
std::optional<AwsKeys> GetKeysFromFile() {
  const char* home_folder = getenv("HOME");
  if (!home_folder)
    return std::nullopt;

  // Open credentials file.
  const char* path_override = getenv("AWS_SHARED_CREDENTIALS_FILE");
  std::string full_path;
  if (path_override != nullptr) {
    full_path = path_override;
  } else {
    full_path = absl::StrCat(home_folder, "/.aws/credentials");
  }
  auto file = io::OpenRead(full_path, io::ReadonlyFile::Options{});
  if (!file)
    return std::nullopt;

  io::FileSource file_source{file.value()};
  auto contents = ::io::ini::Parse(&file_source, Ownership::DO_NOT_TAKE_OWNERSHIP);
  if (!contents)
    return std::nullopt;

  // Read profile data.
  const char* profile = getenv("AWS_PROFILE");
  auto it = contents.value().find(profile != nullptr ? profile : "default");
  if (it != contents.value().end()) {
    AwsKeys keys;
    keys.access_key = it->second["aws_access_key_id"];
    keys.secret_key = it->second["aws_secret_access_key"];
    keys.session_token = it->second["aws_session_token"];
    return keys;
  }
  return std::nullopt;
}

// Try getting AwsKeys from instance metadata.
std::optional<AwsKeys> GetKeysFromMetadata() {
  error_code ec;
  ProactorBase* pb = ProactorBase::me();
  CHECK(pb);

  // TODO: Replace mock path with real. Make mockable path?
  http::Client http_client{pb};
  ec = http_client.Connect("127.0.0.1", "1338");
  if (ec)
    return std::nullopt;

  // TODO: fix role name
  const char* PATH = "/latest/meta-data/iam/security-credentials/baskinc-role";
  h2::request<h2::empty_body> req{h2::verb::get, PATH, 11};
  h2::response<h2::string_body> resp;
  req.set(h2::field::host, http_client.host());

  ec = http_client.Send(req, &resp);
  if (ec || resp.result() != h2::status::ok)
    return std::nullopt;

  rapidjson::Document doc;
  doc.Parse(resp.body().c_str());

  if (doc.HasMember("AccessKeyId") && doc.HasMember("SecretAccessKey")) {
    AwsKeys keys;
    keys.access_key = doc["AccessKeyId"].GetString();
    keys.secret_key = doc["SecretAccessKey"].GetString();
    if (doc.HasMember("Token")) {
      keys.session_token = doc["Token"].GetString();
    }
    return keys;
  }

  return std::nullopt;
}

AwsKeys GetKeys() {
  std::optional<AwsKeys> keys;

  keys = GetKeysFromEnv();
  if (keys)
    return *keys;

  keys = GetKeysFromFile();
  if (keys)
    return *keys;

  keys = GetKeysFromMetadata();
  if (keys)
    return *keys;

  return {};
}

}  // namespace

const char AWS::kEmptySig[] = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const char AWS::kUnsignedPayloadSig[] = "UNSIGNED-PAYLOAD";

string AWS::AuthHeader(string_view method, string_view headers, string_view target,
                       string_view content_sha256, string_view amz_date) const {
  CHECK(!access_key_.empty());

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
  if (!session_token_.empty()) {
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
      absl::StrCat(kAlgo, " Credential=", access_key_, "/", credential_scope_,
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

  if (!session_token_.empty()) {
    header->set("x-amz-security-token", session_token_);
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
  if (!session_token_.empty()) {
    absl::StrAppend(&canonical_headers, "x-amz-security-token", ":", session_token_, "\n");
  }

  string auth_header = AuthHeader(std_sv(header->method_string()), canonical_headers,
                                  std_sv(header->target()), payload_sig, amz_date);

  header->set(h2::field::authorization, auth_header);
}

error_code AWS::Init() {
  AwsKeys keys = GetKeys();

  if (keys.access_key.empty()) {
    LOG(WARNING) << "Can not find AWS_ACCESS_KEY_ID";
    return make_error_code(errc::operation_not_permitted);
  }

  if (keys.secret_key.empty()) {
    LOG(WARNING) << "Can not find AWS_SECRET_ACCESS_KEY";
    return make_error_code(errc::operation_not_permitted);
  }

  secret_ = std::move(keys.secret_key);
  access_key_ = std::move(keys.access_key);
  session_token_ = std::move(keys.session_token);

  const absl::TimeZone utc_tz = absl::UTCTimeZone();

  // Must be recent (upto 900sec skew is allowed vs amazon servers).
  absl::Time tn = absl::Now();
  string amz_date = absl::FormatTime("%Y%m%d", tn, utc_tz);
  strcpy(date_str_, amz_date.c_str());

  SetScopeAndSignKey();

  return error_code{};
}

void AWS::UpdateRegion(std::string_view region) {
  region_ = region;
  SetScopeAndSignKey();
}

void AWS::SetScopeAndSignKey() {
  sign_key_ = DeriveSigKey(secret_, date_str_, region_, service_);
  credential_scope_ = absl::StrCat(date_str_, "/", region_, "/", service_, "/", "aws4_request");
}

}  // namespace cloud
}  // namespace util

// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/fields.hpp>
#include <boost/beast/http/string_body.hpp>
#include <system_error>

#include "util/http/http_client.h"

namespace util {
namespace cloud {

namespace h2 = boost::beast::http;

struct AwsConnectionData {
  std::string access_key, secret_key, session_token;
  std::string region, role_name;
};

class AwsSignKey {
 public:
  using HttpHeader = ::boost::beast::http::header<true, ::boost::beast::http::fields>;

  AwsSignKey() = default;

  AwsSignKey(std::string sign_key, std::string credential_scope, AwsConnectionData connection_data)
      : sign_key_(std::move(sign_key)), credential_scope_(std::move(credential_scope)),
        connection_data_(std::move(connection_data)) {
  }

  void Sign(std::string_view payload_sig, HttpHeader* header) const;

  const AwsConnectionData& connection_data() const {
    return connection_data_;
  }

 private:
  struct SignHeaders {
    std::string_view method, headers, target;
    std::string_view content_sha256, amz_date;
  };

  std::string AuthHeader(const SignHeaders& headers) const;

  std::string sign_key_, credential_scope_;
  AwsConnectionData connection_data_;
};

class AWS {
 public:
  static const char kEmptySig[];
  static const char kUnsignedPayloadSig[];
  using HttpParser = ::boost::beast::http::response_parser<::boost::beast::http::buffer_body>;
  using EmptyBodyReq = ::boost::beast::http::request<::boost::beast::http::empty_body>;

  AWS(const std::string& service, const std::string& region = "") : service_(service) {
    connection_data_.region = region;
  }

  std::error_code Init();

  const AwsConnectionData& connection_data() const {
    return connection_data_;
  }

  // Returns true if succeeded to refresh the metadata.
  bool RefreshToken();

  AwsSignKey GetSignKey(std::string_view region) const;

  std::error_code SendRequest(
      http::Client* client, AwsSignKey* cached_key, EmptyBodyReq* req,
      ::boost::beast::http::response<::boost::beast::http::string_body>* resp);

  // Sends a request and reads back header response. Handles the response according to the header.
  // The caller is responsible to read the rest of the response via parser.
  std::error_code Handshake(http::Client* client, AwsSignKey* cached_key, EmptyBodyReq* req,
                            HttpParser* resp);

 private:
  std::error_code RetryExpired(http::Client* client, AwsSignKey* cached_key, EmptyBodyReq* req,
                               ::boost::beast::http::response_header<>* header);

  std::string service_;
  AwsConnectionData connection_data_;

  char date_str_[32];
};

}  // namespace cloud

}  // namespace util

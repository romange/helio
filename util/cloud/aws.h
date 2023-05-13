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

#include "util/fibers/synchronization.h"

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

  AwsSignKey(std::string_view service, AwsConnectionData connection_data);

  void Sign(std::string_view payload_sig, HttpHeader* header) const;

  const AwsConnectionData& connection_data() const {
    return connection_data_;
  }

  time_t now() const {
    return now_;
  }

 private:

  void RefreshIfNeeded() const;

  struct SignHeaders {
    std::string_view method, headers, target;
    std::string_view content_sha256, amz_date;
  };

  std::string AuthHeader(const SignHeaders& headers) const;

  std::string service_;
  mutable std::string sign_key_, credential_scope_;
  mutable time_t now_;  // epoch time.

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
  // Thread-safe.
  bool RefreshToken() const;

  AwsSignKey GetSignKey(std::string_view region) const;

  std::error_code SendRequest(
      http::Client* client, AwsSignKey* cached_key, EmptyBodyReq* req,
      ::boost::beast::http::response<::boost::beast::http::string_body>* resp) const;

  // Sends a request and reads back header response. Handles the response according to the header.
  // The caller is responsible to read the rest of the response via parser.
  std::error_code Handshake(http::Client* client, AwsSignKey* cached_key, EmptyBodyReq* req,
                            HttpParser* resp) const;

 private:
  std::error_code RetryExpired(http::Client* client, AwsSignKey* cached_key, EmptyBodyReq* req,
                               ::boost::beast::http::response_header<>* header) const;

  std::string service_;
  mutable AwsConnectionData connection_data_;

  mutable fb2::Mutex mu_;
};

}  // namespace cloud

}  // namespace util

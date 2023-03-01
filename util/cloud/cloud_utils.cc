// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/cloud_utils.h"

#include "util/cloud/aws.h"

using namespace std;
namespace h2 = boost::beast::http;

namespace util {
namespace cloud {

namespace {

const char* kExpiredTokenSentinel = "<Error><Code>ExpiredToken</Code>";

// Return true if the response indicates an expired token.
bool IsExpiredBody(string_view body) {
  return body.find(kExpiredTokenSentinel) != std::string::npos;
}

}  // namespace

error_code SendRequest(AWS* aws, http::Client* client, h2::request<h2::empty_body>* req,
                       h2::response<h2::string_body>* resp) {
  aws->Sign(AWS::kEmptySig, req);
  error_code ec = client->Send(*req);
  if (ec)
    return ec;

  ec = client->Recv(resp);
  if (ec)
    return ec;

  if (resp->result() == h2::status::bad_request && IsExpiredBody(resp->body())) {
    aws->RefreshToken();

    // Re-connect client if needed.
    if ((*resp)[h2::field::connection] == "close") {
      ec = client->Reconnect();
      if (ec)
        return ec;
    }

    aws->Sign(AWS::kEmptySig, req);
    ec = client->Send(*req);
    if (ec)
      return ec;

    resp->clear();
    ec = client->Recv(resp);
    if (ec)
      return ec;
  }
  return ec;
}

error_code SendRequest(AWS* aws, http::Client* client, h2::request<h2::empty_body>* req,
                       HttpParser* parser) {
  aws->Sign(AWS::kEmptySig, req);
  error_code ec = client->Send(*req);
  if (ec)
    return ec;

  parser->body_limit(UINT64_MAX);

  ec = client->ReadHeader(parser);
  if (ec)
    return ec;

  auto& msg = parser->get();

  if (msg.result() == h2::status::bad_request) {
    string str(512, '\0');
    msg.body().data = str.data();
    msg.body().size = str.size();
    ec = client->Recv(parser);
    if (ec)
      return ec;

    if (IsExpiredBody(str)) {
      aws->RefreshToken();

      // Re-connect client if needed.
      if (msg[h2::field::connection] == "close") {
        ec = client->Reconnect();
        if (ec)
          return ec;
      }

      aws->Sign(AWS::kEmptySig, req);
      ec = client->Send(*req);
      if (ec)
        return ec;

      // TODO: seems we can not reuse the parser here.
      // (*parser) = std::move(HttpParser{});
      parser->body_limit(UINT64_MAX);
      ec = client->ReadHeader(parser);
      if (ec)
        return ec;
    }
  }

  return ec;
}

}  // namespace cloud
}  // namespace util
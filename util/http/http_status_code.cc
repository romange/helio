// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "http_status_code.h"
#include "base/logging.h"

namespace http {

const char* StatusStringFromCode(HttpStatusCode code) {
  switch (code) {
    case HTTP_OK: return "200 OK";
    case HTTP_ACCEPTED: return"202 Accepted";
    case HTTP_NO_CONTENT: return"204 No Content";
    case HTTP_BAD_REQUEST: return "400 Bad Request";
    case HTTP_UNAUTHORIZED: return "401 Unauthorized";
    case HTTP_FORBIDDEN: return "403 Forbidden";
    case HTTP_NOT_FOUND: return "404 Not Found";
    default:
      LOG(FATAL) << "Not implemented " << code;
  }
  return nullptr;
}
}  // namespace http
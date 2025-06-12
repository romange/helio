// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/utils.h"

#include <boost/beast/http/string_body.hpp>

#include "base/logging.h"

namespace util::cloud {

using namespace std;
namespace h2 = boost::beast::http;

namespace {

constexpr auto kResumeIncomplete = h2::status::permanent_redirect;

bool IsUnauthorized(const h2::header<false, h2::fields>& resp) {
  if (resp.result() != h2::status::unauthorized) {
    return false;
  }
  auto it = resp.find("WWW-Authenticate");

  return it != resp.end();
}

inline bool DoesServerPushback(h2::status st) {
  return st == h2::status::too_many_requests ||
         h2::to_status_class(st) == h2::status_class::server_error;
}

bool IsResponseOK(h2::status st) {
  // Partial content can appear because of the previous reconnect.
  // For multipart uploads kResumeIncomplete can be returned.
  return st == h2::status::ok || st == h2::status::partial_content || st == kResumeIncomplete ||
         st == h2::status::created /* returned by azure blob upload */;
}

}  // namespace
namespace detail {

EmptyRequestImpl::EmptyRequestImpl(h2::verb req_verb, std::string_view url)
    : req_{req_verb, boost::beast::string_view{url.data(), url.size()}, 11} {
}

std::error_code EmptyRequestImpl::Send(http::Client* client) {
  return client->Send(req_);
}

std::error_code DynamicBodyRequestImpl::Send(http::Client* client) {
  return client->Send(req_);
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

io::Result<size_t> AbstractStorageFile::WriteSome(const iovec* v, uint32_t len) {
  size_t total = 0;
  for (uint32_t i = 0; i < len; ++i) {
    RETURN_UNEXPECTED(FillBuf(reinterpret_cast<const uint8_t*>(v->iov_base), v->iov_len));
    total += v->iov_len;
  }
  return total;
}

error_code AbstractStorageFile::FillBuf(const uint8_t* buffer, size_t length) {
  while (length >= body_mb_.max_size() - body_mb_.size()) {
    size_t prepare_size = body_mb_.max_size() - body_mb_.size();
    auto mbs = body_mb_.prepare(prepare_size);
    size_t offs = 0;
    for (auto mb : mbs) {
      memcpy(mb.data(), buffer + offs, mb.size());
      offs += mb.size();
    }
    DCHECK_EQ(offs, prepare_size);
    body_mb_.commit(prepare_size);

    auto ec = Upload();
    if (ec)
      return ec;

    length -= prepare_size;
    buffer += prepare_size;
  }

  if (length) {
    auto mbs = body_mb_.prepare(length);
    for (auto mb : mbs) {
      memcpy(mb.data(), buffer, mb.size());
      buffer += mb.size();
    }
    body_mb_.commit(length);
  }
  return {};
}

}  // namespace detail

RobustSender::RobustSender(http::ClientPool* pool, CredentialsProvider* provider)
    : pool_(pool), provider_(provider) {
}

error_code RobustSender::Send(unsigned num_iterations, detail::HttpRequestBase* req,
                              SenderResult* result) {
  error_code ec;
  for (unsigned i = 0; i < num_iterations; ++i) {  // Iterate for possible token refresh.
    auto res = pool_->GetHandle();
    if (!res)
      return res.error();

    result->client_handle = std::move(res.value());
    auto* client_handle = result->client_handle.get();
    VLOG(1) << "HttpReq " << client_handle->host() << ": " << req->GetHeaders() << ", ["
            << client_handle->native_handle() << "]";

    auto send_err = req->Send(client_handle);
    // Socket that were inactive for longer time can be closed from server side.
    // Writing can return `broken_pipe` so we need to retry.
    if (FiberSocketBase::IsConnClosed(send_err)) {
      VLOG(1) << "Retrying. Connection closed with error: " << send_err.message();
      continue;
    }
    RETURN_ERROR(send_err);
    result->eb_parser.reset(new h2::response_parser<h2::empty_body>());

    // no limit. Prevent from this parser to throw an error due to large body.
    // result->eb_parser->body_limit(boost::optional<uint64_t>());
    auto header_err = client_handle->ReadHeader(result->eb_parser.get());
    // Reading from closed socket can result in `connection_aborted`.
    if (FiberSocketBase::IsConnClosed(header_err)) {
      VLOG(1) << "Retrying. Connection closed with error: " << header_err.message();
      continue;
    }
    // Unfortunately earlier versions of boost (1.74-) have a bug that do not support the body_limit
    // directive above. Therefore, we fix it here.
    if (header_err == h2::error::body_limit) {
      header_err.clear();
    }
    RETURN_ERROR(header_err);
    {
      const auto& msg = result->eb_parser->get();
      VLOG(1) << "RespHeader" << i << ": " << msg;

      if (!result->eb_parser->keep_alive()) {
        LOG(FATAL) << "TBD: Schedule reconnect due to conn-close header";
      }

      if (IsResponseOK(msg.result())) {
        return {};
      }
    }

    // We have some kind of error, possibly with body that needs to be drained.
    h2::response_parser<h2::string_body> drainer(std::move(*result->eb_parser));
    RETURN_ERROR(client_handle->Recv(&drainer));
    const auto& msg = drainer.get();

    if (DoesServerPushback(msg.result())) {
      LOG(INFO) << "Retrying(" << client_handle->native_handle() << ") with " << msg;

      ThisFiber::SleepFor(100ms);
      continue;
    }

    if (IsUnauthorized(msg)) {
      VLOG(1) << "Refreshing token";
      RETURN_ERROR(provider_->RefreshToken());
      provider_->Sign(req);
      continue;
    }

    if (msg.result() == h2::status::forbidden) {
      return make_error_code(errc::operation_not_permitted);
    }

    if (msg.result() == h2::status::not_found) {
      return make_error_code(errc::no_such_file_or_directory);
    }

    ec = make_error_code(errc::bad_message);
    LOG(DFATAL) << "Unexpected response " << msg << "\n" << msg.body() << "\n";
  }

  return ec;
}

}  // namespace util::cloud

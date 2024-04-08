// Create an https client test program
// Note this that example show how to run HTTPS client
// which connect to the remote server, read a resource and print thi
// the response to the LOG.
// To see command line args run with --helpful command line flag
// Since there is no reasonable default host name you must at minimum run
// this with --host <domain name: https://en.wikipedia.org/wiki/Domain_name> such as www.google.com
// (note that you should not add the service name at the start such as https).
// To demo that this is not a blocking HTTPS client this would run 2 fibers for connection
// and another task that basically does nothing.
// output from this is either an error (in case we failed - try given illegal host or port) or a
// the headers and the body from the host and resource you gave.
// for example:
// ./https_client --host redis.io --resource /commands/asking/ --vmodule=https_client=2
// --logbuflevel=-1 --alsologtostderr
// will print the content from https://redis.io/commands/asking command to the log as well as to the
// terminal
#include <openssl/err.h>

#include <boost/beast/http.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include "base/flags.h"
#include "base/init.h"
#include "util/fibers/pool.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/synchronization.h"
#include "util/http/http_client.h"
#include "util/tls/tls_engine.h"
#include "util/tls/tls_socket.h"

using namespace util;
using http::Client;
using http::TlsClient;
namespace h2 = boost::beast::http;
using ResponseType = h2::response<h2::string_body>;
using absl::GetFlag;
using namespace std;

ABSL_FLAG(short, http_port, 443, "HTTPS remote server port.");
ABSL_FLAG(std::string, host, "", "Remote host domain name");
ABSL_FLAG(std::string, resource, "/", "resource path on the remote host");

struct RequestResults {
  unsigned int status = 400;
  std::optional<ResponseType> result;

  RequestResults() = default;

  RequestResults(unsigned int s, ResponseType&& res) : status{s}, result{std::move(res)} {
  }

  bool good() const {
    return status < 400;
  }

  bool has_result() const {
    return result != std::nullopt;
  }

  const ResponseType& get() const {
    assert(good() && has_result());
    return result.value();
  }
};

std::ostream& operator<<(std::ostream& os, const RequestResults& res) {
  if (res.has_result()) {
    if (res.good()) {
      const auto& msg = res.get();
      os << "headers: " << msg.base();
      return os << "body: " << msg.body();
    } else {
      return os << "got errors results: " << res.result.value();
    }
  } else {
    return os << "no result were successfully read from remote host";
  }
}

using OpResult = io::Result<RequestResults, std::string>;

OpResult ConnectAndRead(ProactorBase* proactor, std::string_view host, std::string_view service,
                        const char* resource, SSL_CTX* ssl_ctx) {
  
  auto list_res = proactor->Await([&] {
    TlsClient http_client{proactor};

    http_client.set_connect_timeout_ms(2000);
    if (auto ec = http_client.Connect(host, service, ssl_ctx); !ec) {
      h2::request<h2::string_body> req{h2::verb::get, resource, 11 /*http 1.1*/};
      req.set(h2::field::host, std::string(host).c_str());
      ResponseType res;
      if (auto ec = http_client.Send(req, &res); !ec) {
        return OpResult{RequestResults{res.result_int(), std::move(res)}};
      } else {
        return OpResult{
            nonstd::unexpected<std::string>("Failed HTTPS GET Send(): " + ec.message())};
      }
    } else {
      return OpResult{nonstd::unexpected<std::string>("Failed to connect: " + ec.message())};
    }
    return OpResult{RequestResults{}};
  });

  return list_res;
}

using namespace std::chrono_literals;

fb2::Fiber ReadFromRemoteHost(ProactorPool* pool, SSL_CTX* ssl_context, std::string_view task_name,
                              fb2::Done* done_condition) {
  std::string service = std::to_string(GetFlag(FLAGS_http_port));
  std::string host = GetFlag(FLAGS_host);
  std::string resource = GetFlag(FLAGS_resource);

  LOG(INFO) << task_name << ": host '" << host << "', port " << service << ", resource '"
            << resource << "'";
  return pool->GetNextProactor()->LaunchFiber([=]() {
    while (true) {
      if (done_condition->WaitFor(1s)) {
        VLOG(1) << task_name << ": finish running HTTPS task";
        return;
      }
      auto pb = pool->GetNextProactor();

      auto results = ConnectAndRead(pb, host, service, resource.c_str(), ssl_context);
      if (results.has_value()) {
        LOG(INFO) << task_name << ": successfully read";
        LOG(INFO) << task_name << ": " << results.value();
      } else {
        LOG(WARNING) << task_name << ": Failed for '" << host << "':" << service << "/" << resource
                     << ", error: '" << results.error() << "'";
      }
    }
  });
}

fb2::Fiber IdleFiberRun(ProactorPool* pool, std::string_view task_name, fb2::Done* done_condition) {
  return pool->GetNextProactor()->LaunchFiber([=]() {
    while (true) {
      LOG(INFO) << task_name << ": just burning CPU";
      if (done_condition->WaitFor(100ms)) {
        VLOG(1) << task_name << ": finish running with idle task";
        return;
      }
    }
  });
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  std::string host = GetFlag(FLAGS_host);
  if (host.empty()) {
    LOG(ERROR) << "Host name is missing - please run with --host <host name>";
    return -1;
  }

  SSL_library_init();
  SSL_load_error_strings();

#ifdef __linux__
  unique_ptr<util::ProactorPool> pp(fb2::Pool::IOUring(128));
#else
  unique_ptr<util::ProactorPool> pp(fb2::Pool::Epoll());
#endif

  pp->Run();

  SSL_CTX* ssl_context = TlsClient::CreateSslContext();
  CHECK(ssl_context) << " failed to create SSL context";
  VLOG(1) << "starting the fibers that test the connection";
  fb2::Done done_condition;
  auto fiber = ReadFromRemoteHost(pp.get(), ssl_context, "task 1", &done_condition);
  auto fiber2 = ReadFromRemoteHost(pp.get(), ssl_context, "task 2", &done_condition);
  auto idle_fiber = IdleFiberRun(pp.get(), "task 3", &done_condition);

  // Let the background tasks run for a while
  for (int i = 0; i < 3; i++) {
    this_thread::sleep_for(1s);
  }
  done_condition.Notify();
  fiber.Join();
  fiber2.Join();
  idle_fiber.Join();
  LOG(INFO) << "finish running the test";
  TlsClient::FreeContext(ssl_context);
}

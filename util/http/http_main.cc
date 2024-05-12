// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>
#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <mimalloc-new-delete.h>

#include <boost/beast/http/span_body.hpp>

#include "base/init.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"
#include "util/html/sorted_table.h"
#include "util/http/http_common.h"
#include "util/http/http_handler.h"
#include "util/metrics/metrics.h"
#include "util/varz.h"


using namespace std;
using namespace util;
using absl::GetFlag;
namespace h2 = boost::beast::http;

ABSL_FLAG(uint32_t, port, 8080, "Port number.");
ABSL_FLAG(bool, use_incoming_cpu, false,
          "If true uses incoming cpu of a socket in order to distribute incoming connections");
ABSL_FLAG(string, password, "", "Protect the web interface with this password.");
ABSL_FLAG(string, root_resp, "", "If set, overrides root page response");

VarzQps http_qps("bar-qps");
metrics::CounterFamily http_req("http_requests_total", "Number of served http requests");

namespace {
class MyListener : public HttpListener<> {
 public:
  ProactorBase* PickConnectionProactor(FiberSocketBase* sock);
};

ProactorBase* MyListener::PickConnectionProactor(FiberSocketBase* sock) {
#ifdef __linux__
  int fd = sock->native_handle();

  int cpu;
  socklen_t len = sizeof(cpu);

  CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len));
  VLOG(1) << "Got socket from cpu " << cpu;

  bool use_incoming = GetFlag(FLAGS_use_incoming_cpu);
  if (use_incoming) {
    vector<unsigned> ids = pool()->MapCpuToThreads(cpu);
    if (!ids.empty()) {
      return pool()->at(ids.front());
    }
  }
#endif

  return pool()->GetNextProactor();
}

void ServerRun(ProactorPool* pool) {
  AcceptServer server(pool);
  http_req.Init(pool, {"type", "handle"});

  HttpListener<>* listener = new MyListener;
  std::string pass = GetFlag(FLAGS_password);
  listener->SetAuthFunctor([pass = std::move(pass)](std::string_view path,
                                                    std::string_view username,
                                                    std::string_view password) {
    if (path == "/foo") {
      return true;
    }
    return password == pass && username == "default";
  });
  auto cb = [](const http::QueryArgs& args, HttpContext* send) {
    http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
    resp.body() = "Bar";

    http::SetMime(http::kTextMime, &resp);
    resp.set(h2::field::server, "http_main");
    http_qps.Inc();
    http_req.IncBy({"get", "foo"}, 1);

    return send->Invoke(std::move(resp));
  };

  listener->RegisterCb("/foo", cb);

  static const char kBody[] = R"({"message":"Hello, World!"})";
  using SB = h2::span_body<const char>;
  static h2::response<SB> sb_resp(h2::status::ok, 11);
  sb_resp.body() = boost::beast::span<const char>(kBody, strlen(kBody));
  http::SetMime(http::kJsonMime, &sb_resp);
  sb_resp.set(h2::field::server, "http_main");

  auto json_cb = [](const http::QueryArgs& args, HttpContext* send) {
    auto resp = sb_resp;
    return send->Invoke(std::move(resp));
  };

  listener->RegisterCb("/json", json_cb);

  auto table_cb = [](const http::QueryArgs& args, HttpContext* send) {
    using html::SortedTable;

    auto cell = [](auto i, auto j) { return absl::StrCat("Val", i, "_", j); };

    http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
    resp.body() = SortedTable::HtmlStart();
    SortedTable::StartTable({"Col1", "Col2", "Col3", "Col4"}, &resp.body());
    for (size_t i = 0; i < 300; ++i) {
      SortedTable::Row({cell(1, i), cell(2, i), cell(3, i), cell(4, i)}, &resp.body());
    }
    SortedTable::EndTable(&resp.body());
    http_req.IncBy({"get", "table"}, 1);
    return send->Invoke(std::move(resp));
  };
  listener->RegisterCb("/table", table_cb);

  auto post_cb = [](const http::QueryArgs& args, util::HttpListenerBase::RequestType&& req,
                    HttpContext* send) {
    if (req.method() != h2::verb::post) {
      return send->Invoke(http::MakeStringResponse(h2::status::bad_request));
    }
    http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
    resp.body() = absl::StrCat("Post: ", req.body(), "\r\n");

    http::SetMime(http::kTextMime, &resp);
    resp.set(h2::field::server, "http_main");
    http_qps.Inc();
    http_req.IncBy({"post", "post"}, 1);

    return send->Invoke(std::move(resp));
  };
  listener->RegisterCb("/post", post_cb);

  listener->enable_metrics();
  listener->set_root_response(GetFlag(FLAGS_root_resp));

  uint16_t port = server.AddListener(GetFlag(FLAGS_port), listener);
  LOG(INFO) << "Listening on port " << port;

  server.Run();
  server.Wait();
}

string LabelTuple(const metrics::ObservationDescriptor& od, unsigned label_index,
                  unsigned observ_index) {
  string res;
  for (size_t i = 0; i < od.label_names.size(); ++i) {
    absl::StrAppend(&res, od.label_names[i].name(), "=", od.label_values[label_index][i], ", ");
  }

  if (observ_index < od.adjustments.size()) {
    const auto& a = od.adjustments[observ_index];
    if (!a.label.first.name().empty()) {
      absl::StrAppend(&res, a.label.first.name(), "=", a.label.second);
    }
  }
  return res;
}

void PrintObservation(const metrics::ObservationDescriptor& od, absl::Span<const double> vals) {
  if (od.adjustments.empty()) {
    for (size_t i = 0; i < od.label_values.size(); ++i) {
      LOG(INFO) << od.metric_name << " [" << LabelTuple(od, i, 0) << "] " << vals[i];
    }
  }
};

bool HelpshortFlags(std::string_view f) {
  return absl::EndsWith(f, "proactor_pool.cc") || absl::EndsWith(f, "http_main.cc");
}

}  // namespace

/**
 * Demo http server. Currently registers most handlers under basic authentication layer
 * with user "default" and empty password. The password can be set with flag "--password".
 * The only "open" handler is /foo.
 * There are handlers like "/json" and "/table" that return json and html tables respectively.
 * In addition, it exposes POST handler that prints the contents of its body back.
 *
 */
int main(int argc, char** argv) {
  absl::SetProgramUsageMessage("http example server");
  absl::FlagsUsageConfig config;
  config.contains_helpshort_flags = &HelpshortFlags;
  config.contains_help_flags = &HelpshortFlags;
  absl::SetFlagsUsageConfig(config);

  MainInitGuard guard(&argc, &argv);
  fb2::SetDefaultStackResource(&fb2::std_malloc_resource, 32 * 1024);
  std::unique_ptr<ProactorPool> pool;

#ifdef __linux__
  pool.reset(fb2::Pool::IOUring(256));
#else
  pool.reset(fb2::Pool::Epoll());
#endif

  pool->Run();
  http_qps.Init(pool.get());
  ServerRun(pool.get());

  metrics::Iterate(PrintObservation);
  http_qps.Shutdown();
  pool->Stop();

  LOG(INFO) << "Exiting server...";
  return 0;
}

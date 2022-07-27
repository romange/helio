// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <mimalloc-new-delete.h>

#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>
#include <absl/strings/match.h>
#include <absl/strings/str_join.h>

#include <boost/beast/http/span_body.hpp>

#include "base/init.h"
#include "util/accept_server.h"
#include "util/html/sorted_table.h"
#include "util/http/http_handler.h"
#include "util/metrics/metrics.h"
#include "util/uring/uring_pool.h"
#include "util/varz.h"

ABSL_FLAG(uint32_t, port, 8080, "Port number.");
ABSL_FLAG(bool, use_incoming_cpu, false,
          "If true uses incoming cpu of a socket in order to distribute incoming connections");

using namespace std;
using namespace util;
namespace h2 = boost::beast::http;

VarzQps http_qps("bar-qps");
metrics::CounterFamily http_req("http_requests_total", "Number of served http requests");

class MyListener : public HttpListener<> {
 public:
  ProactorBase* PickConnectionProactor(LinuxSocketBase* sock);
};

ProactorBase* MyListener::PickConnectionProactor(LinuxSocketBase* sock) {
  int fd = sock->native_handle();

  int cpu;
  socklen_t len = sizeof(cpu);

  CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len));
  VLOG(1) << "Got socket from cpu " << cpu;

  bool use_incoming = absl::GetFlag(FLAGS_use_incoming_cpu);
  if (use_incoming) {
    vector<unsigned> ids = pool()->MapCpuToThreads(cpu);
    if (!ids.empty()) {
      return pool()->at(ids.front());
    }
  }
  return pool()->GetNextProactor();
}

void ServerRun(ProactorPool* pool) {
  AcceptServer server(pool);
  http_req.Init(pool, {"type", "handle"});

  HttpListener<>* listener = new MyListener;
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

  static const char kBody[]= R"({"message":"Hello, World!"})";
  using SB = h2::span_body<const char>;
  static h2::response<SB> sb_resp(h2::status::ok, 11);
  sb_resp.body() = boost::beast::span<const char>(kBody, strlen(kBody));
  http::SetMime(http::kJsonMime, &sb_resp);
  sb_resp.set(h2::field::server, "http_main");

  auto json_cb = [](const http::QueryArgs& args, HttpContext* send) {
    auto resp = sb_resp;
    return send->Invoke(move(resp));
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
  listener->enable_metrics();

  uint16_t port = server.AddListener(absl::GetFlag(FLAGS_port), listener);
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


int main(int argc, char** argv) {
  absl::SetProgramUsageMessage("http example server");
  absl::FlagsUsageConfig config;
  config.contains_helpshort_flags = &HelpshortFlags;
  config.contains_help_flags = &HelpshortFlags;
  absl::SetFlagsUsageConfig(config);

  MainInitGuard guard(&argc, &argv);

  uring::UringPool pool;
  pool.Run();
  http_qps.Init(&pool);
  ServerRun(&pool);

  metrics::Iterate(PrintObservation);
  http_qps.Shutdown();
  pool.Stop();

  LOG(INFO) << "Exiting server...";
  return 0;
}

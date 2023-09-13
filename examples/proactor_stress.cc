// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/hash.h"
#include "base/histogram.h"
#include "base/init.cc"
#include "base/logging.h"
#include "util/accept_server.h"
#include "util/http/http_handler.h"
#include "util/fibers/pool.h"

DEFINE_uint32(c, 10, "Number of connections per thread");
DEFINE_uint32(n, 500000, "num requests");
DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_bool(send_myself, false, "");

using namespace util;
using namespace std;
using uring::UringPool;

atomic_bool finish_run{false};

/* To demonstrate the difference in enqueing, run-patterms:
 time ./proactor_stress -n 15000 --send_myself={true,false}   --proactor_threads=4 \
       --vmodule=proactor=1
 With --send_myself=true the run-time is much shorter with
 less cycles spent on proactor spinning.

 You may need to comment out the line in AwaitBrief that runs the function inline.
****/
class Driver {
  Driver(const Driver&) = delete;

 public:
  Driver(ProactorPool* pool);
  ~Driver();

  void Wait();
  void Run();

  const base::Histogram& hist() const {
    return hist_;
  }

 private:
  void FiberRun(unsigned index);

  ProactorPool* pool_;
  vector<fiber> fibers_;
  base::Histogram hist_;
};

Driver::Driver(ProactorPool* pool) : pool_(pool) {
  fibers_.resize(FLAGS_c);
}

Driver::~Driver() {
}

void Driver::Wait() {
  for (auto& f : fibers_)
    f.join();
}

void Driver::Run() {
  for (size_t i = 0; i < fibers_.size(); ++i) {
    fibers_[i] = fiber([this, i] { this->FiberRun(i); });
  }
}

void Driver::FiberRun(unsigned index) {
  static thread_local size_t var = 0;

  unsigned my_index = ProactorBase::GetIndex();

  auto cb = [] { var++; };
  size_t end = FLAGS_n;
  size_t last_cnt = 0;
  (void)last_cnt;
  static thread_local unsigned last_fiber = 0;
  static thread_local unsigned last_fiber_cnt = 0;

  for (size_t i = 0; i < end && !finish_run.load(memory_order_relaxed); ++i) {
    uint64_t hash = base::XXHash64(i);
    unsigned sid = hash % pool_->size();
    if (FLAGS_send_myself || sid != my_index) {
      uint64_t start1 = absl::GetCurrentTimeNanos();
#if 0
      bool res = pool_->at(sid)->DispatchBrief(cb);

      if (res) {  // preempted
        unsigned num_tasks = i - last_cnt;
        last_cnt = i;

        hist_.Add(num_tasks);
      }
#else
      pool_->at(sid)->AwaitBrief(cb);
      uint64_t start2 = absl::GetCurrentTimeNanos();
      hist_.Add((start2 - start1) / 1000);
#endif
      if (last_fiber != index) {
        VLOG(1) << "Fiber [" << last_fiber << "] sending " << last_fiber_cnt << " items";
        last_fiber = index;
        last_fiber_cnt = 0;
      }
      ++last_fiber_cnt;
      //
    }

    if (i % 256 == 0)
      boost::this_fiber::yield();
  }

  VLOG(1) << "Fiber [" << last_fiber << "] sending " << last_fiber_cnt << " items";
}

vector<unique_ptr<Driver>> thread_drivers;

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  unique_ptr<ProactorPool> pp;
  pp.reset(new UringPool);

  pp->Run();

  AcceptServer acceptor(pp.get());

  if (FLAGS_http_port >= 0) {
    uint16_t port = acceptor.AddListener(FLAGS_http_port, new HttpListener<>);
    LOG(INFO) << "Started http server on port " << port;
  }

  acceptor.TriggerOnBreakSignal([] {
    LOG(INFO) << "Finish run " << true;
    finish_run = true;
  });

  acceptor.Run();

  thread_drivers.resize(pp->size());

  pp->AwaitFiberOnAll([&](unsigned index, auto* p) {
    thread_drivers[index].reset(new Driver(pp.get()));
    thread_drivers[index]->Run();
  });

  mutex mu;
  base::Histogram total;

  pp->AwaitFiberOnAll([&](unsigned index, auto* p) {
    thread_drivers[index]->Wait();

    unique_lock<mutex> lk(mu);
    total.Merge(thread_drivers[index]->hist());

    thread_drivers[index].reset();
  });

  acceptor.Stop(true);
  pp->Stop();

  CONSOLE_INFO << "Latency histogram: " << total.ToString();
  return 0;
}

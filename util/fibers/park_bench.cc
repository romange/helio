// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

// Benchmark for direct handoff through a park queue across proactor threads.
//
// A single token is protected by a spinlock. A fiber that finds the token taken parks in a queue,
// and the fiber that releases the token hands it directly to a parked fiber. Each token fiber
// acquires the token, holds it for --hold_usec, releases it and then works for a random time in
// [--work_min_usec, --work_max_usec]. All work is a busy loop.
//
// Prints a row per run and wake policy (--wake), then the medians. Columns, times in usec:
//   tok/s    - acquisitions per second.
//   hoff%    - share of acquisitions that received the token by handoff.
//   cross%   - share of handoffs whose waker runs on another thread.
//   top%     - share of acquisitions made by the busiest thread. 100 / threads is even.
//   idle*    - time from a handoff until the receiving fiber runs and owns the token: mean, p50,
//              p99, and the means of same-thread (L) and cross-thread (X) handoffs.
//   wait*    - time from the start of Acquire() until the token is owned: p50, p99, max.
//
// Filler fibers (--busy_fibers) keep the ready queue of every thread non-empty:
//   yield: each filler spins --busy_usec and yields, so a round is one pass over the fibers.
//   chain: fillers form pairs on one thread that wake each other after spinning --busy_usec,
//          so the ready queue is never empty and a round ends only when its budget runs out.
//
// Example: park_bench --proactor=uring --threads=2 --cpus=0,2 --busy_fibers=2 --wake=fifo,near

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>
#include <sched.h>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include "base/cycle_clock.h"
#include "base/flags.h"
#include "base/histogram.h"
#include "base/init.h"
#include "base/logging.h"
#include "base/spinlock.h"
#include "util/fibers/park.h"
#include "util/fibers/pool.h"

ABSL_FLAG(std::string, proactor, "uring", "uring or epoll");
ABSL_FLAG(std::string, cpus, "0,2,4,6",
          "CPUs for the proactor threads, one per thread. Pick separate physical cores.");
ABSL_FLAG(uint32_t, threads, 2, "Number of proactor threads");
ABSL_FLAG(uint32_t, fibers, 4, "Token fibers per thread");
ABSL_FLAG(uint32_t, hold_usec, 1, "How long a fiber holds the token");
ABSL_FLAG(uint32_t, work_min_usec, 5, "Minimal work between acquisitions");
ABSL_FLAG(uint32_t, work_max_usec, 20, "Maximal work between acquisitions");
ABSL_FLAG(bool, release_yield, false,
          "Token fibers yield right after release, as if they waited for I/O. Otherwise they "
          "keep running and a waiter on their thread waits for their work");
ABSL_FLAG(uint32_t, busy_fibers, 0, "Filler fibers per thread. 0 lets threads go idle");
ABSL_FLAG(std::string, busy_mode, "yield", "yield or chain, see the file comment");
ABSL_FLAG(uint32_t, busy_usec, 10, "Work of a filler fiber between yields or wakes");
ABSL_FLAG(uint32_t, warmup_ms, 100, "Warmup before measuring");
ABSL_FLAG(uint32_t, duration_ms, 1000, "Measured duration of a run");
ABSL_FLAG(uint32_t, runs, 5, "Runs per wake policy. Policies alternate between runs");
ABSL_FLAG(std::string, wake, "fifo,near",
          "Comma separated wake policies. fifo: ParkQueue::Pop. lfifo: LocalityParkQueue::Pop. "
          "near[/max_wait_usec/max_bypass]: LocalityParkQueue::PopNear");

using namespace std;
using namespace util;
using base::CycleClock;
using fb2::Fiber;
using fb2::LocalityParkNode;
using fb2::LocalityParkQueue;
using fb2::ParkNode;
using fb2::ParkQueue;
using fb2::WakeList;

namespace {

double ToUsec(uint64_t cycles) {
  return double(cycles) * 1e6 / CycleClock::Frequency();
}

void Spin(uint64_t cycles) {
  uint64_t end = CycleClock::Now() + cycles;
  while (CycleClock::Now() < end) {
    base::detail::CpuPause();
  }
}

// Written only by fibers of one thread, read after they are joined.
struct alignas(64) ThreadStats {
  uint64_t acquires = 0;
  uint64_t handoffs = 0;  // Acquisitions that received the token by handoff.
  uint64_t cross = 0;     // Handoffs from a fiber on another thread.
  uint64_t local_idle_cycles = 0;
  uint64_t cross_idle_cycles = 0;
  base::Histogram wait_us;
  base::Histogram idle_us;
};

struct RunState {
  // Fibers yield until all of them are launched: a fiber that never suspends would stall
  // the proactor loop and with it the launch of the next fiber.
  atomic_bool go{false};
  atomic_bool recording{false};
  atomic_bool stop{false};
  vector<ThreadStats> stats;
};

// A token that is handed directly to a parked fiber on release.
template <typename Queue, typename NodeBase> class Token {
 public:
  using PopFn = NodeBase* (Queue::*)();

  template <typename... Args> explicit Token(PopFn pop, Args... args) : pop_(pop), queue_(args...) {
  }

  void Acquire(unsigned me, ThreadStats* st, bool record) {
    uint64_t start = CycleClock::Now();
    unique_lock lk(lock_);
    if (!held_) {
      held_ = true;
      lk.unlock();
      if (record) {
        ++st->acquires;
        st->wait_us.Add(ToUsec(CycleClock::Now() - start));
      }
      return;
    }

    Waiter waiter;
    queue_.Push(&waiter);
    lk.unlock();
    waiter.Park();

    if (record) {
      uint64_t now = CycleClock::Now();
      uint64_t idle = now - waiter.handoff_cycles;
      ++st->acquires;
      ++st->handoffs;
      if (waiter.waker != me) {
        ++st->cross;
        st->cross_idle_cycles += idle;
      } else {
        st->local_idle_cycles += idle;
      }
      st->wait_us.Add(ToUsec(now - start));
      st->idle_us.Add(ToUsec(idle));
    }
  }

  void Release(unsigned me) {
    WakeList wakes;
    lock_guard lk(lock_);
    NodeBase* node = (queue_.*pop_)();
    if (!node) {
      held_ = false;
      return;
    }
    Waiter* waiter = static_cast<Waiter*>(node);
    waiter->handoff_cycles = CycleClock::Now();
    waiter->waker = me;
    wakes.Add(waiter);
  }

 private:
  struct Waiter : public NodeBase {
    uint64_t handoff_cycles = 0;
    unsigned waker = 0;
  };

  PopFn pop_;
  base::SpinLock lock_;
  bool held_ = false;  // Protected by lock_.
  Queue queue_;        // Protected by lock_.
};

struct Config {
  unsigned fibers;
  uint64_t hold_cycles;
  uint64_t work_min_cycles;
  uint64_t work_span_cycles;
  bool release_yield;
  unsigned busy_fibers;
  bool busy_chain;
  uint64_t busy_cycles;
};

void AwaitGo(RunState* rs) {
  while (!rs->go.load(memory_order_relaxed)) {
    ThisFiber::Yield();
  }
}

template <typename TokenT>
void TokenLoop(const Config& cfg, unsigned me, unsigned seed, TokenT* token, RunState* rs) {
  AwaitGo(rs);
  ThreadStats* st = &rs->stats[me];
  uint64_t rnd = 0x9E3779B97F4A7C15ULL * (seed + 1);
  while (!rs->stop.load(memory_order_relaxed)) {
    bool record = rs->recording.load(memory_order_relaxed);
    token->Acquire(me, st, record);
    Spin(cfg.hold_cycles);
    token->Release(me);
    if (cfg.release_yield) {
      ThisFiber::Yield();
    }

    rnd ^= rnd << 13;
    rnd ^= rnd >> 7;
    rnd ^= rnd << 17;
    Spin(cfg.work_min_cycles + rnd % cfg.work_span_cycles);
  }
}

// Two fibers on one thread that alternate: each spins and then wakes the other and parks.
struct Chain {
  ParkNode* parked = nullptr;
};

void ChainLoop(const Config& cfg, Chain* chain, RunState* rs) {
  AwaitGo(rs);
  while (!rs->stop.load(memory_order_relaxed)) {
    Spin(cfg.busy_cycles);
    WakeList wakes;
    if (ParkNode* other = exchange(chain->parked, nullptr)) {
      wakes.Add(other);
    }
    ParkNode node;
    chain->parked = &node;
    wakes.Wake();
    node.Park();
  }

  WakeList wakes;
  if (ParkNode* other = exchange(chain->parked, nullptr)) {
    wakes.Add(other);
  }
}

struct Result {
  double tokens_per_sec;
  double handoff_pct;  // Acquisitions by handoff.
  double cross_pct;    // Share of handoffs from another thread.
  double top_pct;      // Share of acquisitions by the busiest thread.
  double idle_mean, idle_p50, idle_p99;
  double local_idle_mean, cross_idle_mean;
  double wait_p50, wait_p99, wait_max;
};

template <typename Queue, typename NodeBase, typename... Args>
Result RunOnce(ProactorPool* pool, const Config& cfg, NodeBase* (Queue::*pop)(), Args... args) {
  using TokenT = Token<Queue, NodeBase>;
  RunState rs;
  rs.stats.resize(pool->size());
  TokenT token(pop, args...);

  vector<Fiber> fibers;
  vector<unique_ptr<Chain>> chains;
  for (unsigned t = 0; t < pool->size(); ++t) {
    fb2::ProactorBase* p = pool->at(t);
    for (unsigned i = 0; i < cfg.fibers; ++i) {
      unsigned seed = t * cfg.fibers + i;
      fibers.push_back(p->LaunchFiber(absl::StrCat("token", seed),
                                      [&, t, seed] { TokenLoop(cfg, t, seed, &token, &rs); }));
    }
    for (unsigned i = 0; i < cfg.busy_fibers; ++i) {
      if (cfg.busy_chain) {
        if (i % 2 == 0)
          chains.push_back(make_unique<Chain>());
        Chain* chain = chains.back().get();
        fibers.push_back(p->LaunchFiber(absl::StrCat("chain", t, "_", i),
                                        [&, chain] { ChainLoop(cfg, chain, &rs); }));
      } else {
        fibers.push_back(p->LaunchFiber(absl::StrCat("busy", t, "_", i), [&] {
          AwaitGo(&rs);
          while (!rs.stop.load(memory_order_relaxed)) {
            Spin(cfg.busy_cycles);
            ThisFiber::Yield();
          }
        }));
      }
    }
  }

  rs.go.store(true, memory_order_relaxed);
  ThisFiber::SleepFor(chrono::milliseconds(absl::GetFlag(FLAGS_warmup_ms)));
  rs.recording.store(true, memory_order_relaxed);
  uint64_t start = CycleClock::Now();
  ThisFiber::SleepFor(chrono::milliseconds(absl::GetFlag(FLAGS_duration_ms)));
  rs.recording.store(false, memory_order_relaxed);
  uint64_t elapsed = CycleClock::Now() - start;
  rs.stop.store(true, memory_order_relaxed);
  for (auto& fb : fibers) {
    fb.Join();
  }

  ThreadStats total;
  uint64_t top = 0;
  for (const ThreadStats& st : rs.stats) {
    top = max(top, st.acquires);
    total.acquires += st.acquires;
    total.handoffs += st.handoffs;
    total.cross += st.cross;
    total.local_idle_cycles += st.local_idle_cycles;
    total.cross_idle_cycles += st.cross_idle_cycles;
    total.wait_us.Merge(st.wait_us);
    total.idle_us.Merge(st.idle_us);
  }

  Result res;
  res.tokens_per_sec = total.acquires / (elapsed / double(CycleClock::Frequency()));
  res.handoff_pct = 100.0 * total.handoffs / max<uint64_t>(total.acquires, 1);
  res.cross_pct = 100.0 * total.cross / max<uint64_t>(total.handoffs, 1);
  res.top_pct = 100.0 * top / max<uint64_t>(total.acquires, 1);
  res.idle_mean = total.idle_us.Average();
  auto [ip50, ip99] = total.idle_us.Percentiles(50, 99);
  res.idle_p50 = ip50;
  res.idle_p99 = ip99;
  uint64_t local = total.handoffs - total.cross;
  res.local_idle_mean = local ? ToUsec(total.local_idle_cycles) / local : 0;
  res.cross_idle_mean = total.cross ? ToUsec(total.cross_idle_cycles) / total.cross : 0;
  auto [wp50, wp99] = total.wait_us.Percentiles(50, 99);
  res.wait_p50 = wp50;
  res.wait_p99 = wp99;
  res.wait_max = total.wait_us.max();
  return res;
}

// Field accessors in printing order.
struct Column {
  const char* name;
  double Result::* field;
  int precision;
};

const Column kColumns[] = {
    {"tok/s", &Result::tokens_per_sec, 0},  {"hoff%", &Result::handoff_pct, 1},
    {"cross%", &Result::cross_pct, 1},      {"top%", &Result::top_pct, 1},
    {"idle", &Result::idle_mean, 2},        {"idle50", &Result::idle_p50, 2},
    {"idle99", &Result::idle_p99, 1},       {"idleL", &Result::local_idle_mean, 2},
    {"idleX", &Result::cross_idle_mean, 2}, {"wait50", &Result::wait_p50, 1},
    {"wait99", &Result::wait_p99, 1},       {"waitMax", &Result::wait_max, 0},
};

string FormatRow(string_view label, const Result& r) {
  string row = absl::StrFormat("%-14s", label);
  for (const Column& c : kColumns) {
    absl::StrAppendFormat(&row, " %10.*f", c.precision, r.*c.field);
  }
  return row;
}

string Header() {
  string row = absl::StrFormat("%-14s", "policy");
  for (const Column& c : kColumns) {
    absl::StrAppendFormat(&row, " %10s", c.name);
  }
  return row;
}

Result Median(vector<Result> results) {
  Result res;
  for (const Column& c : kColumns) {
    vector<double> vals;
    for (const Result& r : results)
      vals.push_back(r.*c.field);
    sort(vals.begin(), vals.end());
    size_t n = vals.size();
    res.*c.field = n % 2 ? vals[n / 2] : (vals[n / 2 - 1] + vals[n / 2]) / 2;
  }
  return res;
}

vector<unsigned> ParseCpus(string_view list) {
  vector<unsigned> cpus;
  for (string_view part : absl::StrSplit(list, ',', absl::SkipEmpty())) {
    unsigned cpu;
    CHECK(absl::SimpleAtoi(part, &cpu)) << part;
    cpus.push_back(cpu);
  }
  return cpus;
}

void SetAffinity(const vector<unsigned>& cpus) {
  cpu_set_t set;
  CPU_ZERO(&set);
  for (unsigned cpu : cpus)
    CPU_SET(cpu, &set);
  CHECK_EQ(0, sched_setaffinity(0, sizeof(set), &set));
}

}  // namespace

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  unsigned threads = absl::GetFlag(FLAGS_threads);
  vector<unsigned> cpus = ParseCpus(absl::GetFlag(FLAGS_cpus));
  CHECK_GE(cpus.size(), threads);
  cpus.resize(threads);

  string busy_mode = absl::GetFlag(FLAGS_busy_mode);
  CHECK(busy_mode == "yield" || busy_mode == "chain") << busy_mode;

  CHECK_LE(absl::GetFlag(FLAGS_work_min_usec), absl::GetFlag(FLAGS_work_max_usec));
  auto us = [](uint32_t usec) { return CycleClock::FromUsec(usec); };
  Config cfg;
  cfg.fibers = absl::GetFlag(FLAGS_fibers);
  cfg.hold_cycles = us(absl::GetFlag(FLAGS_hold_usec));
  cfg.work_min_cycles = us(absl::GetFlag(FLAGS_work_min_usec));
  cfg.work_span_cycles = us(absl::GetFlag(FLAGS_work_max_usec)) - cfg.work_min_cycles + 1;
  cfg.release_yield = absl::GetFlag(FLAGS_release_yield);
  cfg.busy_fibers = absl::GetFlag(FLAGS_busy_fibers);
  cfg.busy_chain = busy_mode == "chain";
  cfg.busy_cycles = us(absl::GetFlag(FLAGS_busy_usec));
  CHECK(!cfg.busy_chain || cfg.busy_fibers % 2 == 0) << "chain fillers come in pairs";

  // The pool pins thread i to the i-th CPU allowed for the process.
  cpu_set_t all_cpus;
  CHECK_EQ(0, sched_getaffinity(0, sizeof(all_cpus), &all_cpus));
  SetAffinity(cpus);

  unique_ptr<ProactorPool> pool;
  string kind = absl::GetFlag(FLAGS_proactor);
  if (kind == "epoll") {
    pool.reset(fb2::Pool::Epoll(threads));
  } else {
    CHECK_EQ(kind, "uring");
    pool.reset(fb2::Pool::IOUring(256, threads));
  }
  pool->Run();

  // Keep the main thread off the proactor CPUs.
  for (unsigned cpu : cpus)
    CPU_CLR(cpu, &all_cpus);
  if (CPU_COUNT(&all_cpus) > 0) {
    CHECK_EQ(0, sched_setaffinity(0, sizeof(all_cpus), &all_cpus));
  }

  pool->AwaitBrief([](unsigned index, fb2::ProactorBase*) {
    LOG(INFO) << "Proactor " << index << " runs on cpu " << sched_getcpu();
  });

  vector<string> policies = absl::StrSplit(absl::GetFlag(FLAGS_wake), ',', absl::SkipEmpty());
  vector<vector<Result>> results(policies.size());
  unsigned runs = absl::GetFlag(FLAGS_runs);

  cout << kind << " threads=" << threads << " fibers=" << cfg.fibers
       << " release_yield=" << cfg.release_yield << " busy=" << cfg.busy_fibers << "/" << busy_mode
       << endl;
  cout << Header() << endl;
  for (unsigned run = 0; run < runs; ++run) {
    for (size_t i = 0; i < policies.size(); ++i) {
      Result res;
      vector<string_view> spec = absl::StrSplit(policies[i], '/');
      if (spec[0] == "fifo") {
        res = RunOnce(pool.get(), cfg, &ParkQueue::Pop);
      } else if (spec[0] == "lfifo") {
        res = RunOnce(pool.get(), cfg, &LocalityParkQueue::Pop);
      } else if (spec[0] == "near") {
        uint32_t max_wait_usec = 50, max_bypass = 16;
        if (spec.size() > 1)
          CHECK(absl::SimpleAtoi(spec[1], &max_wait_usec)) << policies[i];
        if (spec.size() > 2)
          CHECK(absl::SimpleAtoi(spec[2], &max_bypass)) << policies[i];
        res = RunOnce(pool.get(), cfg, &LocalityParkQueue::PopNear, max_wait_usec, max_bypass);
      } else {
        LOG(FATAL) << "Unknown wake policy " << policies[i];
      }
      results[i].push_back(res);
      cout << FormatRow(policies[i], res) << endl;
    }
  }

  cout << "median of " << runs << " runs" << endl;
  for (size_t i = 0; i < policies.size(); ++i) {
    cout << FormatRow(policies[i], Median(results[i])) << endl;
  }

  pool->Stop();
  return 0;
}

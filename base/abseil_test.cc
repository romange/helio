// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// To see assembler `objdump -S -M intel base/CMakeFiles/bits_test.dir/bits_test.cc.o`
// To compile assembler:
//   gcc -O3 -mtune=native -mavx -std=c++11 -S -masm=intel  -fverbose-asm bits_test.cc
//    -I.. -I../third_party/libs/benchmark/include/ -I../third_party/libs/gtest/include/

#ifdef __linux__

#include <absl/base/internal/cycleclock.h>
#include <absl/debugging/internal/vdso_support.h>
#include <absl/debugging/stacktrace.h>
#include <absl/strings/str_format.h>
#include <absl/time/clock.h>
#include <ucontext.h>

#include "base/gtest.h"
#include "base/logging.h"

namespace base {

using namespace absl;
using namespace std;
using benchmark::DoNotOptimize;

class AbseilTest : public testing::Test {
 public:
};

TEST_F(AbseilTest, VDSO) {
  debugging_internal::VDSOSupport vdso;
  vdso.Init();
  for (auto it = vdso.begin(); it != vdso.end(); ++it) {
    LOG(INFO) << it->name << ": " << it->version << " " << ELF64_ST_TYPE(it->symbol->st_info);
  }
  absl::debugging_internal::VDSOSupport::SymbolInfo symbol_info;
#if defined(__aarch64__)
  EXPECT_TRUE(vdso.LookupSymbol("__kernel_rt_sigreturn", "LINUX_2.6.39", STT_NOTYPE, &symbol_info));
#else
  EXPECT_TRUE(vdso.LookupSymbol("__vdso_clock_gettime", "LINUX_2.6", STT_FUNC, &symbol_info));
#endif
}

// Does not work in release-mode.
#if 0
TEST_F(AbseilTest, PerftoolsProfile) {
  void* stack[256];

  int res = ::GetStackTrace(stack, 255, 1);
  ASSERT_GT(res, 5);
}

TEST_F(AbseilTest, Stacktrace) {
  void* stack[256];
  int res = absl::GetStackTraceWithContext(stack, 255, 1, NULL, NULL);
  ASSERT_GT(res, 5);
}
#endif

TEST_F(AbseilTest, SNPrintF) {
  char buf[16];
  absl::SNPrintF(buf, sizeof(buf), "FOOBAR%04d", 42);
}

void BM_CycleClock(benchmark::State& state) {
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < 10; ++i) {
      DoNotOptimize(absl::base_internal::CycleClock::Now());
    }
  }
}
BENCHMARK(BM_CycleClock);

#if defined(__x86_64__)

void BM_RDTSC(benchmark::State& state) {
  uint64_t low, high;
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < 10; ++i) {
      __asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
    }
  }
}
BENCHMARK(BM_RDTSC);

#endif

void BM_AbslCurrentTime(benchmark::State& state) {
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < 10; ++i) {
      DoNotOptimize(absl::GetCurrentTimeNanos());
    }
  }
}
BENCHMARK(BM_AbslCurrentTime);

template <clockid_t cid> void BM_ClockType(benchmark::State& state) {
  timespec ts;
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < 10; ++i)
      DoNotOptimize(clock_gettime(cid, &ts));
  }
}

BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_REALTIME);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_REALTIME_COARSE);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_MONOTONIC);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_MONOTONIC_COARSE);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_BOOTTIME);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_PROCESS_CPUTIME_ID);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_THREAD_CPUTIME_ID);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_BOOTTIME_ALARM);

}  // namespace base

#endif  // __linux__

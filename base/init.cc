// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"

#ifdef USE_ABSL_LOG
#include <absl/log/initialize.h>
#include <absl/log/log_sink_registry.h>

#include "base/file_log_sink.h"
#endif

#include <absl/base/internal/cycleclock.h>
#include <absl/base/internal/sysinfo.h>
#include <absl/debugging/failure_signal_handler.h>
#include <absl/debugging/symbolize.h>
#include <absl/flags/parse.h>

#include <atomic>
#include <exception>

#include "base/cycle_clock.h"
#include "base/logging.h"

// This overrides glibc's default assert handler in debug builds so
// we can get a stack trace.
#ifndef NDEBUG
#ifdef __GLIBC__
extern "C" void __assert_fail(const char* assertion, const char* file, unsigned int line,
                              const char* function) {
  LOG(FATAL) << "[" << file << ":" << line << "]: " << "assert(" << assertion << ") failed!";
}
#endif
#endif

namespace __internal__ {

ModuleInitializer::ModuleInitializer(VoidFunction ftor, bool is_ctor)
    : node_{ftor, global_list(), is_ctor} {
  global_list() = &node_;
}

auto ModuleInitializer::global_list() -> CtorNode*& {
  static CtorNode* my_global_list = nullptr;
  return my_global_list;
}

void ModuleInitializer::RunFtors(bool is_ctor) {
  CtorNode* node = global_list();
  while (node) {
    if (node->is_ctor == is_ctor)
      node->func();
    node = node->next;
  }
}

}  // namespace __internal__

#undef MainInitGuard

static std::atomic<int> main_init_guard_count{0};

namespace base {

uint64_t CycleClock::frequency_ = 0;
uint64_t CycleClock::cycles_to_usec_ = 0;
uint64_t CycleClock::usec_to_cycles_ = 0;

void CycleClock::InitOnce() {
#ifdef __aarch64__
  // On aarch64, we can read the frequency from the cntfrq_el0 register.
  uint64_t res;
  __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(res));
  frequency_ = res;
#elif defined(__x86_64__)
  // Now() reads the raw TSC. absl::base_internal::CycleClock::Frequency() is scaled down by
  // kCycleClockShift (2x with NDEBUG, 4x without), which would inflate ToUsec() accordingly.
  frequency_ = absl::base_internal::NominalCPUFrequency();
#else
  frequency_ = absl::base_internal::CycleClock::Frequency();
#endif

  // The cycles_to_usec_ multiplier fits into 64 bits only above 62.5kHz.
  // This should not happen on real devices but may happen on virtual devices or emulators like
  // faketime.
  if (frequency_ < 63'000u) {
    frequency_ = 63'000u;
  }

  using u128 = unsigned __int128;
  cycles_to_usec_ = ((static_cast<u128>(1000'000) << 60) + frequency_ - 1) / frequency_;
  usec_to_cycles_ = ((static_cast<u128>(frequency_) << 32) + 999'999) / 1000'000;
}

// We bound our measurements since number of cycles should not overflow 1ms or 10ms.
// But if our measurements cover larger time intervals, we just set saturated value
// kHighCycleBound which should actually be larger than 100% cpu time in 10ms.
constexpr uint64_t kHighCycleBound = 1 << 31UL;

void CycleBucketAggregator::Add(uint64_t start, uint64_t now) {
  uint64_t now_ms = Milliseconds(now);

  const uint64_t cycles = now - start;

  if (measurement_start_1ms_ == now_ms) {
    cycles_1ms_ = std::min(cycles_1ms_ + cycles, kHighCycleBound);
    cycles_10ms_ = std::min(cycles_10ms_ + cycles, kHighCycleBound);
  } else {
    cycles_1ms_ = std::min(cycles, kHighCycleBound);
    measurement_start_1ms_ = now_ms;

    const uint64_t current_10ms_bucket_start = (now_ms / 10) * 10;
    if (measurement_start_10ms_ == current_10ms_bucket_start) {
      cycles_10ms_ = std::min(cycles_10ms_ + cycles, kHighCycleBound);
    } else {
      cycles_10ms_ = std::min(cycles, kHighCycleBound);
      measurement_start_10ms_ = current_10ms_bucket_start;
    }
  }
}

#ifdef USE_ABSL_LOG
static FileLogSink file_log_sink;

// We initialize and cache the candidate inside the object.
std::vector<std::string> GetLoggingDirectories() {
  const auto& dir = file_log_sink.base_dir();
  return dir.empty() ? std::vector<std::string>{} : std::vector<std::string>{dir};
}

#endif

}  // namespace base

MainInitGuard::MainInitGuard(int* argc, char*** argv, uint32_t flags) {
  // MallocExtension::Initialize();
  if (main_init_guard_count.fetch_add(1))
    return;

#ifdef USE_ABSL_LOG
  base::InitLoggingFlagsFromEnv();
#endif
  absl::ParseCommandLine(*argc, *argv);

#ifdef USE_ABSL_LOG
  absl::InitializeLog();
  base::file_log_sink.Init();
  absl::AddLogSink(&base::file_log_sink);
#else
  if (!google::IsGoogleLoggingInitialized()) {
    google::InitGoogleLogging((*argv)[0]);
  }
#endif
  absl::InitializeSymbolizer((*argv)[0]);
  absl::FailureSignalHandlerOptions options;
  absl::InstallFailureSignalHandler(options);

  base::kProgramName = (*argv)[0];
  base::CycleClock::InitOnce();

#if defined NDEBUG
  LOG(INFO) << (*argv)[0] << " running in opt mode.";
#else
  LOG(INFO) << (*argv)[0] << " running in debug mode.";
#endif
  std::set_terminate([] {
    std::exception_ptr e_ptr = std::current_exception();
    if (!e_ptr) {
      LOG(FATAL) << "Terminate handler called without exception";
    }

    try {
      std::rethrow_exception(e_ptr);
    } catch (std::exception& e) {
      LOG(FATAL) << "Uncaught exception: " << e.what();
    } catch (...) {
      LOG(FATAL) << "Uncaught exception";
    }
  });

  __internal__::ModuleInitializer::RunFtors(true);
}

MainInitGuard::~MainInitGuard() {
  int count = main_init_guard_count.fetch_add(-1);
  CHECK_GT(count, 0);
  if (count > 1)
    return;

  __internal__::ModuleInitializer::RunFtors(false);
#ifdef USE_ABSL_LOG
  absl::RemoveLogSink(&base::file_log_sink);
  base::file_log_sink.Flush();
#else
  google::ShutdownGoogleLogging();
#endif
}

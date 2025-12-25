// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"

#ifdef USE_ABSL_LOG
#include <absl/log/initialize.h>
#endif

#include <absl/base/internal/cycleclock.h>
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
  abort();
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

namespace base {

uint64_t CycleClock::frequency_ = 0;
static uint64_t freq_ms = 0;

void CycleClock::InitOnce() {
#ifdef __aarch64__
  // On aarch64, we can read the frequency from the cntfrq_el0 register.
  uint64_t res;
  __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(res));
  frequency_ = res;
#else
  frequency_ = absl::base_internal::CycleClock::Frequency();
#endif
  freq_ms = frequency_ / 1000;
}

// We bound our measurements since number of cycles should not overflow 1ms or 10ms.
// But if our measurements cover larger time intervals, we just set saturated value
// kHighCycleBound which should actually be larger than 100% cpu time in 10ms.
constexpr uint64_t kHighCycleBound = 1 << 31UL;

void RealTimeAggregator::Add(uint64_t start, uint64_t now) {
  uint32_t now_ms = static_cast<uint32_t>(now / freq_ms);

  const uint64_t cycles = now - start;

  if (measurement_start_1ms_ == now_ms) {
    cycles_1ms_ = std::min(cycles_1ms_ + cycles, kHighCycleBound);
    cycles_10ms_ = std::min(cycles_10ms_ + cycles, kHighCycleBound);
  } else {
    cycles_1ms_ = std::min(cycles, kHighCycleBound);
    measurement_start_1ms_ = now_ms;

    const uint32_t current_10ms_bucket_start = (now_ms / 10) * 10;
    if (measurement_start_10ms_ == current_10ms_bucket_start) {
      cycles_10ms_ = std::min(cycles_10ms_ + cycles, kHighCycleBound);
    } else {
      cycles_10ms_ = std::min(cycles, kHighCycleBound);
      measurement_start_10ms_ = current_10ms_bucket_start;
    }
  }
}

}  // namespace base

static std::atomic<int> main_init_guard_count{0};

MainInitGuard::MainInitGuard(int* argc, char*** argv, uint32_t flags) {
  // MallocExtension::Initialize();
  if (main_init_guard_count.fetch_add(1))
    return;

  absl::ParseCommandLine(*argc, *argv);
#ifdef USE_ABSL_LOG
  absl::InitializeLog();
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
#ifndef USE_ABSL_LOG
  google::ShutdownGoogleLogging();
#endif
}

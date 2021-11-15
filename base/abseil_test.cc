// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// To see assembler `objdump -S -M intel base/CMakeFiles/bits_test.dir/bits_test.cc.o`
// To compile assembler:
//   gcc -O3 -mtune=native -mavx -std=c++11 -S -masm=intel  -fverbose-asm bits_test.cc
//    -I.. -I../third_party/libs/benchmark/include/ -I../third_party/libs/gtest/include/

#include <absl/debugging/internal/vdso_support.h>
#include <absl/debugging/stacktrace.h>

#include <gperftools/stacktrace.h>
#include <ucontext.h>

#include "base/gtest.h"
#include "base/logging.h"

namespace base {

using namespace absl;
using namespace std;

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

}  // namespace base

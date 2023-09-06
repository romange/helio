// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/stacktrace.h"

#include "absl/debugging/stacktrace.h"
#include "absl/debugging/symbolize.h"
#include "absl/strings/str_format.h"

#ifdef NDEBUG
#define SKIP_COUNT_TOP 2
#define SKIP_COUNT_BOTTOM 2
#else
#define SKIP_COUNT_TOP 5
#define SKIP_COUNT_BOTTOM 5
#endif

std::string util::GetStacktrace() {
  void* addresses[20];
  int size = absl::GetStackTrace(addresses, sizeof(addresses) / sizeof(void*),
                                 /*skip_count=*/SKIP_COUNT_TOP);

  std::string rv;
  for (int i = 0; i < size - SKIP_COUNT_BOTTOM; i++) {
    char symbol_buf[1024];
    const char* symbol = "(unknown)";
    if (absl::Symbolize(addresses[i], symbol_buf, sizeof(symbol_buf))) {
      symbol = symbol_buf;
    }
    rv += absl::StrFormat("%p  %s\n", addresses[i], symbol);
  }

  return rv;
}

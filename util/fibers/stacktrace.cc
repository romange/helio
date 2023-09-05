// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/stacktrace.h"

#include <dlfcn.h>
#include <execinfo.h>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "base/logging.h"

std::string util::GetStacktrace(bool symbolize) {
  void* addresses[20];
  int size = backtrace(addresses, sizeof(addresses) / sizeof(void*));

  // Faster code path, no function/lines
  if (!symbolize) {
    char** symbolized = backtrace_symbols(addresses, size);

    std::string rv = absl::StrJoin(symbolized, symbolized + size, "\n");
    free(symbolized);

    return rv;
  }

  for (int i = 0; i < size; i++) {
    Dl_info info;
    if (dladdr(addresses[i], &info) != 0) {
      // dladdr returns non-zero on success
      uintptr_t base_address = reinterpret_cast<uintptr_t>(info.dli_fbase);
      addresses[i] =
          reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(addresses[i]) - base_address);
    }
  }

  std::string addresses_string;
  for (int i = 0; i < size; ++i) {
    addresses_string += absl::StrFormat(" %p", addresses[i]);
  }

  std::string cmd =
      absl::StrCat("addr2line -e ", base::ProgramAbsoluteFileName(), " -p -f", addresses_string);
  FILE* pipe = popen(cmd.c_str(), "r");
  if (pipe == nullptr) {
    LOG(ERROR) << "Error opening pipe to addr2line." << std::endl;
    return "";
  }

  // Read and print the addr2line output
  char line[1024];
  std::string rv;
  int i = 0;
  while (fgets(line, sizeof(line), pipe) != nullptr) {
    rv += absl::StrFormat("[%02d]: %s", i, line);
    i++;
  }

  pclose(pipe);
  return rv;
}

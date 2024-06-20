// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// Much slimmer version of https://github.com/google/cpu_features/
// Assumes only relatively recent cpu families (found in the cloud)
namespace base {

struct CpuFeatures {
#ifdef __x86_64__
  bool has_avx2 = false;
  bool has_avx512f = false;
#endif

#ifdef __aarch64__
// TBD
#endif
};

#ifdef __x86_64__
CpuFeatures GetCpuFeatures();

#else

// Stub for now.
inline CpuFeatures GetCpuFeatures() {
  return CpuFeatures{};

}

#endif

}  // namespace base
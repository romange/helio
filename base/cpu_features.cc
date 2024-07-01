// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/cpu_features.h"

#include <cstdint>

// aarch64 is currently a noop.
#ifdef __x86_64__

namespace base {

namespace {

// See <cpuid.h> for constants reference
constexpr unsigned BIT_AVX2 = (1 << 5);
constexpr unsigned BIT_AVX512F = (1 << 16);

constexpr unsigned BIT_XSAVE = (1 << 26);
constexpr unsigned BIT_OSXSAVE = (1 << 27);

// A struct to hold the result of a call to cpuid.
typedef struct {
  uint32_t eax, ebx, ecx, edx;
} Leaf;

Leaf GetCpuidLeaf(uint32_t leaf_id) {
  Leaf leaf;
  __asm__ __volatile__("cpuid"
                       : "=a"(leaf.eax), "=b"(leaf.ebx), "=c"(leaf.ecx), "=d"(leaf.edx)
                       : "a"(leaf_id), "c"(0));
  return leaf;
}

uint32_t GetXCR0() {
  uint32_t xcr0;
  __asm__("xgetbv" : "=a"(xcr0) : "c"(0) : "%edx");
  return xcr0;
}

}  // namespace

CpuFeatures GetCpuFeatures() {
  CpuFeatures res;
  Leaf leaf = GetCpuidLeaf(0);
  const uint32_t max_cpuid_leaf = leaf.eax;

  if (max_cpuid_leaf < 7)
    return res;

  leaf = GetCpuidLeaf(1);

  bool has_xcr0 = (leaf.ecx & BIT_OSXSAVE) && (leaf.ecx & BIT_XSAVE);
  if (!has_xcr0)
    return res;

  leaf = GetCpuidLeaf(7);
  const uint32_t xcr0 = GetXCR0();

  if ((xcr0 & 6) != 6)
    return res;

  // See https://en.wikichip.org/wiki/x86/avx-512 for explanation about the variants
  res.has_avx2 = leaf.ebx & BIT_AVX2;
  res.has_avx512f = leaf.ebx & BIT_AVX512F;

  return res;
}

}  // namespace base

#endif

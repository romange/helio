// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// based on MurmurHash code.
//
#include "base/hash.h"
#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include <string.h>

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace {

inline uint32_t fmix(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

inline uint32_t rotl32(uint32_t x, int8_t r) {
    return (x << r) | (x >> (32 - r));
}

}  // namespace


namespace base {

uint32_t MurmurHash3_x86_32(const uint8_t* data, uint32_t len, uint32_t seed) {
  const uint32_t nblocks = len / 4;

  uint32_t h1 = seed;

  uint32_t c1 = 0xcc9e2d51;
  uint32_t c2 = 0x1b873593;

  //----------
  // body

  const uint32_t * blocks = (const uint32_t*) (data + nblocks * 4);

  int i;
  for(i = -nblocks; i; i++)
  {
      uint32_t k1;
      memcpy(&k1, blocks + i, sizeof(uint32_t));

      k1 *= c1;
      k1 = rotl32(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = rotl32(h1, 13);
      h1 = h1*5+0xe6546b64;
  }

  //----------
  // tail

  const uint8_t * tail = data + nblocks*4;

  uint32_t k1 = 0;

  switch(len & 3)
  {
      case 3: k1 ^= tail[2] << 16;ABSL_FALLTHROUGH_INTENDED;
      case 2: k1 ^= tail[1] << 8;ABSL_FALLTHROUGH_INTENDED;
      case 1: k1 ^= tail[0];
            k1 *= c1; k1 = rotl32(k1,15); k1 *= c2; h1 ^= k1;
  }

  //----------
  // finalization

  h1 ^= len;

  h1 = fmix(h1);

  return h1;
}

uint64_t Fingerprint(const char* str, uint32_t len) {
  uint64_t res = XXH64(str, len, 24061983);
  if (ABSL_PREDICT_TRUE(res > 1))
    return res;
  return 2;
}

}  // namespace base

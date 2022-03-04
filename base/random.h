// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace base {

/**
 * @brief Random generator with c++ interface.
 *
 * original documentation by Vigna:
 * This is a fixed-increment version of Java 8's SplittableRandom generator
 * See http://dx.doi.org/10.1145/2714064.2660195 and
 * http://docs.oracle.com/javase/8/docs/api/java/util/SplittableRandom.html
 * It is a very fast generator passing BigCrush, and it can be useful if
 * for some reason you absolutely want 64 bits of state; otherwise, we
 * rather suggest to use a xoroshiro128+ (for moderately parallel
 * computations) or xorshift1024* (for massively parallel computations)
 * generator.
 */

class SplitMix64 {
 public:
  using result_type = uint64_t;

  SplitMix64(uint64_t state = 0x6a45554a264d72bULL) : state_(state) {
  }

  void seed(uint64_t s) {
    state_ = s;
  }

  result_type operator()() {
    uint64_t z = (state_ += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }

  static constexpr result_type min() {
    return std::numeric_limits<result_type>::min();
  }

  static constexpr result_type max() {
    return std::numeric_limits<result_type>::max();
  }

 private:
  uint64_t state_;
};

/**
 * @brief xoroshiro128+ random number generator.
 *
 * original documentation by Vigna:
 * This is the successor to xorshift128+. It is the fastest fULL-period
 * generator passing BigCrush without systematic failures, but due to the
 * relatively short period it is acceptable only for applications with a
 * mild amount of parallelism; otherwise, use a xorshift1024* generator.
 * Beside passing BigCrush, this generator passes the PractRand test suite
 * up to (and included) 16TB, with the exception of binary rank tests,
 * which fail due to the lowest bit being an LFSR; all other bits pass all
 * tests. We suggest to use a sign test to extract a random Boolean value.
 * Note that the generator uses a simulated rotate operation, which most C
 * compilers will turn into a single instruction. In Java, you can use
 * Long.rotateLeft(). In languages that do not make low-level rotation
 * instructions accessible xorshift128+ could be faster.
 * The state must be seeded so that it is not everywhere zero. If you have
 * a 64-bit seed, we suggest to seed a splitmix64 generator and use its
 * output to fill s.
 */
class Xoroshiro128p {
  static uint64_t rotl(uint64_t x, unsigned k) {
    return (x << k) | (x >> (64 - k));
  }

 public:
  using result_type = uint64_t;

  Xoroshiro128p() : state_{0xc898c79bcd50055bULL, 0x5dfe998262db6465ULL} {
  }

  Xoroshiro128p(uint64_t s) {
    seed(s);
  }

  void seed(uint64_t s) {
    SplitMix64 sm(s);

    state_[0] = sm();
    state_[1] = sm();
  }

  result_type operator()() {
    uint64_t s0 = state_[0];
    uint64_t s1 = state_[1];
    uint64_t result = s0 + s1;

    s1 ^= s0;
    state_[0] = rotl(s0, 24) ^ s1 ^ (s1 << 16);  // a, b
    state_[1] = rotl(s1, 37);                    // c

    return result;
  }

  static constexpr result_type min() {
    return std::numeric_limits<result_type>::min();
  }

  static constexpr result_type max() {
    return std::numeric_limits<result_type>::max();
  }

  /* This is the jump function for the generator. It is equivalent
     to 2^64 calls to next(); it can be used to generate 2^64
     non-overlapping subsequences for parallel computations. */
  void jump() {
    const uint64_t JUMP[] = {
        0xbeac0467eba5facb,
        0xd86b048b86aa9922,
    };

    uint64_t s0 = 0;
    uint64_t s1 = 0;

    for (unsigned i = 0; i < 2; i++) {
      for (int b = 0; b < 64; b++) {
        if (JUMP[i] & 1ULL << b) {
          s0 ^= state_[0];
          s1 ^= state_[1];
        }
        (*this)();
      }
    }

    state_[0] = s0;
    state_[1] = s1;
  }

 private:
  uint64_t state_[2];
};

}  // namespace base

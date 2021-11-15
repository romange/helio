// Copyright 2018 J. Andrew Rogers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef AQUAHASH_H
#define AQUAHASH_H

#include <emmintrin.h>
#include <smmintrin.h>
#include <wmmintrin.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>

class AquaHash {
 private:
  // INCREMENTAL CONSTRUCTION STATE

  // 4 x 128-bit hashing lanes
  __m128i block[4];
  // input block buffer
  __m128i input[4];
  // initialization vector
  __m128i initialize;
  // cumulative input bytes
  size_t input_bytes;

  static constexpr size_t max_input = std::numeric_limits<size_t>::max() - 1;
  // sentinel to prevent double finalization
  static constexpr size_t finalized = max_input + 1;

 public:
  // Reference implementation of AquaHash small key algorithm
  static __m128i SmallKeyAlgorithm(const uint8_t* key, const size_t bytes,
                                   __m128i initialize = _mm_setzero_si128()) {
    assert(bytes <= max_input);
    __m128i hash = initialize;

    // bulk hashing loop -- 128-bit block size
    const __m128i* ptr128 = reinterpret_cast<const __m128i*>(key);
    if (bytes / sizeof(hash)) {
      __m128i temp = _mm_set_epi64x(0xa11202c9b468bea1, 0xd75157a01452495b);
      for (uint32_t i = 0; i < bytes / sizeof(hash); ++i) {
        __m128i b = _mm_loadu_si128(ptr128++);
        hash = _mm_aesenc_si128(hash, b);
        temp = _mm_aesenc_si128(temp, b);
      }
      hash = _mm_aesenc_si128(hash, temp);
    }

    // AES sub-block processor
    const uint8_t* ptr8 = reinterpret_cast<const uint8_t*>(ptr128);
    if (bytes & 8) {
      __m128i b = _mm_set_epi64x(*reinterpret_cast<const uint64_t*>(ptr8), 0xa11202c9b468bea1);
      hash = _mm_xor_si128(hash, b);
      ptr8 += 8;
    }

    if (bytes & 4) {
      __m128i b = _mm_set_epi32(0xb1293b33, 0x05418592, *reinterpret_cast<const uint32_t*>(ptr8),
                                0xd210d232);
      hash = _mm_xor_si128(hash, b);
      ptr8 += 4;
    }

    if (bytes & 2) {
      __m128i b = _mm_set_epi16(0xbd3d, 0xc2b7, 0xb87c, 0x4715, 0x6a6c, 0x9527,
                                *reinterpret_cast<const uint16_t*>(ptr8), 0xac2e);
      hash = _mm_xor_si128(hash, b);
      ptr8 += 2;
    }

    if (bytes & 1) {
      __m128i b = _mm_set_epi8(0xcc, 0x96, 0xed, 0x16, 0x74, 0xea, 0xaa, 0x03, 0x1e, 0x86, 0x3f,
                               0x24, 0xb2, 0xa8, *reinterpret_cast<const uint8_t*>(ptr8), 0x31);
      hash = _mm_xor_si128(hash, b);
    }

    // this algorithm construction requires no less than three AES rounds to finalize
    hash = _mm_aesenc_si128(hash, _mm_set_epi64x(0x8e51ef21fabb4522, 0xe43d7a0656954b6c));
    hash = _mm_aesenc_si128(hash, _mm_set_epi64x(0x56082007c71ab18f, 0x76435569a03af7fa));
    return _mm_aesenc_si128(hash, _mm_set_epi64x(0xd2600de7157abc68, 0x6339e901c3031efb));
  }

  // Reference implementation of AquaHash large key algorithm
  static __m128i LargeKeyAlgorithm(const uint8_t* key, const size_t bytes,
                                   __m128i initialize = _mm_setzero_si128()) {
    assert(bytes <= max_input);

    // initialize 4 x 128-bit hashing lanes, for a 512-bit block size
    __m128i block[4] = {
        _mm_xor_si128(initialize, _mm_set_epi64x(0xa11202c9b468bea1, 0xd75157a01452495b)),
        _mm_xor_si128(initialize, _mm_set_epi64x(0xb1293b3305418592, 0xd210d232c6429b69)),
        _mm_xor_si128(initialize, _mm_set_epi64x(0xbd3dc2b7b87c4715, 0x6a6c9527ac2e0e4e)),
        _mm_xor_si128(initialize, _mm_set_epi64x(0xcc96ed1674eaaa03, 0x1e863f24b2a8316a))};

    // bulk hashing loop -- 512-bit block size
    const __m128i* ptr128 = reinterpret_cast<const __m128i*>(key);
    for (size_t block_counter = 0; block_counter < bytes / sizeof(block); block_counter++) {
      block[0] = _mm_aesenc_si128(block[0], _mm_loadu_si128(ptr128++));
      block[1] = _mm_aesenc_si128(block[1], _mm_loadu_si128(ptr128++));
      block[2] = _mm_aesenc_si128(block[2], _mm_loadu_si128(ptr128++));
      block[3] = _mm_aesenc_si128(block[3], _mm_loadu_si128(ptr128++));
    }

    // process remaining AES blocks
    if (bytes & 32) {
      block[0] = _mm_aesenc_si128(block[0], _mm_loadu_si128(ptr128++));
      block[1] = _mm_aesenc_si128(block[1], _mm_loadu_si128(ptr128++));
    }

    if (bytes & 16) {
      block[2] = _mm_aesenc_si128(block[2], _mm_loadu_si128(ptr128++));
    }

    // AES sub-block processor
    const uint8_t* ptr8 = reinterpret_cast<const uint8_t*>(ptr128);
    if (bytes & 8) {
      __m128i b = _mm_set_epi64x(*reinterpret_cast<const uint64_t*>(ptr8), 0xa11202c9b468bea1);
      block[3] = _mm_aesenc_si128(block[3], b);
      ptr8 += 8;
    }

    if (bytes & 4) {
      __m128i b = _mm_set_epi32(0xb1293b33, 0x05418592, *reinterpret_cast<const uint32_t*>(ptr8),
                                0xd210d232);
      block[0] = _mm_aesenc_si128(block[0], b);
      ptr8 += 4;
    }

    if (bytes & 2) {
      __m128i b = _mm_set_epi16(0xbd3d, 0xc2b7, 0xb87c, 0x4715, 0x6a6c, 0x9527,
                                *reinterpret_cast<const uint16_t*>(ptr8), 0xac2e);
      block[1] = _mm_aesenc_si128(block[1], b);
      ptr8 += 2;
    }

    if (bytes & 1) {
      __m128i b = _mm_set_epi8(0xcc, 0x96, 0xed, 0x16, 0x74, 0xea, 0xaa, 0x03, 0x1e, 0x86, 0x3f,
                               0x24, 0xb2, 0xa8, *ptr8, 0x31);
      block[2] = _mm_aesenc_si128(block[2], b);
    }

    // indirectly mix hashing lanes
    const __m128i mix =
        _mm_xor_si128(_mm_xor_si128(block[0], block[1]), _mm_xor_si128(block[2], block[3]));
    block[0] = _mm_aesenc_si128(block[0], mix);
    block[1] = _mm_aesenc_si128(block[1], mix);
    block[2] = _mm_aesenc_si128(block[2], mix);
    block[3] = _mm_aesenc_si128(block[3], mix);

    // reduction from 512-bit block size to 128-bit hash
    __m128i hash = _mm_aesenc_si128(_mm_aesenc_si128(block[0], block[1]),
                                    _mm_aesenc_si128(block[2], block[3]));

    // this algorithm construction requires no less than one round to finalize
    return _mm_aesenc_si128(hash, _mm_set_epi64x(0x8e51ef21fabb4522, 0xe43d7a0656954b6c));
  }

  // NON-INCREMENTAL HYBRID ALGORITHM

  static __m128i Hash(const uint8_t* key, const size_t bytes,
                      __m128i initialize = _mm_setzero_si128()) {
    return bytes < 64 ? SmallKeyAlgorithm(key, bytes, initialize)
                      : LargeKeyAlgorithm(key, bytes, initialize);
  }

  // INCREMENTAL HYBRID ALGORITHM

  // Initialize a new incremental hashing object
  AquaHash(const __m128i initialize = _mm_setzero_si128())
      : block{_mm_xor_si128(initialize, _mm_set_epi64x(0xa11202c9b468bea1, 0xd75157a01452495b)),
              _mm_xor_si128(initialize, _mm_set_epi64x(0xb1293b3305418592, 0xd210d232c6429b69)),
              _mm_xor_si128(initialize, _mm_set_epi64x(0xbd3dc2b7b87c4715, 0x6a6c9527ac2e0e4e)),
              _mm_xor_si128(initialize, _mm_set_epi64x(0xcc96ed1674eaaa03, 0x1e863f24b2a8316a))},
        initialize(initialize), input_bytes(0) {
  }

  // Initialize an existing hashing object -- all previous state is destroyed
  void Initialize(const __m128i initialize = _mm_setzero_si128()) {
    this->initialize = initialize;
    this->input_bytes = 0;
    block[0] = _mm_xor_si128(initialize, _mm_set_epi64x(0xa11202c9b468bea1, 0xd75157a01452495b));
    block[1] = _mm_xor_si128(initialize, _mm_set_epi64x(0xb1293b3305418592, 0xd210d232c6429b69));
    block[2] = _mm_xor_si128(initialize, _mm_set_epi64x(0xbd3dc2b7b87c4715, 0x6a6c9527ac2e0e4e));
    block[3] = _mm_xor_si128(initialize, _mm_set_epi64x(0xcc96ed1674eaaa03, 0x1e863f24b2a8316a));
  }

  // Append key to existing hashing object state
  void Update(const uint8_t* key, size_t bytes) {
    assert(input_bytes != finalized);
    assert(bytes <= max_input && max_input - input_bytes >= bytes);

    if (bytes == 0)
      return;

    // input buffer may be partially filled
    if (input_bytes % sizeof(input)) {
      // pointer to first unused byte in input buffer
      uint8_t* ptr8 = reinterpret_cast<uint8_t*>(input) + (input_bytes % sizeof(input));

      // compute initial copy size from key to input buffer
      size_t copy_size = sizeof(input) - (input_bytes % sizeof(input));
      if (copy_size > bytes)
        copy_size = bytes;

      // append new key bytes to input buffer
      memcpy(ptr8, key, copy_size);
      input_bytes += copy_size;
      bytes -= copy_size;

      // input buffer not filled by update
      if (input_bytes % sizeof(input))
        return;

      // update key pointer to first byte not in the input buffer
      key += copy_size;

      // hash input buffer
      block[0] = _mm_aesenc_si128(block[0], input[0]);
      block[1] = _mm_aesenc_si128(block[1], input[1]);
      block[2] = _mm_aesenc_si128(block[2], input[2]);
      block[3] = _mm_aesenc_si128(block[3], input[3]);
    }

    input_bytes += bytes;

    // input buffer is empty
    const __m128i* ptr128 = reinterpret_cast<const __m128i*>(key);
    while (bytes >= sizeof(block)) {
      block[0] = _mm_aesenc_si128(block[0], _mm_loadu_si128(ptr128++));
      block[1] = _mm_aesenc_si128(block[1], _mm_loadu_si128(ptr128++));
      block[2] = _mm_aesenc_si128(block[2], _mm_loadu_si128(ptr128++));
      block[3] = _mm_aesenc_si128(block[3], _mm_loadu_si128(ptr128++));
      bytes -= sizeof(block);
    }

    // load remaining bytes into input buffer
    if (bytes)
      memcpy(input, ptr128, bytes);
  }

  // Generate hash from hashing object state. After finalization, the hashing
  // object is in an undefined state and must be initialized before any
  // subsequent calls on the object.
  __m128i Finalize() {
    assert(input_bytes != finalized);
    if (input_bytes < sizeof(block)) {
      __m128i hash = SmallKeyAlgorithm(reinterpret_cast<uint8_t*>(input), input_bytes, initialize);
      input_bytes = finalized;
      return hash;
    } else {
      // process remaining AES blocks
      if (input_bytes & 32) {
        block[0] = _mm_aesenc_si128(block[0], input[0]);
        block[1] = _mm_aesenc_si128(block[1], input[1]);
      }

      if (input_bytes & 16) {
        block[2] = _mm_aesenc_si128(block[2], input[2]);
      }

      // AES sub-block processor
      const uint8_t* ptr8 = reinterpret_cast<const uint8_t*>(&input[3]);
      if (input_bytes & 8) {
        __m128i b = _mm_set_epi64x(*reinterpret_cast<const uint64_t*>(ptr8), 0xa11202c9b468bea1);
        block[3] = _mm_aesenc_si128(block[3], b);
        ptr8 += 8;
      }

      if (input_bytes & 4) {
        __m128i b = _mm_set_epi32(0xb1293b33, 0x05418592, *reinterpret_cast<const uint32_t*>(ptr8),
                                  0xd210d232);
        block[0] = _mm_aesenc_si128(block[0], b);
        ptr8 += 4;
      }

      if (input_bytes & 2) {
        __m128i b = _mm_set_epi16(0xbd3d, 0xc2b7, 0xb87c, 0x4715, 0x6a6c, 0x9527,
                                  *reinterpret_cast<const uint16_t*>(ptr8), 0xac2e);
        block[1] = _mm_aesenc_si128(block[1], b);
        ptr8 += 2;
      }

      if (input_bytes & 1) {
        __m128i b = _mm_set_epi8(0xcc, 0x96, 0xed, 0x16, 0x74, 0xea, 0xaa, 0x03, 0x1e, 0x86, 0x3f,
                                 0x24, 0xb2, 0xa8, *ptr8, 0x31);
        block[2] = _mm_aesenc_si128(block[2], b);
      }

      // indirectly mix hashing lanes
      const __m128i mix =
          _mm_xor_si128(_mm_xor_si128(block[0], block[1]), _mm_xor_si128(block[2], block[3]));
      block[0] = _mm_aesenc_si128(block[0], mix);
      block[1] = _mm_aesenc_si128(block[1], mix);
      block[2] = _mm_aesenc_si128(block[2], mix);
      block[3] = _mm_aesenc_si128(block[3], mix);

      // reduction from 512-bit block size to 128-bit hash
      __m128i hash = _mm_aesenc_si128(_mm_aesenc_si128(block[0], block[1]),
                                      _mm_aesenc_si128(block[2], block[3]));

      // this algorithm construction requires no less than 1 round to finalize
      input_bytes = finalized;
      return _mm_aesenc_si128(hash, _mm_set_epi64x(0x8e51ef21fabb4522, 0xe43d7a0656954b6c));
    }
  }
};

#endif  // #ifndef AQUAHASH_H

// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/numeric/bits.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#include "base/endian.h"

namespace base {

namespace flit {

/**
FLIT64 - faster alternative to VarInt. Credit goes to @pascaldekloe who released the code to
public domain: https://github.com/pascaldekloe/flit

The 64-bit unsigned version encodes an integer in 1 to 9 octets.

The first octet advertises the number of octets following with the trailing
zero count. Any remaining bits hold the least significant data bits and the
following octets, if any at all, hold the rest in little-endian order.

| Total Size | First Bits  | Range                               |
|:-----------|:------------|:------------------------------------|
| 1 octet    | `xxxx xxx1` | 7-bit (128)                         |
| 2 octets   | `xxxx xx10` | 14-bit (16'384)                     |
| 3 octets   | `xxxx x100` | 21-bit (2'097'152)                  |
| 4 octets   | `xxxx 1000` | 28-bit (268'435'456)                |
| 5 octets   | `xxx1 0000` | 35-bit (34'359'738'368)             |
| 6 octets   | `xx10 0000` | 42-bit (4'398'046'511'104)          |
| 7 octets   | `x100 0000` | 49-bit (562'949'953'421'312)        |
| 8 octets   | `1000 0000` | 56-bit (72'057'594'037'927'936)     |
| 9 octets   | `0000 0000` | 64-bit (18'446'744'073'709'551'616) |

Encoding *should* pick the smallest range capable to hold the value.

*/

static_assert(LE::IsLittleEndian(), "Current flit implentation does not suppport big endian");

namespace detail {

// Division by 7, it's a bit overkill for saving a couple of cpu instructions but if
// we are into bit hacks lets do it. We found the constants from godbolt and removed the fixes
// for high range of uint8_t since we only need [0, 64].
constexpr inline uint8_t Div7Small(uint8_t val) {
  return (uint16_t(val) * 37) >> 8;
}

template <typename F> constexpr bool TestRange(unsigned from, unsigned to, F f) {
  for (unsigned i = from; i < to; ++i) {
    if (!f(i))
      return false;
  }
  return true;
}

constexpr bool TestDiv7Small() {
  return TestRange(1, 70, [](auto i) { return Div7Small(i) == i / 7; });
}

constexpr bool TestXorMinus() {
  return TestRange(0, 64, [](auto i) { return (63 ^ i) == (63 - i); });
}

// C++ switch/case dispatching. Provides optimal code similar to switch case.
template <unsigned I> struct dispatch_impl {
  static_assert(I > 1);

  template <typename F> static void visit(unsigned len, F&& fun) {
    if (len == I)
      fun(I);
    else
      dispatch_impl<I - 1>::visit(len, std::move(fun));
  }
};

template <> struct dispatch_impl<1> {
  template <typename F> static void visit(unsigned len, F fun) {
    assert(len == 1);
    fun(1);
  }
};

static_assert(TestDiv7Small());
static_assert(TestXorMinus());

}  // namespace detail

template <typename T> constexpr uint32_t EncodingLength(T v) {
  uint8_t index = (sizeof(v) * 8 - 1) ^ absl::countl_zero(v | 1);
  uint8_t num_bytes = detail::Div7Small(index);

  return 1 + num_bytes;
}

static_assert(detail::TestRange(1, 9, [](unsigned i) {
  return EncodingLength(1ULL << (7 * i)) == i + 1;
}));

static_assert(detail::TestRange(1, 9, [](unsigned i) {
  return EncodingLength((1ULL << (7 * i)) - 1) == i;
}));



// Decodes buf into v and returns number of bytes needed to encode the number.
// Requires that src points to buffer has memory access to at least 8 bytes
// even if the encoding smaller and it assumes that the input is valid,
// i.e. the data was encoded by flit.
// The generated code is verified with https://godbolt.org/
// Work for decoding uint32 as well, however the result will be stored into uint64.
inline unsigned Parse64Fast(const uint8_t* src, uint64_t* v) {
  // It's better use local variables then reuse output argument - compiler can substitute them
  // with registers.
  // compiler is smart enough to replace it with a single load instruction.
  uint64_t val = LE::LoadT<uint64_t>(src);  // memcpy to val

  if ((val & 0xff) == 0) {  // 9 bytes - 64 bit
    ++src;
    *v = LE::LoadT<uint64_t>(src);
    return 9;
  }

  uint32_t index = absl::countr_zero(val);

#define USE_MASK 0

#if USE_MASK
  static constexpr uint64_t mask[8] = {
      0xff,         0xffff,         0xffffff,         0xffffffff,
      0xffffffffff, 0xffffffffffff, 0xffffffffffffff, 0xffffffffffffffff,
  };

  val &= mask[index];

  ++index;
#else
  ++index;

  val &= ((1ULL << index * 8) - 1);
#endif

  *v = val >> index;

#undef USE_MASK

  return index;
}

// Returns 0 if the input is invalid, number of parsed bytes otherwise.
inline unsigned Parse64Safe(const uint8_t* begin, const uint8_t* end, uint64_t* v) {
  size_t sz = end - begin;

  if (*begin == 0) {
    if (sz < 8)
      return 0;  // invalid input, must be at least 9.
    ++begin;
    *v = LE::LoadT<uint64_t>(begin);

    if (*v < (1ULL << 56))  // consistency check.
      return 0;
    return 9;
  }

  unsigned index = absl::countr_zero(*begin) + 1;
  if (sz < index)
    return 0;

  detail::dispatch_impl<8>::visit(index, [&](unsigned len) { memcpy(v, begin, len); });

  *v &= ((1ULL << index * 8) - 1);
  *v >>= index;

  return index;
}

// Requires at least EncodeLength(v) bytes in dest.
template <typename T> unsigned EncodeT(T v, uint8_t* dest) {
  static_assert(std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
                "Only uint32/64 supported");
  constexpr T kLarge = T(1) << (7 * sizeof(v));

  if (v >= kLarge) {
    *dest++ = (1 << sizeof(T)) & 0xFF;

    LE::StoreT(v, dest);
    return sizeof(v) + 1;
  }

  static_assert(absl::countl_zero(uint8_t(128)) == 0);
  static_assert(absl::countl_zero(128u) == 24);
  static_assert(absl::countl_zero(1u) == 31);

  uint32_t enc_len = EncodingLength(v);

  // prefix
  v = ((v << 1) + 1) << (enc_len - 1);

  // Compares num_bytes to constants and calls memcpy with a constant.
  // Subsequently, provides an effient code.
  detail::dispatch_impl<sizeof(T)>::visit(enc_len, [&](unsigned len) { memcpy(dest, &v, len); });

  return enc_len;
}

// May write beyond the encoding length.
// Specifically, requires Traits<T>::kMaxSize bytes in dest to work properly.
template <typename T> unsigned EncodeTFast(T v, uint8_t* dest) {
  static_assert(std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
                "Only uint32/64 supported");
  constexpr T kLarge = T(1) << (7 * sizeof(v));

  if (v >= kLarge) {
    *dest++ = (1 << sizeof(T)) & 0xFF;

    LE::StoreT(v, dest);
    return sizeof(v) + 1;
  }

  uint32_t enc_len = EncodingLength(v);

  // add prefix header.
  v = ((v << 1) + 1) << (enc_len - 1);

  LE::StoreT(v, dest);
  return enc_len;
}

template <typename T> unsigned ParseLengthT(const uint8_t* src) {
  uint8_t val = *src;

  constexpr uint8_t kMaxLenHeader = (1U << sizeof(T)) & 0xFF;
  if ((val & 0xFF) == kMaxLenHeader) {
    return sizeof(T) + 1;
  }
  return 1 + absl::countr_zero(val);
}

// Result is undefined on invalid input.
// Also requires at least 4/8 bytes of accessible memory at src.
template <typename T> unsigned ParseT(const uint8_t* src, T* dest) {
  static_assert(std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
                "Only uint32/64 supported");
  T val = LE::LoadT<T>(src);
  constexpr uint8_t kMaxLenHeader = (1 << sizeof(T)) & 0xFF;

  if ((val & 0xFF) == kMaxLenHeader) {
    ++src;
    *dest = LE::LoadT<T>(src);
    return sizeof(T) + 1;
  }

  uint32_t index = absl::countr_zero(val) + 1;
  val &= ((T(1) << index * 8) - 1);
  *dest = val >> index;
  return index;
}

// Encodes v into buf and returns pointer to the next byte.
// dest must have at least 9 bytes.
inline unsigned Encode64(uint64_t v, uint8_t* dest) {
  return EncodeT<uint64_t>(v, dest);
}

// dest must have at least 5 bytes.
inline unsigned Encode32(uint32_t v, uint8_t* dest) {
  return EncodeT<uint32_t>(v, dest);
}

template <typename T> struct Trait {
  static_assert(std::numeric_limits<T>::is_integer, "T must be integer");

  static constexpr size_t kMaxSize = sizeof(T) + 1 + (sizeof(T) - 1) / 8;
};

}  // namespace flit

// From protobuf documentation:
// ZigZag Transform:  Encodes signed integers so that they can be
// effectively used with varint encoding.
//
// varint operates on unsigned integers, encoding smaller numbers into
// fewer bytes.  If you try to use it on a signed integer, it will treat
// this number as a very large unsigned integer, which means that even
// small signed numbers like -1 will take the maximum number of bytes
// (10) to encode.  ZigZagEncode() maps signed integers to unsigned
// in such a way that those with a small absolute value will have smaller
// encoded values, making them appropriate for encoding using varint.
//
//       int32 ->     uint32
// -------------------------
//           0 ->          0
//          -1 ->          1
//           1 ->          2
//          -2 ->          3
//         ... ->        ...
//  2147483647 -> 4294967294
// -2147483648 -> 4294967295
//

// For unsigned types ZigZag is just an identity.
template <typename T>
constexpr typename std::enable_if_t<std::is_unsigned<T>::value, T> ZigZagEncode(T t) {
  return t;
}

// For signed types ZigZag transforms numbers into their unsigned type.
template <typename T>
constexpr typename std::enable_if_t<std::is_signed<T>::value, typename std::make_unsigned_t<T>>
ZigZagEncode(T t) {
  using UT = typename std::make_unsigned_t<T>;

  // puts MSB bit of input into LSB of the unsigned result.
  return (UT(t) << 1) ^ (t >> (sizeof(t) * 8 - 1));
}

static_assert(ZigZagEncode<int32_t>(1) == 2, "");
static_assert(ZigZagEncode<int32_t>(-2) == 3, "");  // (1110 << 1) ^ 1111

template <typename T> typename std::enable_if_t<std::is_unsigned_v<T>, T> ZigZagDecode(T t) {
  return t;
}

template <typename T>
typename std::enable_if_t<std::is_signed_v<T>, T> ZigZagDecode(typename std::make_unsigned_t<T> t) {
  return (t >> 1) ^ -static_cast<T>(t & 1);
}

}  // namespace base

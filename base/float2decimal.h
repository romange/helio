// Copyright 2017 Alexander Bolz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Implements the Grisu2 algorithm for binary to decimal floating-point
// conversion.
//
// This implementation is a slightly modified version of the reference
// implementation by Florian Loitsch which can be obtained from
// http://florian.loitsch.com/publications (bench.tar.gz)
// The original license can be found at the end of this file.
//
// References:
//
// [1]  Loitsch, "Printing Floating-Point Numbers Quickly and Accurately with Integers",
//      Proceedings of the ACM SIGPLAN 2010 Conference on Programming Language Design and
//      Implementation, PLDI 2010
// [2]  Burger, Dybvig, "Printing Floating-Point Numbers Quickly and Accurately",
//      Proceedings of the ACM SIGPLAN 1996 Conference on Programming Language Design and
//      Implementation, PLDI 1996

// Header-only U+1F926 U+1F937

#pragma once

#include <cassert>
#include <cstring>
#include <utility>

#include "base/ieeefloat.h"
#include "base/bits.h"

namespace base {

namespace dtoa {

template <typename Float> char* ToString(Float value, char* dest);

// TODO: to implement it.
// Any non-special float number will have at most 17 significant decimal digits.
// See https://en.wikipedia.org/wiki/Double-precision_floating-point_format#IEEE_754_double-precision_binary_floating-point_format:_binary64
// So for non-nans we should always be able to convert float to int64 integer/exponent pair
// without loosing precision.
// The exception is negative zero floating number - this will be translated to just 0.
// The function returns integer val, it's decimal length not including sign and the exponent
// such that the resulting float := val * 10^exponent.
// For example, for -0.05 it returns: (-5, -2, 1)
template <typename Float> bool ToDecimal(Float value, int64_t* val, int16_t* exponent,
                                         uint16_t* decimal_len);

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------

struct Fp {                                    // f * 2^e
  static constexpr int const kPrecision = 64;  // = q

  uint64_t f;
  int e;

  constexpr Fp() : f(0), e(0) {}
  constexpr Fp(uint64_t f_, int e_) : f(f_), e(e_) {}

  // Returns *this - y.
  // Requires: *this.e == y.e and x.f >= y.f
  Fp Sub(Fp y) const {
    assert(e == y.e);
    assert(f >= y.f);

    return Fp(f - y.f, e);
  }

  // Returns *this * y.
  // The result is rounded. (Only the upper q bits are returned.)
  Fp Mul(Fp y) const;

  // Normalize x such that the significand is >= 2^(q-1).
  // Requires: x.f != 0
  void Normalize() {
    unsigned const leading_zeros = absl::countl_zero(f);
    f <<= leading_zeros;
    e -= leading_zeros;
  }

  // Normalize x such that the result has the exponent E.
  // Requires: e >= this->e and the upper (e - this->e) bits of this->f must be zero.
  void NormalizeTo(int e);
};

// If we compute float := f * 2 ^ (e - bias), where f, e are integers (no implicit 1. point)
  // In that case we need to divide f by 2 ^ kSignificandLen, which in fact increases exp bias:
  // res : = (f / 2^kSignificandLen) * 2^(1 - kExponentBias) =
  //         f * 2 (1 - kExponentBias - kSignificandLen)
  // So we redefine kExpBias for this equation.
template<typename Float> struct FpTraits {  // explicit division of mantissa by 2^mantissalen.
  using IEEEType = IEEEFloat<Float>;
  static constexpr int const kExpBias = IEEEType::kExponentBias + IEEEType::kSignificandLen;

  static constexpr int const kDenormalExp = 1 - kExpBias;
  static constexpr int const kMaxExp = IEEEType::kMaxExponent - kExpBias;


  static Float FromFP(uint64_t f, int e) {
    while (f > IEEEType::kHiddenBit + IEEEType::kSignificandMask) {
      f >>= 1;
      e++;
    }

    if (e >= kMaxExp) {
      return std::numeric_limits<Float>::infinity();
    }

    if (e < kDenormalExp) {
      return 0.0;
    }

    while (e > kDenormalExp && (f & IEEEType::kHiddenBit) == 0) {
      f <<= 1;
      e--;
    }

    uint64_t biased_exponent;
    if (e == kDenormalExp && (f & IEEEType::kHiddenBit) == 0)
      biased_exponent = 0;
    else
      biased_exponent = static_cast<uint64_t>(e + kExpBias);

    return IEEEType(biased_exponent, f).value;
  }
};

// Returns (filled buffer len, decimal_exponent) pair.
std::pair<unsigned, int> Grisu2(Fp m_minus, Fp v, Fp m_plus, char* buf);

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------

struct BoundedFp {
  Fp w;
  Fp minus;
  Fp plus;
};

//
// Computes the boundaries m- and m+ of the floating-point value v.
//
// Determine v- and v+, the floating-point predecessor and successor if v,
// respectively.
//
//      v- = v - 2^e        if f != 2^(p-1) or e != e_min                    (A)
//         = v - 2^(e-1)    if f == 2^(p-1) and e > e_min                    (B)
//
//      v+ = v + 2^e
//
// Let m- = (v- + v) / 2 and m+ = (v + v+) / 2. All real numbers _strictly_
// between m- and m+ round to v, regardless of how the input rounding algorithm
// breaks ties.
//
//      ---+-------------+-------------+-------------+-------------+---      (A)
//         v-            m-            v             m+            v+
//
//      -----------------+------+------+-------------+-------------+---      (B)
//                       v-     m-     v             m+            v+
//
// Note that m- and m+ are (by definition) not representable with precision p
// and we therefore need some extra bits of precision.
//
template <typename Float>
inline BoundedFp ComputeBoundedFp(Float v_ieee) {
  using IEEEType = IEEEFloat<Float>;

  //
  // Convert the IEEE representation into a DiyFp.
  //
  // If v is denormal:
  //      value = 0.F * 2^(1 - E_bias) = (F) * 2^(1 - E_bias - (p-1))
  // If v is normalized:
  //      value = 1.F * 2^(E - E_bias) = (2^(p-1) + F) * 2^(E - E_bias - (p-1))
  //
  IEEEType const v_ieee_bits(v_ieee);

  // biased exponent.
  uint64_t const E = v_ieee_bits.ExponentBits();
  uint64_t const F = v_ieee_bits.SignificandBits();

  Fp v = (E == 0)  // denormal?
        ? Fp(F, FpTraits<Float>::kDenormalExp)
        : Fp(IEEEType::kHiddenBit + F, static_cast<int>(E) - FpTraits<Float>::kExpBias);

  //
  // v+ = v + 2^e = (f + 1) * 2^e and therefore
  //
  //      m+ = (v + v+) / 2
  //         = (2*f + 1) * 2^(e-1)
  //
  Fp m_plus = Fp(2 * v.f + 1, v.e - 1);

  //
  // If f != 2^(p-1), then v- = v - 2^e = (f - 1) * 2^e and
  //
  //      m- = (v- + v) / 2
  //         = (2*f - 1) * 2^(e-1)
  //
  // If f = 2^(p-1), then the next smaller _normalized_ floating-point number
  // is actually v- = v - 2^(e-1) = (2^p - 1) * 2^(e-1) and therefore
  //
  //      m- = (4*f - 1) * 2^(e-2)
  //
  // The exception is the smallest normalized floating-point number
  // v = 2^(p-1) * 2^e_min. In this case the predecessor is the largest
  // denormalized floating-point number: v- = (2^(p-1) - 1) * 2^e_min and then
  //
  //      m- = (2*f - 1) * 2^(e-1)
  //
  // If v is denormal, v = f * 2^e_min and v- = v - 2^e = (f - 1) * 2^e and
  // again
  //
  //      m- = (2*f - 1) * 2^(e-1)
  //
  // Note: 0 is not a valid input for Grisu and in case v is denormal:
  // f != 2^(p-1).
  //
  // For IEEE floating-point numbers not equal to 0, the condition f = 2^(p-1)
  // is equivalent to F = 0, and for the smallest normalized number E = 1.
  // For denormals E = 0 (and F != 0).
  //SignificandBits
  Fp m_minus = (F == 0 && E > 1) ? Fp(4 * v.f - 1, v.e - 2) : Fp(m_plus.f - 2, m_plus.e);

  //
  // Determine the normalized w+ = m+.
  //
  m_plus.Normalize();
  v.Normalize();

  //
  // Determine w- = m- such that e_(w-) = e_(w+).
  //
  m_minus.NormalizeTo(m_plus.e);

  return BoundedFp{v, m_minus, m_plus};
}

char* FormatBuffer(char* buf, int k, int n);

//
// Generates a decimal representation of the input floating-point number V in
// BUF.
//
// The result is formatted like JavaScript's ToString applied to a number type.
// Except that:
// An argument representing an infinity is converted to "inf" or "-inf".
// An argument representing a NaN is converted to "nan".
//
// This function never writes more than 25 characters to BUF and returns an
// iterator pointing past-the-end of the decimal representation.
// The result is guaranteed to round-trip (when read back by a correctly
// rounding implementation.)
//
// Note:
// The result is not null-terminated.
//

// next should point to at least 25 bytes.
template <typename Float> char* ToString(Float value, char* dest) {
  using IEEEType = IEEEFloat<Float>;
  static_assert(Fp::kPrecision >= IEEEType::kPrecision + 3, "insufficient precision");

  constexpr char kNaNString[] = "NaN";       // assert len <= 25
  constexpr char kInfString[] = "Infinity";  // assert len <= 24
  static_assert(sizeof(kNaNString) == 4, "");

  IEEEType const v(value);
  // assert(!v.IsNaN());
  // assert(!v.IsInf());

  if (v.IsNaN()) {
    std::memcpy(dest, kNaNString, sizeof(kNaNString) - 1);
    return dest + sizeof(kNaNString) - 1;
  }
  if (v.IsNegative()) {
    *dest++ = '-';
  }

  if (v.IsInf()) {
    std::memcpy(dest, kInfString, sizeof(kInfString) - 1);
    return dest + sizeof(kInfString) - 1;
  }

  if (v.IsZero()) {
    *dest++ = '0';
  } else {
    BoundedFp w = ComputeBoundedFp(v.Abs());

    // Compute the decimal digits of v = digits * 10^decimal_exponent.
    // len is the length of the buffer, i.e. the number of decimal digits
    std::pair<unsigned, int> res = Grisu2(w.minus, w.w, w.plus, dest);

    // Compute the position of the decimal point relative to the start of the buffer.
    int n = res.first + res.second;

    dest = FormatBuffer(dest, res.first, n);
    // (len <= 1 + 24 = 25)
  }

  return dest;
}

template <typename Float> bool ToDecimal(Float value, int64_t* val, int16_t* exponent,
                                         uint16_t* decimal_len) {
  static_assert(std::is_floating_point<Float>::value, "");

  using IEEEType = IEEEFloat<Float>;
  const IEEEType v(value);
  if (v.IsNaN() || v.IsInf())
    return false;
  int64_t decimal = 0;

  if (v.IsZero()) {
    *exponent = 0;
    *decimal_len = 1;
  } else {
    BoundedFp w = ComputeBoundedFp(v.Abs());

    // Compute the decimal digits of v = digits * 10^decimal_exponent.
    // len is the length of the buffer, i.e. the number of decimal digits
    char dest[20];

    std::pair<unsigned, int> res = Grisu2(w.minus, w.w, w.plus, dest);
    assert(res.first < 18);
    for (unsigned i = 0; i < res.first; ++i) {
      decimal = decimal * 10 + (dest[i] - '0');
    }
    *decimal_len = res.first;
    if (v.IsNegative())
      decimal = -decimal;
    *exponent = res.second;
  }
  *val = decimal;

  return true;
}

inline Fp Fp::Mul(Fp y) const {
// Computes:
//  f = round((x.f * y.f) / 2^q)
//  e = x.e + y.e + q
  __extension__ using Uint128 = unsigned __int128;

  Uint128 const p = Uint128{f} * Uint128{y.f};

  uint64_t h = static_cast<uint64_t>(p >> 64);
  uint64_t l = static_cast<uint64_t>(p);
  h += l >> 63;  // round, ties up: [h, l] += 2^q / 2

  return Fp(h, e + y.e + 64);
}

inline void Fp::NormalizeTo(int new_e) {
  int const delta = e - new_e;

  assert(delta >= 0);
  assert(((this->f << delta) >> delta) == this->f);
  f <<= delta;
  e = new_e;
}

extern const uint64_t powers_of_10_internal[];

}  // namespace dtoa
}  // namespace util

// http://florian.loitsch.com/publications (bench.tar.gz)
//
// Copyright (c) 2009 Florian Loitsch
//
//   Permission is hereby granted, free of charge, to any person
//   obtaining a copy of this software and associated documentation
//   files (the "Software"), to deal in the Software without
//   restriction, including without limitation the rights to use,
//   copy, modify, merge, publish, distribute, sublicense, and/or sell
//   copies of the Software, and to permit persons to whom the
//   Software is furnished to do so, subject to the following
//   conditions:
//
//   The above copyright notice and this permission notice shall be
//   included in all copies or substantial portions of the Software.
//
//   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
//   OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
//   HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//   WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//   FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//   OTHER DEALINGS IN THE SOFTWARE.

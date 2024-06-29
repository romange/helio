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
// Header-only U+1F926 U+1F937
// Modified by Roman Gershman
#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>

namespace base {

template <typename Float>
struct IEEEFloat {
  using NL = std::numeric_limits<Float>;

  static constexpr bool const is_single = (NL::digits == 24) && (NL::max_exponent) == 128;

  static constexpr bool const is_double = (NL::digits == 53) && (NL::max_exponent == 1024);

  static_assert(NL::is_iec559 && (is_single || is_double), "IEEE-754 implementation required");

  using Uint = typename std::conditional<is_single, uint32_t, uint64_t>::type;

  static_assert(sizeof(Float) == sizeof(Uint), "Size mismatch");

  static constexpr unsigned const kPrecision = is_single ? 24 : 53;  // = p (includes the hidden bit!)
  static constexpr unsigned const kSignificandLen = kPrecision - 1;
  static constexpr unsigned const kMaxExponent = is_single ? 0xFF : 0x7FF;
  static constexpr unsigned const kExponentBias = kMaxExponent / 2;
  static constexpr Uint const kHiddenBit = Uint{1} << kSignificandLen;
  static constexpr Uint const kSignMask = Uint{1} << (is_single ? 31 : 63);

  static constexpr Uint const kExponentMask = Uint{kMaxExponent} << kSignificandLen;
  static constexpr Uint const kSignificandMask = kHiddenBit - 1;

  union {  // XXX: memcpy?
    Float value;
    Uint bits;
  };

  constexpr explicit IEEEFloat(Float value_) : value(value_) {}
  constexpr explicit IEEEFloat(Uint bits_) : bits(bits_) {}
  constexpr IEEEFloat(Uint exponent, Uint significand)
    : bits(exponent << kSignificandLen | (significand & kSignificandMask)) {}

  constexpr Uint ExponentBits() const { return (bits & kExponentMask) >> (kPrecision - 1); }

  constexpr Uint SignificandBits() const { return (bits & kSignificandMask); }

  // Returns true if the sign-bit is set.
  constexpr bool IsNegative() const { return (bits & kSignMask) != 0; }

  // Returns true if this value is -0 or +0.
  bool IsZero() const { return (bits & ~kSignMask) == 0; }

  // Returns true if this value is denormal or 0.
  bool IsDenormal() const { return (bits & kExponentMask) == 0; }

  // Returns true if this value is NaN
  bool IsNaN() const {
    return (bits & kExponentMask) == kExponentMask && (bits & kSignificandMask) != 0;
  }

  // Returns true if this value is -Inf or +Inf.
  bool IsInf() const {
    return (bits & kExponentMask) == kExponentMask && (bits & kSignificandMask) == 0;
  }

  // Returns this value with the sign-bit cleared.
  Float Abs() const { return IEEEFloat(bits & ~kSignMask).value; }

};


}  // namespace util

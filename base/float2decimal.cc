// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/float2decimal.h"
#include "base/logging.h"

#define FAST_DTOA_UNREACHABLE() __builtin_unreachable();

namespace base {

namespace dtoa {

constexpr uint64_t powers_of_10_internal[] = {
        0,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000,
        100000000000,
        1000000000000,
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000U
};

namespace {

constexpr unsigned int BitSetRight(uint32_t v) {
  return __builtin_clz(v) ^ 31;
}

// Returns a number between 1 and 10.
inline unsigned CountDecimalDigit32(uint32_t n) {
  unsigned long i = BitSetRight(n | 1);
  uint32_t t = (i + 1) * 1233 >> 12;
  return t - (n < powers_of_10_internal[t]) + 1;
}

// n must be less or equal to 20.
inline uint64_t Power10(unsigned n) {
  return n == 0 ? 1 : powers_of_10_internal[n];
}

//
// Given a (normalized) floating-point number v and its neighbors m- and m+
//
//      ---+---------------------------+---------------------------+---
//         m-                          v                           m+
//
// Grisu first scales the input number w, and its boundaries w- and w+, by an
// approximate power-of-ten c ~= 10^-k (which needs to be precomputed using
// high-precision arithmetic and stored in a table) such that the exponent of
// the products lies within a certain range [alpha, gamma]. It then remains to
// produce the decimal digits of a DiyFp number M = f * 2^e, where
// alpha <= e <= gamma.
//
// The choice of alpha and gamma determines the digit generation procedure and
// the size of the look-up table (or vice versa...) and depends on the
// extended precision q.
//
// In other words, given normalized w, Grisu needs to find a (normalized) cached
// power-of-ten c, such that the exponent of the product c * w = f * 2^e
// satisfies (Definition 3.2)
//
//      alpha <= e = e_c + e_w + q <= gamma
//
// or
//
//      f_c * f_w * 2^alpha <= f_c 2^(e_c) * f_w 2^(e_w) * 2^q
//                          <= f_c * f_w * 2^gamma
//
// Since c and w are normalized, i.e. 2^(q-1) <= f < 2^q, this implies
//
//      2^(q-1) * 2^(q-1) * 2^alpha <= c * w * 2^q < 2^q * 2^q * 2^gamma
//
// or
//
//      2^(q - 2 + alpha) <= c * w < 2^(q + gamma)
//
// The distance (gamma - alpha) should be as large as possible in order to make
// the table as small as possible, but the digit generation procedure should
// still be efficient.
//
// Assume q = 64 and e < 0. The idea is to cut the number c * w = f * 2^e into
// two parts, which can be processed independently: An integral part p1, and a
// fractional part p2:
//
//      f * 2^e = ( (f div 2^-e) * 2^-e + (f mod 2^-e) ) * 2^e
//              = (f div 2^-e) + (f mod 2^-e) * 2^e
//              = p1 + p2 * 2^e
//
// The conversion of p1 into decimal form requires some divisions and modulos by
// a power-of-ten. These operations are faster for 32-bit than for 64-bit
// integers, so p1 should ideally fit into a 32-bit integer. This be achieved by
// choosing
//
//      -e >= 32   or   e <= -32 := gamma
//
// In order to convert the fractional part
//
//      p2 * 2^e = d[-1] / 10^1 + d[-2] / 10^2 + ... + d[-k] / 10^k
//
// into decimal form, the fraction is repeatedly multiplied by 10 and the digits
// d[-i] are extracted in order:
//
//      (10 * p2) div 2^-e = d[-1]
//      (10 * p2) mod 2^-e = d[-2] / 10^1 + ... + d[-k] / 10^(k-1)
//
// The multiplication by 10 must not overflow. For this it is sufficient to have
//
//      10 * p2 < 16 * p2 = 2^4 * p2 <= 2^64.
//
// Since p2 = f mod 2^-e < 2^-e one may choose
//
//      -e <= 60   or   e >= -60 := alpha
//
// Different considerations may lead to different digit generation procedures
// and different values of alpha and gamma...
//
constexpr int const kAlpha = -60;
constexpr int const kGamma = -32;


// Grisu needs to find a(normalized) cached power-of-ten c, such that the
// exponent of the product c * w = f * 2^e satisfies (Definition 3.2)
//
//      alpha <= e = e_c + e_w + q <= gamma
//
// For IEEE double precision floating-point numbers v converted into a DiyFp's
// w = f * 2^e,
//
//      e >= -1022      (min IEEE exponent)
//           -52        (IEEE significand size)
//           -52        (possibly normalize denormal IEEE numbers)
//           -11        (normalize the DiyFp)
//         = -1137
//
// and
//
//      e <= +1023      (max IEEE exponent)
//           -52        (IEEE significand size)
//           -11        (normalize the DiyFp)
//         = 960
//
// (For IEEE single precision the exponent range is [-196, 80].)
//
// Now
//
//      alpha <= e_c + e + q <= gamma
//          ==> f_c * 2^alpha <= c * 2^e * 2^q
//
// and since the c's are normalized, 2^(q-1) <= f_c,
//
//          ==> 2^(q - 1 + alpha) <= c * 2^(e + q)
//          ==> 2^(alpha - e - 1) <= c
//
// If c were an exakt power of ten, i.e. c = 10^k, one may determine k as
//
//      k = ceil( log_10( 2^(alpha - e - 1) ) )
//        = ceil( (alpha - e - 1) * log_10(2) )
//
// (From the paper)
// "In theory the result of the procedure could be wrong since c is rounded, and
// the computation itself is approximated [...]. In practice, however, this
// simple function is sufficient."
//
// The difference of the decimal exponents of adjacent table entries must be
// <= floor( (gamma - alpha) * log_10(2) ) = 8.

struct CachedPower {  // c = f * 2^e ~= 10^k
  uint64_t f;
  int e;
  int k;
};

// Returns a cached power-of-ten c, such that alpha <= e_c + e + q <= gamma.
CachedPower GetCachedPowerForBinaryExponent(int e) {
  // NB:
  // Actually this function returns c, such that -60 <= e_c + e + 64 <= -34.

  static constexpr CachedPower const kCachedPowers[] = {
      {0xAB70FE17C79AC6CA, -1060, -300},  // -1060 + 960 + 64 = -36
      {0xFF77B1FCBEBCDC4F, -1034, -292}, {0xBE5691EF416BD60C, -1007, -284},
      {0x8DD01FAD907FFC3C, -980, -276},  {0xD3515C2831559A83, -954, -268},
      {0x9D71AC8FADA6C9B5, -927, -260},  {0xEA9C227723EE8BCB, -901, -252},
      {0xAECC49914078536D, -874, -244},  {0x823C12795DB6CE57, -847, -236},
      {0xC21094364DFB5637, -821, -228},  {0x9096EA6F3848984F, -794, -220},
      {0xD77485CB25823AC7, -768, -212},  {0xA086CFCD97BF97F4, -741, -204},
      {0xEF340A98172AACE5, -715, -196},  {0xB23867FB2A35B28E, -688, -188},
      {0x84C8D4DFD2C63F3B, -661, -180},  {0xC5DD44271AD3CDBA, -635, -172},
      {0x936B9FCEBB25C996, -608, -164},  {0xDBAC6C247D62A584, -582, -156},
      {0xA3AB66580D5FDAF6, -555, -148},  {0xF3E2F893DEC3F126, -529, -140},
      {0xB5B5ADA8AAFF80B8, -502, -132},  {0x87625F056C7C4A8B, -475, -124},
      {0xC9BCFF6034C13053, -449, -116},  {0x964E858C91BA2655, -422, -108},
      {0xDFF9772470297EBD, -396, -100},  {0xA6DFBD9FB8E5B88F, -369, -92},
      {0xF8A95FCF88747D94, -343, -84},   {0xB94470938FA89BCF, -316, -76},
      {0x8A08F0F8BF0F156B, -289, -68},   {0xCDB02555653131B6, -263, -60},
      {0x993FE2C6D07B7FAC, -236, -52},   {0xE45C10C42A2B3B06, -210, -44},
      {0xAA242499697392D3, -183, -36},  // -183 + 80 + 64 = -39
      {0xFD87B5F28300CA0E, -157, -28},  //
      {0xBCE5086492111AEB, -130, -20},  //
      {0x8CBCCC096F5088CC, -103, -12},  //
      {0xD1B71758E219652C, -77, -4},    //
      {0x9C40000000000000, -50, 4},     //
      {0xE8D4A51000000000, -24, 12},    //
      {0xAD78EBC5AC620000, 3, 20},      //
      {0x813F3978F8940984, 30, 28},     //
      {0xC097CE7BC90715B3, 56, 36},     //
      {0x8F7E32CE7BEA5C70, 83, 44},     // 83 - 196 + 64 = -49
      {0xD5D238A4ABE98068, 109, 52},     {0x9F4F2726179A2245, 136, 60},
      {0xED63A231D4C4FB27, 162, 68},     {0xB0DE65388CC8ADA8, 189, 76},
      {0x83C7088E1AAB65DB, 216, 84},     {0xC45D1DF942711D9A, 242, 92},
      {0x924D692CA61BE758, 269, 100},    {0xDA01EE641A708DEA, 295, 108},
      {0xA26DA3999AEF774A, 322, 116},    {0xF209787BB47D6B85, 348, 124},
      {0xB454E4A179DD1877, 375, 132},    {0x865B86925B9BC5C2, 402, 140},
      {0xC83553C5C8965D3D, 428, 148},    {0x952AB45CFA97A0B3, 455, 156},
      {0xDE469FBD99A05FE3, 481, 164},    {0xA59BC234DB398C25, 508, 172},
      {0xF6C69A72A3989F5C, 534, 180},    {0xB7DCBF5354E9BECE, 561, 188},
      {0x88FCF317F22241E2, 588, 196},    {0xCC20CE9BD35C78A5, 614, 204},
      {0x98165AF37B2153DF, 641, 212},    {0xE2A0B5DC971F303A, 667, 220},
      {0xA8D9D1535CE3B396, 694, 228},    {0xFB9B7CD9A4A7443C, 720, 236},
      {0xBB764C4CA7A44410, 747, 244},    {0x8BAB8EEFB6409C1A, 774, 252},
      {0xD01FEF10A657842C, 800, 260},    {0x9B10A4E5E9913129, 827, 268},
      {0xE7109BFBA19C0C9D, 853, 276},    {0xAC2820D9623BF429, 880, 284},
      {0x80444B5E7AA7CF85, 907, 292},    {0xBF21E44003ACDD2D, 933, 300},
      {0x8E679C2F5E44FF8F, 960, 308},    {0xD433179D9C8CB841, 986, 316},
      {0x9E19DB92B4E31BA9, 1013, 324},  // 1013 - 1137 + 64 = -60
  };

  constexpr int const kCachedPowersSize = 79;
  constexpr int const kCachedPowersMinDecExp = -300;

  // This computation gives exactly the same results for k as
  //
  //      k = ceil((kAlpha - e - 1) * 0.30102999566398114)
  //
  // for |e| <= 1500, but doesn't require floating-point operations.
  // NB: log_10(2) ~= 78913 / 2^18
  assert(e >= -1500);
  assert(e <= 1500);
  int const f = kAlpha - e - 1;
  int const k = (f * 78913) / (1 << 18) + (f > 0);

  int const index = (-kCachedPowersMinDecExp + k + (8 - 1)) / 8;
  assert(index >= 0);
  assert(index < kCachedPowersSize);
  static_cast<void>(kCachedPowersSize);  // Fix warning.

  CachedPower const cached = kCachedPowers[index];
  assert(kAlpha <= cached.e + e + 64);
  assert(kGamma >= cached.e + e + 64);

  // XXX:
  // cached.k = kCachedPowersMinDecExp + 8*index

  return cached;
}

// For n != 0, returns k, such that 10^(k-1) <= n < 10^k.
// For n == 0, returns 1.
inline unsigned InitKappa(uint32_t n) {
  return CountDecimalDigit32(n);
}

inline void Grisu2Round(char& digit, uint64_t dist, uint64_t delta, uint64_t rest,
                        uint64_t ten_k) {
  // dist, delta, rest and ten_k all are the significands of
  // floating-point numbers with an exponent e.
  assert(dist <= delta);
  assert(rest <= delta);
  assert(ten_k > 0);

  //
  //               <--------------------------- delta ---->
  //                                  <---- dist --------->
  // --------------[------------------+-------------------]--------------
  //               w-                 w                   w+
  //
  //                                  10^k
  //                                <------>
  //                                       <---- rest ---->
  // --------------[------------------+----+--------------]--------------
  //                                  w    V
  //                                       = buf * 10^k
  //
  // ten_k represents a unit-in-the-last-place in the decimal representation
  // stored in buf.
  // Decrement buf by ten_k while this takes buf closer to w.
  //

  while (rest < dist && delta - rest >= ten_k &&
         (rest + ten_k < dist || dist - rest > rest + ten_k - dist)) {
    DCHECK_NE('0', digit);
    digit--;
    rest += ten_k;
  }
}

void SetDigits(uint32_t value, unsigned num_digits, char* buffer) {
  const char DIGITS[] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";

  buffer += num_digits;

  while (value >= 100) {
    // Integer division is slow so do it for a group of two digits instead
    // of for every digit. The idea comes from the talk by Alexandrescu
    // "Three Optimization Tips for C++".
    unsigned index = static_cast<unsigned>((value % 100) * 2);
    value /= 100;
    *--buffer = DIGITS[index + 1];
    *--buffer = DIGITS[index];
  }
  if (value < 10) {
    *--buffer = static_cast<char>('0' + value);
    return;
  }
  unsigned index = static_cast<unsigned>(value * 2);
  *--buffer = DIGITS[index + 1];
  *--buffer = DIGITS[index];
}

std::pair<unsigned, int> Grisu2DigitGen(char* buffer, int decimal_exponent,
  const Fp& M_minus, const Fp& w, const Fp& M_plus) {
  static constexpr char const* const kDigits = "0123456789";

  static_assert(kAlpha >= -60, "invalid parameter");
  static_assert(kGamma <= -32, "invalid parameter");

  assert(M_plus.e >= kAlpha);
  assert(M_plus.e <= kGamma);

  uint64_t delta = M_plus.f - M_minus.f;        // (significand of (w+ - w-), implicit exponent is e)
  uint64_t dist = M_plus.f - w.f;  // (significand of (w+ - w ), implicit exponent is e)

  //               <--------------------------- delta ---->
  //                                  <---- dist --------->
  // --------------[------------------+-------------------]--------------
  //               w-                 w                   w+
  //
  // Split w+ = f * 2^e into two parts p1 and p2 (note: e < 0):
  //
  //      w+ = f * 2^e
  //         = ((f div 2^-e) * 2^-e + (f mod 2^-e)) * 2^e
  //         = ((p1        ) * 2^-e + (p2        )) * 2^e
  //         = p1 + p2 * 2^e

  // p1 = f div 2^-e (Since -e >= 32, p1 fits into a 32-bit int.)
  const uint64_t e_shift = -M_plus.e;
  const uint64_t e_mask = (1ULL << e_shift) - 1;

  uint32_t p1 = M_plus.f >> e_shift;
  uint64_t p2 = M_plus.f & e_mask;  // p2 = f mod 2^-e

  DCHECK_GT(p1, 0u);

//
// 1.
// Generate the digits of the integral part p1 = d[n-1]...d[1]d[0]
//

  unsigned kappa = InitKappa(p1);

  // We now have
  //  (B = buffer, L = length = k - n)
  //
  //      10^(k-1) <= p1 < 10^k
  //
  //      p1 = (p1 div 10^(k-1)) * 10^(k-1) + (p1 mod 10^(k-1))
  //         = (B[0]           ) * 10^(k-1) + (p1 mod 10^(k-1))
  //
  //      w+ = p1 + p2 * 2^e
  //         = B[0] * 10^(k-1) + (p1 mod 10^(k-1)) + p2 * 2^e
  //         = B[0] * 10^(k-1) + ((p1 mod 10^(k-1)) * 2^-e + p2) * 2^e
  //         = B[0] * 10^(k-1) + (                         rest) * 2^e
  //
  // and generate the digits d of p1 from left to right:
  //
  //      p1 = (B[0]B[1]...B[L  -1]B[L]B[L+1] ...B[k-2]B[k-1])_10
  //         = (B[0]B[1]...B[L  -1])_10 * 10^(k-L) + (B[L    ]...B[k-2]B[k-1])_10  (L = 1...k)
  //         = (B[0]B[1]...B[k-n-1])_10 * 10^(n  ) + (B[k-n  ]...B[k-2]B[k-1])_10  (n = k...1)

  bool cut_early = delta >= p2;
  uint32_t length = kappa;

  if (cut_early) {
    uint32_t p1_remainder = 0;
    uint32_t ten_n = 1;
    unsigned power = 0;
    uint32_t delta_shifted = (delta - p2) >> e_shift;  // valid due to cut_early
    bool check_increase_power = true;

    if (delta_shifted > 0) {
      power = CountDecimalDigit32(delta_shifted);
      ten_n = Power10(power);
      p1_remainder = p1 % ten_n;
      if (p1_remainder > delta_shifted) {
        --power;
        ten_n /= 10;
        p1_remainder %= ten_n;
        check_increase_power = false;
      }
      DCHECK_LE(p1_remainder, delta_shifted);
      p1 /= ten_n;
      length -= power;
    }

    if (check_increase_power) {
      while (p1 % 10 == 0) {
        ++power;
        ten_n *= 10;
        p1 /= 10;
        --length;
      }
    }

    SetDigits(p1, length, buffer);

    // Found V = buffer * 10^n, with w- <= V <= w+.
    // And V is correctly rounded.
    //
    decimal_exponent += power;
    if (dist > p2) {
      // We may now just stop. But instead look if the buffer could be
      // decremented to bring V closer to w.
      //
      // 10^n is now 1 ulp in the decimal representation V.
      //
      // The rounding procedure works with DiyFp's with an implicit
      // exponent e.
      //
      //      10^n = ten_n * 2^e = (10^n * 2^-e) * 2^e
      //
      // Note:
      // n has been decremented above, i.e. pow10 = 10^n
      //

      uint64_t rest = (uint64_t(p1_remainder) << e_shift) + p2;
      uint64_t ten_n64 = uint64_t(ten_n) << e_shift;

      while (rest < dist && delta - rest >= ten_n64 &&
             ten_n64 / 2 < (dist - rest)) {
        DCHECK_NE('0', buffer[length-1]);
        buffer[length-1]--;
        rest += ten_n64;
      }
    }

    return std::pair<unsigned, int>(length, decimal_exponent);
  }

  SetDigits(p1, length, buffer);
  assert(p2 != 0);
  // (otherwise the loop above would have been exited with rest <= delta)

  //
  // 2.
  // The digits of the integral part have been generated:
  //
  //      w+ = d[k-1]...d[1]d[0] + p2 * 2^e = buffer + p2 * 2^e
  //
  // Now generate the digits of the fractional part p2 * 2^e.
  //
  // Note:
  // No decimal point is generated: the exponent is adjusted instead.
  //
  // p2 actually represents the fraction
  //
  //      p2 * 2^e
  //          = p2 / 2^-e
  //          = d[-1] / 10^1 + d[-2] / 10^2 + d[-3] / 10^3 + ... + d[-m] / 10^m
  //
  // or
  //
  //      10 * p2 / 2^-e = d[-1] + (d[-2] / 10^1 + ... + d[-m] / 10^(m-1))
  //
  // and the digits can be obtained from left to right by
  //
  //      (10 * p2) div 2^-e = d[-1]
  //      (10 * p2) mod 2^-e = d[-2] / 10^1 + ... + d[-m] / 10^(m-1)
  //
  DCHECK_GT(p2, delta);
  int m = 0;
  for (;;) {
    // Invariant:
    //  1.  w+ = buffer * 10^m + 10^m * p2 * 2^e  (Note: m <= 0)

    // p2 * 10 < 2^60 * 10 < 2^60 * 2^4 = 2^64,
    // so the multiplication by 10 does not overflow.
    DCHECK_LE(p2, e_mask);
    p2 *= 10;

    uint64_t const d = p2 >> e_shift;      // = p2 div 2^-e
    uint64_t const r = p2 & e_mask;  // = p2 mod 2^-e

    // w+ = buffer * 10^m + 10^m * p2 * 2^e
    //    = buffer * 10^m + 10^(m-1) * (10 * p2     ) * 2^e
    //    = buffer * 10^m + 10^(m-1) * (d * 2^-e + r) * 2^e
    //    = buffer * 10^m + 10^(m-1) * d + 10^(m-1) * r * 2^e
    //    = (buffer * 10 + d) * 10^(m-1) + 10^(m-1) * r * 2^e

    assert(d <= 9);
    buffer[length++] = kDigits[d];  // buffer := buffer * 10 + d

    // w+ = buffer * 10^(m-1) + 10^(m-1) * r * 2^e

    p2 = r;
    m -= 1;

    // w+ = buffer * 10^m + 10^m * p2 * 2^e
    //
    // Invariant (1) restored.

    // p2 is now scaled by 10^(-m) since it repeatedly is multiplied by 10.
    // To keep the units in sync, delta and  dist need to be scaled too.
    delta *= 10;
    dist *= 10;

    uint64_t const rest = p2;

    // Check if enough digits have been generated.
    if (rest <= delta) {
      decimal_exponent += m;

      // ten_m represents 10^m as a Fp with an exponent e.
      //
      // Note: m < 0
      //
      // Note:
      // delta and dist are now scaled by 10^(-m) (they are repeatedly
      // multiplied by 10) and we need to do the same with ten_m.
      //
      //      10^(-m) * 10^m = 10^(-m) * ten_m * 2^e
      //                     = (10^(-m) * 10^m * 2^-e) * 2^e
      //                     = 2^-e * 2^e
      //
      // one.f = 2^-e and the exponent e is implicit.
      //
      uint64_t const ten_m = 1ULL << e_shift;
      Grisu2Round(buffer[length-1], dist, delta, rest, ten_m);
      return std::pair<unsigned, int>(length, decimal_exponent);
    }
  }

  // By construction this algorithm generates the shortest possible decimal
  // number (Loitsch, Theorem 6.2) which rounds back to w.
  // For an input number of precision p, at least
  //
  //      N = 1 + ceil(p * log_10(2))
  //
  // decimal digits are sufficient to identify all binary floating-point
  // numbers (Matula, "In-and-Out conversions").
  // This implies that the algorithm does not produce more than N decimal
  // digits.
  //
  //      N = 17 for p = 53 (IEEE double precision)
  //      N = 9  for p = 24 (IEEE single precision)
}

}  // namespace

// v = buf * 10^decimal_exponent
// len is the length of the buffer (number of decimal digits)
std::pair<unsigned, int> Grisu2(Fp m_minus, Fp v, Fp m_plus, char* buf) {
  assert(v.e == m_minus.e);
  assert(v.e == m_plus.e);
  //
  //  --------(-----------------------+-----------------------)--------    (A)
  //          m-                      v                       m+
  //
  //  --------------------(-----------+-----------------------)--------    (B)
  //                      m-          v                       m+
  //
  // First scale v (and m- and m+) such that the exponent is in the range
  // [alpha, beta].
  //

  CachedPower const cached = GetCachedPowerForBinaryExponent(m_plus.e);

  Fp const c_minus_k(cached.f, cached.e);  // = c ~= 10^k

  Fp const w = v.Mul(c_minus_k);  // Exponent of the products is v.e + c_minus_k.e + q
  Fp const w_minus = m_minus.Mul(c_minus_k);
  Fp const w_plus = m_plus.Mul(c_minus_k);

  //
  //  ----(---+---)---------------(---+---)---------------(---+---)----
  //          w-                      w                       w+
  //          = c*m-                  = c*v                   = c*m+
  //
  // Fp::Mul rounds its result and c_minus_k is approximated too. w (as well
  // as w- and w+) are now off by a small amount.
  // In fact:
  //
  //      w - v * 10^k < 1 ulp
  //
  // To account for this inaccuracy, add resp. subtract 1 ulp.
  //
  //  --------+---[---------------(---+---)---------------]---+--------
  //          w-  M-                  w                   M+  w+
  //
  // Now any number in [M-, M+] (bounds included) will round to w when input,
  // regardless of how the input rounding algorithm breaks ties.
  //
  // And DigitGen generates the shortest possible such number in [M-, M+].
  // This does not mean that Grisu2 always generates the shortest possible
  // number in the interval (m-, m+).
  //
  Fp const M_minus = Fp(w_minus.f + 1, w_minus.e);
  Fp const M_plus = Fp(w_plus.f - 1, w_plus.e);

  return Grisu2DigitGen(buf, -cached.k, M_minus, w, M_plus);
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------

// Returns a pointer to the element following the exponent
char* AppendExponent(char* buf, int e) {
  static constexpr char const* const kDigits = "0123456789";
  static constexpr char const* const kDigits100 =
      "00010203040506070809"
      "10111213141516171819"
      "20212223242526272829"
      "30313233343536373839"
      "40414243444546474849"
      "50515253545556575859"
      "60616263646566676869"
      "70717273747576777879"
      "80818283848586878889"
      "90919293949596979899";

  assert(e > -1000);
  assert(e < 1000);

  if (e < 0)
    *buf++ = '-', e = -e;
  else
    *buf++ = '+';

  uint32_t const k = static_cast<uint32_t>(e);
  if (k < 10) {
    //      *buf++ = kDigits[0];
    *buf++ = kDigits[k];
  } else if (k < 100) {
    *buf++ = kDigits100[2 * k + 0];
    *buf++ = kDigits100[2 * k + 1];
  } else {
    uint32_t const q = k / 100;
    uint32_t const r = k % 100;
    *buf++ = kDigits[q];
    *buf++ = kDigits100[2 * r + 0];
    *buf++ = kDigits100[2 * r + 1];
  }

  return buf;
}

char* FormatBuffer(char* buf, int k, int n) {
  // v = digits * 10^(n-k)
  // k is the length of the buffer (number of decimal digits)
  // n is the position of the decimal point relative to the start of the buffer.
  //
  // Format the decimal floating-number v in the same way as JavaScript's ToString
  // applied to number type.
  //
  // See:
  // https://tc39.github.io/ecma262/#sec-tostring-applied-to-the-number-type

  if (k <= n && n <= 21) {
    // digits[000]

    std::memset(buf + k, '0', static_cast<size_t>(n - k));
    // if (trailing_dot_zero)
    //{
    //    buf[n++] = '.';
    //    buf[n++] = '0';
    //}
    return buf + n;  // (len <= 21 + 2 = 23)
  }

  if (0 < n && n <= 21) {
    // dig.its
    assert(k > n);

    std::memmove(buf + (n + 1), buf + n, static_cast<size_t>(k - n));
    buf[n] = '.';
    return buf + (k + 1);  // (len == k + 1 <= 18)
  }

  if (-6 < n && n <= 0) {
    // 0.[000]digits

    std::memmove(buf + (2 + -n), buf, static_cast<size_t>(k));
    buf[0] = '0';
    buf[1] = '.';
    std::memset(buf + 2, '0', static_cast<size_t>(-n));
    return buf + (2 + (-n) + k);  // (len <= k + 7 <= 24)
  }

  if (k == 1) {
    // dE+123

    buf += 1;  // (len <= 1 + 5 = 6)
  } else {
    // d.igitsE+123

    std::memmove(buf + 2, buf + 1, static_cast<size_t>(k - 1));
    buf[1] = '.';
    buf += 1 + k;  // (len <= k + 6 = 23)
  }

  *buf++ = 'e';
  return AppendExponent(buf, n - 1);
}

}  // namespace dtoa
}  // namespace base

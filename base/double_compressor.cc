// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/double_compressor.h"

#include <cmath>
#include <numeric>

#include <blosc.h>

#include "base/endian.h"
#include "base/logging.h"
#include "base/float2decimal.h"


namespace base {

namespace {

inline uint64_t Power10(unsigned n) {
  return n == 0 ? 1 : dtoa::powers_of_10_internal[n];
}

static inline unsigned DecimalCost(unsigned dec_len) {
  if (dec_len >= 16)
    return 10;

  return (dec_len + 1) / 2;
}

constexpr uint8_t kRawBit = 1 << 7;
constexpr uint8_t kHasExceptionsBit = 1 << 6;

double FromPositive(int64_t significand, int exponent) {
  while (significand % 10 == 0) {
    significand /= 10;
    ++exponent;
  }
  // DCHECK_LE(Bits::FindMSBSet64NonZero(significand), 52);
  double res(significand);
  return res * exp10(exponent);
}

// TODO: to memoize exp10(exponent).
double FromDecimal(int64_t significand, int exponent) {
  if (significand == 0)
    return 0.0;

  if (significand < 0)
    return -FromPositive(-significand, exponent);
  else
    return FromPositive(significand, exponent);
}

void bitshuffle2(const int64_t* src, size_t count, uint8_t* dest) {
  const uint8_t* src_b = reinterpret_cast<const uint8_t*>(src);
  uint8_t btmp[64] = {0};
  while (count >= 8) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
      for (unsigned i = 0; i < 8; ++i) {
        btmp[i + shift] = (src[i] >> shift) & 0xFF;
      }
    }
    memcpy(dest, btmp, sizeof(btmp));
    src += 8;
    count -= 8;
    dest += sizeof(btmp);
  }
  if (count > 0) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
      for (unsigned i = 0; i < count; ++i) {
        btmp[i + shift] = (src[i] >> shift) & 0xFF;
      }
    }
    memcpy(dest, btmp, count * 8);
  }
}

#if 0
inline void bitunshuffle2(const uint8_t* src, size_t sz, uint8_t* dest) {
  uint8_t tmp[kShuffleStep];

  size_t i = 0;
  uint8_t* ptr = dest;
  for (; i + kShuffleStep <= sz; i += kShuffleStep) {
    bitunshuffle(sizeof(uint64_t), kShuffleStep, src + i, ptr, tmp);
    ptr+= kShuffleStep;
  }
  if (i != sz) {
    bitunshuffle(sizeof(uint64_t), sz - i, src + i, ptr, tmp);
  }
}
#endif

}  // namespace

struct DoubleCompressor::ExpInfo {
  uint16_t cnt[17];
  ExpInfo() { std::fill(cnt, cnt + 17, 0); }

  uint32_t count() const { return std::accumulate(cnt, cnt + 17, uint32_t(0)); }
  uint32_t Cost(int edelta) const {
    DCHECK_GE(edelta, 0);

    uint32_t res = 0;
    for (unsigned i = 0; i < 17; ++i) {
      res += cnt[i] * DecimalCost(edelta + i + 1);
    }
    return res;
  }
};

namespace  LittleEndian =  absl::little_endian;

void DoubleCompressor::DecimalHeader::Serialize(uint8_t flags, uint8_t* dest) {
  LittleEndian::Store64(dest, min_val); // 8
  dest += sizeof(uint64_t);
  LittleEndian::Store16(dest, exponent); //2
  dest += sizeof(uint16_t);

  LittleEndian::Store16(dest, lz4_size); //2
  dest += sizeof(uint16_t);

  if (flags & kHasExceptionsBit)
    LittleEndian::Store16(dest, first_exception_index);
}

uint32_t DoubleCompressor::DecimalHeader::Parse(uint8_t flags, const uint8_t* src) {
  min_val = LittleEndian::Load64(src); // 8
  src += sizeof(uint64_t);
  exponent = LittleEndian::Load16(src); //2
  src += sizeof(uint16_t);

  lz4_size = LittleEndian::Load16(src); //2
  src += sizeof(uint16_t);

  uint32_t res = 12;
  if (flags & kHasExceptionsBit) {
    first_exception_index = LittleEndian::Load16(src);
    res += 2;
  }
  return res;
}

unsigned DoubleCompressor::NormalizeDecimals(unsigned count, const double* dbl_src) {
  aux_->header.min_val = UINT64_MAX;
  aux_->header.first_exception_index = count;

  unsigned normal_cnt = 0;
  unsigned prev_exception_index = 0;
  unsigned exception_index = 0;

  for (unsigned i = 0; i < count; ++i) {
    const Decimal& dec = aux_->dec[i];

    if (dec.CanNormalize(aux_->header.exponent)) {
      int64_t normal_val = dec.val * Power10(dec.exp - aux_->header.exponent);
      aux_->normalized[i] = normal_val;
      if (normal_val < aux_->header.min_val)
        aux_->header.min_val = normal_val;
      normal_cnt++;
    } else {
      aux_->exceptions[exception_index++] = dbl_src[i];

      if (aux_->header.first_exception_index != count) {
        aux_->normalized[prev_exception_index] = i - prev_exception_index;
      } else {
        aux_->header.first_exception_index = i;
      }
      prev_exception_index = i;
    }
  }

  if (exception_index) {
    aux_->normalized[prev_exception_index] = 0; // close the linked list.
  }

  exception_index = aux_->header.first_exception_index;

  // Rebase the numbers.
  for (unsigned i = 0; i < count; ++i) {
    if (i == exception_index) {
      exception_index += aux_->normalized[i];
    } else {
      aux_->normalized[i] -= aux_->header.min_val;
    }
  }

  return normal_cnt;
}

uint32_t DoubleCompressor::Commit(const double* src, uint32_t count, uint8_t* dest) {
  if (count <= 16) {
    return WriteRawDoubles(src, count, dest);
  }

  CHECK_LE(count, BLOCK_MAX_LEN);
  if (!aux_)
    aux_.reset(new Aux);

  ExponentMap exp_map;

  for (unsigned i = 0; i < count; ++i) {
    Decimal& dec = aux_->dec[i];
    CHECK(dtoa::ToDecimal(src[i], &dec.val, &dec.exp, &dec.dec_len));

    DCHECK_GT(dec.dec_len, 0);
    DCHECK_LE(dec.dec_len, 17);

    // For dec_len we automatically put them into exceptions.
    if (dec.val != 0 && dec.dec_len < 17) {
      ++exp_map[dec.exp].cnt[dec.dec_len - 1];
    }
  }
  uint32_t cost = Optimize(exp_map);

  unsigned normal_cnt = NormalizeDecimals(count, src);
  if (normal_cnt < count / 2) {
    return WriteRawDoubles(src, count, dest);
  }
  VLOG(1) << "Cost: " << cost << " normalized count: " << normal_cnt;

  bitshuffle2(aux_->normalized, count, dest);

#if 0
  char* const cdest = reinterpret_cast<char*>(dest);
  char* next = cdest + 3 + DECIMAL_HEADER_MAX_SIZE - 2;
  char* end = cdest + CommitMaxSize(count);
  unsigned exc_count = count - normal_cnt;
  uint8_t flags = 0;
  if (exc_count > 0) {
    next += 2;
    flags = kHasExceptionsBit;
  }
  const unsigned kByteSize = sizeof(uint64_t) * count;
  int res = LZ4_compress_fast(reinterpret_cast<const char*>(shuffle_buf),  next,
                              kByteSize, LZ4_COMPRESSBOUND(kByteSize), 3 /* level */);
  CHECK_GT(res, 0);
  if ((res + 8 * exc_count) * 1.1 > kByteSize) {
    return WriteRawDoubles(src, count, dest);
  }

  aux_->header.lz4_size = res;
  aux_->header.Serialize(flags, dest + 3);

  next += res;
  if (exc_count) {
    shuffle(sizeof(uint64_t), exc_count * sizeof(uint64_t),
            reinterpret_cast<const uint8_t*>(aux_->exceptions), shuffle_buf);

    res = LZ4_compress_fast(reinterpret_cast<const char*>(shuffle_buf), next,
                            exc_count * 8, end - next, 5);
    CHECK_GT(res, 0);
    next += res;
  }
  uint32_t written = next - cdest;

  CHECK_LE(written, COMPRESS_BLOCK_BOUND);
  LittleEndian::Store16(dest + 1, written - 3);
  *dest = flags;
#endif
  return count * 8;
}

uint32_t DoubleCompressor::Optimize(const ExponentMap& em) {
  uint32_t best = UINT32_MAX;

  uint32_t prefix_cnt = 0;

  // we chose the exponent that produces smallest cost when
  // normalizing all the decimal numbers to its value.
  for (auto it = em.begin(); it != em.end(); ++it) {
    int16_t e_base = it->first;
    uint32_t current = it->second.Cost(0);
    auto next = it;

    for (++next; next != em.end(); ++next) {
      current += next->second.Cost(next->first - e_base);
    }
    current += prefix_cnt * 9;
    if (current < best) {
      best = current;
      aux_->header.exponent = e_base;
    }
    prefix_cnt += it->second.count();
  }
  return best;
}

uint32_t DoubleCompressor::WriteRawDoubles(const double* src, uint32_t count, uint8_t* dest) {
  *dest = kRawBit;
  uint16_t sz = count * sizeof(double);
  LittleEndian::Store16(dest + 1, sz);
  memcpy(dest + 3, src, sz);
  return sz + 3;
}

#if 0
int32_t  DoubleDecompressor::Decompress(const uint8_t* src, uint32_t src_len, double* dest) {
  if (src_len < 3 || LittleEndian::Load16(src + 1) != src_len - 3)
    return -1;

  src_len -= 3;

  uint8_t flags = *src;
  src += 3;

  if ((flags & kRawBit) != 0) {
    CHECK_EQ(0, src_len % sizeof(double));
    memcpy(dest, src, src_len);
    return src_len / sizeof(double);
  }
  CHECK_GT(src_len, DoubleCompressor::DECIMAL_HEADER_MAX_SIZE);

  if (!aux_)
    aux_.reset(new Aux);

  DoubleCompressor::DecimalHeader dh;
  uint32_t read = dh.Parse(flags, src);
  src_len -= read;

  CHECK_LE(dh.lz4_size, src_len);
  constexpr size_t kMaxSize = DoubleCompressor::BLOCK_MAX_BYTES;

  src += read;
  src_len -= dh.lz4_size;

  int res = LZ4_decompress_safe(reinterpret_cast<const char*>(src),
                                reinterpret_cast<char*>(aux_->z4buf), dh.lz4_size, kMaxSize);
  CHECK_GT(res, 0);
  CHECK_EQ(0, res % 8);
  src += dh.lz4_size;

  bitunshuffle2(aux_->z4buf, res, reinterpret_cast<uint8_t*>(dest));
  unsigned count = res / 8;

  unsigned exception_index = count;
  unsigned exception_id = 0;
  if (flags & kHasExceptionsBit) {
    res = LZ4_decompress_safe(reinterpret_cast<const char*>(src),
                              reinterpret_cast<char*>(aux_->z4buf), src_len,
                              kMaxSize);
    CHECK_GT(res, 0);
    CHECK_EQ(0, res % 8);
    unshuffle(sizeof(double), res, aux_->z4buf, reinterpret_cast<uint8_t*>(aux_->exceptions));
    exception_index = dh.first_exception_index;
  }

  int64_t* i64 = reinterpret_cast<int64_t*>(dest);
  for (unsigned i = 0; i < count; ++i) {
    if (i == exception_index) {
      unsigned delta = i64[i];
      dest[i] = aux_->exceptions[exception_id++];
      exception_index += delta;
      CHECK_LT(exception_index, count);
    } else {
      int64_t f = i64[i] + dh.min_val;
      dest[i] = FromDecimal(f, dh.exponent);
    }
  }
  return count;
}
#endif
}  // namespace util

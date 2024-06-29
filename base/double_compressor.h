// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cstdint>
#include <map>
#include <memory>

namespace base {

class DoubleCompressor {
 public:
  enum { BLOCK_MAX_BYTES = 1U << 16, BLOCK_MAX_LEN = BLOCK_MAX_BYTES / sizeof(double) };  // 2^13
  enum { COMPRESS_BLOCK_BOUND = (1U << 16) + 3,
         DECIMAL_HEADER_MAX_SIZE = 14};

  static constexpr uint32_t CommitMaxSize(uint32_t sz) {
    return (sz*8 + (sz*8 / 255) + 16) + 3 + DECIMAL_HEADER_MAX_SIZE;
  }

  enum {COMMIT_MAX_SIZE = BLOCK_MAX_LEN * 8 + BLOCK_MAX_LEN * 8 / 255 + 16 /* lz4 space */ + 3 +
                                   DECIMAL_HEADER_MAX_SIZE /*header*/};

  // Dest must be at least of CommitMaxSize(sz). The simpler approach is to always use
  // COMMIT_MAX_SIZE to accomodate BLOCK_MAX_LEN.
  // Commit will finally write no more than COMPRESS_BLOCK_BOUND bytes even though it will use
  // more space in between.
  uint32_t Commit(const double* src, uint32_t sz, uint8_t* dest);

 private:
  struct ExpInfo;
  typedef std::map<int16_t, ExpInfo> ExponentMap;

  unsigned NormalizeDecimals(unsigned count, const double* dbl_src);
  uint32_t Optimize(const ExponentMap& em);
  uint32_t WriteRawDoubles(const double* src, uint32_t sz, uint8_t* dest);

  struct __attribute__((aligned(4))) Decimal {
    int64_t val;
    int16_t exp;
    uint16_t dec_len;

    bool CanNormalize(int exp_reference) const {
      return val == 0 || (dec_len < 17 && exp >= exp_reference &&
                          exp - exp_reference <= 17 - dec_len);
    }
  };

  struct DecimalHeader {
    int64_t min_val;
    int16_t exponent;
    uint16_t lz4_size, first_exception_index;

    void Serialize(uint8_t flags, uint8_t* dest);
    uint32_t Parse(uint8_t flags, const uint8_t* src);
  };

  struct Aux {
    Decimal dec[BLOCK_MAX_LEN];
    double exceptions[BLOCK_MAX_LEN];
    int64_t normalized[BLOCK_MAX_LEN];

    DecimalHeader header;
  };

  std::unique_ptr<Aux> aux_;
  friend class DoubleDecompressor;
};

class DoubleDecompressor {
 public:
  enum {BLOCK_MAX_LEN = DoubleCompressor::BLOCK_MAX_LEN};

  DoubleDecompressor() {}

  // dest must accomodate at least BLOCK_MAX_LEN.
  // Returns -1 if it can not decompress src because src_len is not exact block size.
  // Fully consumes successfully decompressed block.
  // On success returns how many doubles were written to dest.
  int32_t Decompress(const uint8_t* src, uint32_t src_len, double* dest);

  // a valid header must point at least 3 bytes.
  // Returns block size including the header size.
  static uint32_t BlockSize(const uint8_t* header) {
    return 3 + ((uint32_t(header[2]) << 8) | header[1]);
  }

 private:
  struct Aux {
    uint8_t z4buf[DoubleCompressor::BLOCK_MAX_BYTES];
    double exceptions[BLOCK_MAX_LEN];
  };

  std::unique_ptr<Aux> aux_;
};

}  // namespace base

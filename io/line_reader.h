// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/integral_types.h"
#include "io/io.h"

namespace io {

// Assumes that source provides stream of text characters.
// Will break the stream into lines ending with EOL (either \r\n\ or \n).
class LineReader {
 public:
  enum { DEFAULT_BUF_LOG = 17 };

  LineReader(Source* source, Ownership ownership, uint32_t buf_log = DEFAULT_BUF_LOG)
      : source_(source), ownership_(ownership) {
    Init(buf_log);
  }

  ~LineReader();


  uint64_t line_num() const {
    return line_num_ & (kEofMask - 1);
  }

  // Sets the result to point to null-terminated line.
  // Empty lines are also returned.
  // Returns true if new line was found or false if end of stream was reached.
  bool Next(std::string_view* result, std::string* scratch = nullptr);

  ::std::error_code status() const {
    return status_;
  }

 private:
  void Init(uint32_t buf_log);

  Source* source_;
  uint64_t line_num_ = 0;  // MSB bit means EOF was reached.
  std::unique_ptr<char[]> buf_;
  char *next_, *end_;

  Ownership ownership_;
  uint32_t page_size_;
  std::string scratch_;
  std::error_code status_;

  static constexpr uint64_t kEofMask = 1ULL << 63;
};

}  // namespace io

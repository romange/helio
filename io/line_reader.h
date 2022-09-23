// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <unordered_map>

#include "base/integral_types.h"
#include "io/io.h"

namespace io {

// Assumes that source provides stream of text characters.
// Will break the stream into lines ending with EOL (either \r\n\ or \n).
class LineReader {
 public:
  enum { DEFAULT_BUF_LOG = 17 };

  class Iterator {
    std::string scratch_;
    std::string_view result_;
    LineReader* master_ = nullptr;

   public:
    Iterator(LineReader* lr = nullptr);

    std::string_view operator*() const {
      return result_;
    }

    Iterator& operator++();

    bool operator==(const Iterator& o) const {
      return o.master_ == master_;
    }

    bool operator!=(const Iterator& o) const {
      return o.master_ != master_;
    }
  };

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

  void set_line_len_limit(uint64_t lim) {
    line_len_limit_ = lim;
  }

  uint64_t line_len_limit() const {
    return line_len_limit_;
  }

  Iterator begin() {
    return Iterator{this};
  }

  Iterator end() {
    return Iterator{};
  }

 private:
  void Init(uint32_t buf_log);

  Source* source_;
  uint64_t line_num_ = 0;  // MSB bit means EOF was reached.
  uint64_t line_len_limit_ = -1;

  std::unique_ptr<char[]> buf_;
  char *next_, *end_;

  Ownership ownership_;
  uint32_t page_size_;
  std::string scratch_;
  std::error_code status_;

  static constexpr uint64_t kEofMask = 1ULL << 63;
};

namespace ini {

using Section = std::string;
using Contents = std::unordered_map<Section, std::unordered_map<std::string, std::string>>;

io::Result<Contents> Parse(Source* source, Ownership ownership);

};  // namespace ini

}  // namespace io

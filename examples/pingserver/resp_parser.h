// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <array>

#include <absl/container/inlined_vector.h>
#include <string_view>
#include <absl/types/span.h>

namespace redis {

class RespParser {
 public:
  enum Status { RESP_OK, MORE_INPUT, INVALID_ARRAYLEN, INVALID_BULKLEN, INVALID_STRING, INVALID_INT};
  using Buffer = absl::Span<uint8_t>;

  explicit RespParser() {}

  // It's a zero-copy parser. A user should not invalidate str if the parser returns COMMAND_READY
  // as long as he continues accessing RespExpr. However, if parser returns MORE_INPUT a user may
  // invalidate str because parser caches the intermediate state internally according to 'consumed'
  // result. A parser does not guarantee to consume the string or part of it if less than a minimal
  // threshold was passed and COMMAN_READY is not reached.
  Status Parse(Buffer str, uint32_t* consumed, std::vector<Buffer>* res);

 private:

  enum ParseResult : uint8_t {
    OK,
    MORE,
    INVALID,
  };

  void InitStart(uint8_t prefix_b, std::vector<Buffer>* res);
  void CacheState(std::vector<Buffer>* res);

  // Skips the first character (*).
  Status ConsumeArrayLen(Buffer str);
  Status ParseArg(Buffer str);
  Status ConsumeBulk(Buffer str);
  Status ParseInline(Buffer str);

  // Updates last_consumed_
  ParseResult ParseNum(Buffer str, int64_t* res);
  void HandleFinishArg();
  void ExtendLastString(Buffer str);

  enum State : uint8_t {
    START = 0,
    INLINE,
    ARRAY_LEN,
    PARSE_ARG,   // Parse [$:+-]string\r\n
    BULK_STR,
    FINISH_ARG,
    CMD_COMPLETE,
  };

  State state_ = START;
  Status last_status_ = RESP_OK;

  uint32_t last_consumed_ = 0;
  uint32_t bulk_len_ = 0;
  uint32_t last_cached_level_ = 0, last_cached_index_ = 0;

  absl::InlinedVector<std::pair<uint32_t, std::vector<Buffer>*>, 4> arr_stack_;
  std::vector<std::unique_ptr<std::vector<Buffer>>> ast_vec_;

  using BlobPtr = std::unique_ptr<uint8_t[]>;
  std::vector<BlobPtr> buf_stash_;
  std::vector<Buffer>* top_ = nullptr;
  bool is_broken_token_ = false;
};

}  // namespace redis

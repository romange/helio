// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "examples/pingserver/resp_parser.h"

#include <absl/strings/numbers.h>
#include "absl/strings/ascii.h"
#include "base/logging.h"

namespace redis {

namespace {
}  // namespace


using namespace std;

constexpr int kMaxArrayLen = 1024;
constexpr int64_t kMaxBulkLen = 64 * (1ul << 20);  // 64MB.

namespace {

inline RespParser::Buffer mkbuf(string* s) {
  return RespParser::Buffer{reinterpret_cast<uint8_t*>(s->data()), s->size()};
}

}  // namespace

auto RespParser::Parse(Buffer str, uint32_t* consumed, vector<Buffer>* res) -> Status {
  *consumed = 0;
  res->clear();

  if (str.size() < 2) {
    return MORE_INPUT;
  }

  if (state_ == CMD_COMPLETE) {
    state_ = START;
  }

  if (state_ == START) {
    InitStart(str[0], res);
  }

  if (!top_)
    top_ = res;

  while (state_ != CMD_COMPLETE) {
    last_consumed_ = 0;
    switch (state_) {
      case ARRAY_LEN:
        last_status_ = ConsumeArrayLen(str);
        break;
      case PARSE_ARG:
        if (str.size() < 4) {
          last_status_ = MORE_INPUT;
        } else {
          last_status_ = ParseArg(str);
        }
        break;
      case INLINE:
        CHECK(arr_stack_.empty());
        last_status_ = ParseInline(str);
        break;
      case BULK_STR:
        last_status_ = ConsumeBulk(str);
        break;
      case FINISH_ARG:
        HandleFinishArg();
        break;
      default:
        LOG(FATAL) << "Unexpected state " << int(state_);
    }

    *consumed += last_consumed_;

    if (last_status_ != RESP_OK) {
      break;
    }
    str.remove_prefix(last_consumed_);
  }

  if (last_status_ == MORE_INPUT) {
    CacheState(res);
  } else if (last_status_ == RESP_OK) {
    DCHECK(top_);
    if (res != top_) {
      DCHECK(!ast_vec_.empty());

      *res = *top_;
    }
  }

  return last_status_;
}

void RespParser::InitStart(uint8_t prefix_b, vector<Buffer>* res) {
  buf_stash_.clear();
  ast_vec_.clear();
  top_ = res;
  arr_stack_.clear();
  last_cached_level_ = 0;
  last_cached_index_ = 0;

  switch (prefix_b) {
    case '$':
    case ':':
      state_ = PARSE_ARG;
      arr_stack_.emplace_back(1, top_);
      break;
    case '*':
      state_ = ARRAY_LEN;
      break;
    default:
      state_ = INLINE;
      break;
  }
}

void RespParser::CacheState(vector<Buffer>* res) {
  if (top_->empty() && ast_vec_.empty()) {
    top_ = nullptr;
    return;
  }

  if (top_ == res) {
    ast_vec_.emplace_back(new vector<Buffer>(*res));
    top_ = ast_vec_.back().get();
  }

  DCHECK_LT(last_cached_level_, ast_vec_.size());
  while (true) {
    auto& cur = *ast_vec_[last_cached_level_];

    for (; last_cached_index_ < cur.size(); ++last_cached_index_) {
      auto& e = cur[last_cached_index_];
      if (!e.empty()) {
        BlobPtr ptr(new uint8_t[e.size()]);
        memcpy(ptr.get(), e.data(), e.size());
        e = Buffer{ptr.get(), e.size()};
        buf_stash_.push_back(std::move(ptr));
      }
    }
    if (last_cached_level_ + 1 == ast_vec_.size())
      break;
    ++last_cached_level_;
    last_cached_index_ = 0;
  }
}

auto RespParser::ParseInline(Buffer str) -> Status {
  DCHECK(!str.empty());

  uint8_t* ptr = str.begin();
  uint8_t* end = str.end();
  uint8_t* token_start = ptr;
  size_t token_len = 0;

  if (is_broken_token_) {
    while (ptr != end && *ptr > 32)
      ++ptr;

    size_t len = ptr - token_start;

    ExtendLastString(Buffer(token_start, len));
    if (ptr != end) {
      is_broken_token_ = false;
    }
  }

  auto is_finish = [&] { return ptr == end || *ptr == '\n'; };

  while (true) {
    while (!is_finish() && *ptr <= 32) {
      ++ptr;
    }
    if (is_finish())  // Too lazy to test for \r as well. Our parser will be more forgiving.
      break;

    DCHECK(!is_broken_token_);

    token_start = ptr;
    while (ptr != end && *ptr > 32)
      ++ptr;

    token_len += (ptr - token_start);
    top_->emplace_back(Buffer(token_start, ptr - token_start));
  }

  last_consumed_ = ptr - str.data();
  if (ptr == end) {  // we have not finished parsing.
    if (ptr[-1] > 32) {
      // we stopped in the middle of the token.
      is_broken_token_ = true;
    }

    return MORE_INPUT;
  } else {
    ++last_consumed_;  // consume the delimiter as well.
  }
  state_ = CMD_COMPLETE;

  return RESP_OK;
}

auto RespParser::ParseNum(Buffer str, int64_t* res) -> ParseResult {
  if (str.size() < 4) {
    return ParseResult::MORE;
  }

  char* s = reinterpret_cast<char*>(str.data() + 1);
  char* pos = reinterpret_cast<char*>(memchr(s, '\n', str.size() - 1));
  if (!pos) {
    return str.size() < 32 ? ParseResult::MORE : ParseResult::INVALID;
  }
  if (pos[-1] != '\r') {
    return ParseResult::INVALID;
  }

  bool success = absl::SimpleAtoi(std::string_view{s, size_t(pos - s - 1)}, res);
  if (!success) {
    return ParseResult::INVALID;
  }
  last_consumed_ = (pos - s) + 2;

  return ParseResult::OK;
}

auto RespParser::ConsumeArrayLen(Buffer str) -> Status {
  int64_t len;

  ParseResult res = ParseNum(str, &len);
  switch (res) {
    case ParseResult::MORE:
      return MORE_INPUT;
    case ParseResult::INVALID:
      return INVALID_ARRAYLEN;
    case ParseResult::OK:
      if (len < -1 || len > kMaxArrayLen)
        return INVALID_ARRAYLEN;
  }

  if (arr_stack_.size() > 0)
    return INVALID_STRING;

  if (arr_stack_.size() == 0 && !top_->empty())
    return INVALID_STRING;

  if (len <= 0  || !top_->empty()) {
    return INVALID_ARRAYLEN;
  }

  arr_stack_.emplace_back(len, top_);
  state_ = PARSE_ARG;

  return RESP_OK;
}

auto RespParser::ParseArg(Buffer str) -> Status {
  if (str[0] == '$') {
    int64_t len;

    ParseResult res = ParseNum(str, &len);
    switch (res) {
      case ParseResult::MORE:
        return MORE_INPUT;
      case ParseResult::INVALID:
        return INVALID_ARRAYLEN;
      case ParseResult::OK:
        if (len < -1 || len > kMaxBulkLen)
          return INVALID_ARRAYLEN;
    }

    if (len < 0) {
      return INVALID_BULKLEN;
    } else {
      top_->emplace_back(Buffer{});
      bulk_len_ = len;
      state_ = BULK_STR;
    }

    return RESP_OK;
  }

  return INVALID_BULKLEN;
}

auto RespParser::ConsumeBulk(Buffer str) -> Status {
  auto& bulk_str = top_->back();

  if (str.size() >= bulk_len_ + 2) {
    if (str[bulk_len_] != '\r' || str[bulk_len_ + 1] != '\n') {
      return INVALID_STRING;
    }
    if (bulk_len_) {
      if (is_broken_token_) {
        memcpy(bulk_str.end(), str.data(), bulk_len_);
        bulk_str = Buffer{bulk_str.data(), bulk_str.size() + bulk_len_};
      } else  {
        bulk_str = str.subspan(0, bulk_len_);
      }
    }
    is_broken_token_ = false;
    state_ = FINISH_ARG;
    last_consumed_ = bulk_len_ + 2;
    bulk_len_ = 0;

    return RESP_OK;
  }

  if (str.size() >= 32) {
    DCHECK(bulk_len_);
    size_t len = std::min<size_t>(str.size(), bulk_len_);

    if (is_broken_token_) {
      memcpy(bulk_str.end(), str.data(), len);
      bulk_str = Buffer{bulk_str.data(), bulk_str.size() + len};
      DVLOG(1) << "Extending bulk stash to size " << bulk_str.size();;
    } else {
      DVLOG(1) << "New bulk stash size " << bulk_len_;
      std::unique_ptr<uint8_t[]> nb(new uint8_t[bulk_len_]);
      memcpy(nb.get(), str.data(), len);
      bulk_str = Buffer{nb.get(), len};
      buf_stash_.emplace_back(move(nb));
      is_broken_token_ = true;
    }
    last_consumed_ = len;
    bulk_len_ -= len;
  }

  return MORE_INPUT;
}

void RespParser::HandleFinishArg() {
  DCHECK(!arr_stack_.empty());
  DCHECK_GT(arr_stack_.back().first, 0u);

  while (true) {
    --arr_stack_.back().first;
    state_ = PARSE_ARG;
    if (arr_stack_.back().first != 0)
      break;

    arr_stack_.pop_back();  // pop 0.
    if (arr_stack_.empty()) {
      state_ = CMD_COMPLETE;
      break;
    }
    top_ = arr_stack_.back().second;
  }
}

void RespParser::ExtendLastString(Buffer str) {
  DCHECK(!top_->empty());
  DCHECK(!buf_stash_.empty());

  Buffer& last_str = top_->back();

  DCHECK(last_str.data() == buf_stash_.back().get());

  std::unique_ptr<uint8_t[]> nb(new uint8_t[last_str.size() + str.size()]);
  memcpy(nb.get(), last_str.data(), last_str.size());
  memcpy(nb.get() + last_str.size(), str.data(), str.size());
  last_str = Buffer{nb.get(), last_str.size() + str.size()};
  buf_stash_.back() = std::move(nb);
}

}  // namespace redis

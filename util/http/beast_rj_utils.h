// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/beast/core/multi_buffer.hpp>

namespace util {
namespace http {

// This stream can not be insitu because rapidjson assumes
// that PutBegin/PutEnd denote continous memory range and not just an abstract stream.
class RjBufSequenceStream {
 public:
  using Ch = char;
  using multi_buffer = ::boost::beast::multi_buffer;

  RjBufSequenceStream(multi_buffer::const_buffers_type mdata) : end_(mdata.end()) {
    src_.it = mdata.begin();
    src_.Setup(end_);
  }

  // Read
  Ch Peek() const { return *src_.current; }

  Ch Take() {
    Ch res = *src_.current++;
    if (src_.finish()) {
      src_consumed_ += src_.Inc(end_);
    }
    return res;
  }

  size_t Tell() const {
    size_t res = src_consumed_;
    if (!src_.end) {
      res += src_.current - reinterpret_cast<const char*>((*src_.it).data());
    }
    return res;
  }

  Ch* PutBegin() { assert(0); return nullptr; }
  size_t PutEnd(Ch* begin) { assert(0); return 0; }
  void Put(Ch c) { assert(0); }

  /*Ch* PutBegin() {
    assert(*src_.current);
    dst_ = src_;
    dst_consumed_ = 0;
    return dst_.current;
  }

  size_t PutEnd(Ch* begin) {
    if (dst_consumed_ == 0)
      return dst_.current - begin;

    size_t res = dst_consumed_ + (dst_.current - reinterpret_cast<char*>((*dst_.it).data()));
    assert(res < 2000);
    return res;
  }

  void Put(Ch c) {
    *dst_.current++ = c;
    if (dst_.finish()) {
      dst_consumed_ += dst_.Inc(end_);
    }
  }*/

 private:
  using const_iterator = multi_buffer::const_buffers_type::const_iterator;

  struct Item {
    const_iterator it;
    const char *current, *end;

    Item() : current(nullptr), end(nullptr) {}

    size_t Inc(const_iterator end_it) {
      size_t res = (*it).size();
      ++it;
      Setup(end_it);
      return res;
    }

    void Setup(const_iterator end_it) {
      if (it == end_it) {
        end = nullptr;                            // fill with 0 bytes
        current = reinterpret_cast<char*>(&end);  // use end_ as placeholder for '\0'
        return;
      }
      const auto& mb = *it;
      current = reinterpret_cast<const char*>(mb.data());
      end = current + mb.size();
    }

    bool finish() const { return end == current; }
  };

  const_iterator end_;
  Item src_;                 //, dst_;
  size_t src_consumed_ = 0;  //, dst_consumed_ = 0;
};

}  // namespace http
}  // namespace util

namespace rapidjson {
template <typename Stream> struct StreamTraits;

template <> struct StreamTraits<util::http::RjBufSequenceStream> {
  enum { copyOptimization = 1 };
};

}  // namespace rapidjson

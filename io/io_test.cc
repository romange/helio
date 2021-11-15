// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/io.h"

#include <deque>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "io/line_reader.h"

using namespace std;
using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;

namespace io {

class FakeSink : public Sink {
 public:
  Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  deque<uint32_t> call_sz;
  string value;
};


class StringSource final : public Source {
 public:
  StringSource(string buf, size_t read_sz = UINT64_MAX) : buf_(buf), read_sz_(read_sz) {}

  Result<size_t> ReadSome(const MutableBytes& dest);

 protected:
  std::string buf_;
  size_t read_sz_;
  off_t offs_ = 0;
};


Result<size_t> FakeSink::WriteSome(const iovec* v, uint32_t len) {
  if (call_sz.empty() || len == 0)
    return 0;

  uint32_t& limit = call_sz.front();

  size_t io_res = 0;
  while (len) {
    size_t ws = std::min<size_t>(limit, v->iov_len);
    value.append(reinterpret_cast<char*>(v->iov_base), ws);
    limit -= ws;
    io_res += ws;
    if (limit == 0) {
      call_sz.pop_front();
      break;
    }
    ++v;
    --len;
  }
  return io_res;
}

Result<size_t> StringSource::ReadSome(const MutableBytes& dest) {
  size_t read_sz = min(read_sz_, dest.size());
  read_sz = min(read_sz, buf_.size() - offs_);
  memcpy(dest.data(), buf_.data() + offs_, read_sz);
  offs_ += read_sz;

  return read_sz;
}


class IoTest : public testing::Test {
 protected:
};

TEST_F(IoTest, Write) {
  string a("0123456789ABCDEF"), b("abcdefghijklmnop"), c("9876543210000000");

  iovec v[3] = {{.iov_base = a.data(), .iov_len = 10},
                {.iov_base = b.data(), .iov_len = 10},
                {.iov_base = c.data(), .iov_len = 10}};

  FakeSink sink;
  sink.call_sz.push_back(4);
  sink.call_sz.push_back(10);
  sink.call_sz.push_back(6);
  sink.call_sz.push_back(10);

  error_code ec = sink.Write(v, 3);
  EXPECT_FALSE(ec);
  EXPECT_EQ("0123456789abcdefghij9876543210", sink.value);
}

TEST_F(IoTest, LineReader) {
  StringSource ss("one\ntwo\r\nthree");
  LineReader lr(&ss, DO_NOT_TAKE_OWNERSHIP);
  std::string_view result;
  EXPECT_TRUE(lr.Next(&result));
  EXPECT_EQ("one", result);
  EXPECT_TRUE(lr.Next(&result));
  EXPECT_EQ("two", result);

  EXPECT_TRUE(lr.Next(&result));
  EXPECT_EQ("three", result);
}

}  // namespace io

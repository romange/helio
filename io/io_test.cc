// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/io.h"

#include <deque>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "io/line_reader.h"
#include "io/proc_reader.h"

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


class StringSource : public Source {
 public:
  StringSource(string buf) : buf_(buf) {}

  Result<size_t> ReadSome(const iovec* v, uint32_t len) final;

 protected:
  std::string buf_;
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

Result<size_t> StringSource::ReadSome(const iovec* v, uint32_t len) {
  ssize_t read_total = 0;
  while (size_t(offs_) < buf_.size() && len > 0) {
    size_t read_sz = min(buf_.size() - offs_, v->iov_len);
    memcpy(v->iov_base, buf_.data() + offs_, read_sz);
    read_total += read_sz;
    offs_ += read_sz;

    ++v;
    --len;
  }

  return read_total;
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

TEST_F(IoTest, ProcReader) {
  auto sdata = ReadStatusInfo();
  ASSERT_TRUE(sdata.has_value());
  LOG(INFO) << sdata->vm_peak << " " << sdata->vm_size << " " << sdata->vm_rss;

  EXPECT_GT(sdata->vm_size, sdata->vm_rss);

  auto mdata = ReadMemInfo();

  ASSERT_TRUE(mdata.has_value());
  EXPECT_LT(mdata->mem_free, mdata->mem_avail);
  EXPECT_GT(mdata->mem_free, 1024);
  EXPECT_GT(mdata->mem_buffers, 0);
  EXPECT_GT(mdata->mem_cached, 0);
  EXPECT_GT(mdata->mem_SReclaimable, 0);
  EXPECT_GT(mdata->mem_total, 1ul << 30);
}

}  // namespace io

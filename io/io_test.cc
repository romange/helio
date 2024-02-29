// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/io.h"

#include <gmock/gmock.h>

#include <deque>

#include "base/gtest.h"
#include "base/logging.h"
#include "io/line_reader.h"
#include "io/proc_reader.h"

using namespace std;
using ::testing::_;
using testing::Pair;
using testing::UnorderedElementsAre;

namespace io {

class FakeSink : public Sink {
 public:
  Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  deque<uint32_t> call_sz;
  string value;
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
  BytesSource ss("one\ntwo\r\nthree");
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
#ifdef __APPLE__
    GTEST_SKIP() << "Skipped IoTest.ProcReader test on MacOS";
    return;
#endif
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

  auto self_stat = ReadSelfStat();
  EXPECT_TRUE(self_stat.has_value());
  EXPECT_GT(self_stat->start_time_sec, 0);
  EXPECT_EQ(0, self_stat->maj_flt);
}

TEST_F(IoTest, IniReader) {
  BytesSource ss(R"(
    foo = bar
    [sec1 ]
    x = y
    z=1
    )");
  io::Result<ini::Contents> contents = ini::Parse(&ss, DO_NOT_TAKE_OWNERSHIP);
  ASSERT_EQ(2, contents->size());
  ASSERT_EQ(1, contents->count(""));
  ASSERT_EQ(1, contents->count("sec1"));
  ASSERT_THAT(contents->at(""), UnorderedElementsAre(Pair("foo", "bar")));
  ASSERT_THAT(contents->at("sec1"), UnorderedElementsAre(Pair("x", "y"), Pair("z", "1")));
}

TEST_F(IoTest, IoBuf) {
  string_view test = "TEST---STRING---VIEW"sv;
  base::IoBuf buf{};

  // Write to buf through sink.
  BufSink sink{&buf};
  sink.Write(Buffer(test));

  // Check it contains it.
  ASSERT_EQ(View(buf.InputBuffer()), test);

  // Read it back through source.
  uint8_t dest[100];
  BufSource source{&buf};
  auto res = source.Read(dest);
  ASSERT_TRUE(res);
  ASSERT_EQ(res.value(), test.size());

  auto fetched = string_view{reinterpret_cast<const char*>(dest), test.size()};
  ASSERT_EQ(fetched, test);
}

}  // namespace io

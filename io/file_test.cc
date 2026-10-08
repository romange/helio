// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/file.h"

#include <absl/strings/numbers.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "io/file_util.h"
#include "io/line_reader.h"

namespace io {

using namespace std;
using testing::EndsWith;
using testing::SizeIs;
class FileTest : public ::testing::Test {
 protected:
};

TEST_F(FileTest, Util) {
  string path1 = base::GetTestTempPath("foo1.txt");
  WriteStringToFileOrDie("foo", path1);
  string path2 = base::GetTestTempPath("foo2.txt");
  WriteStringToFileOrDie("foo", path2);

  string glob = base::GetTestTempPath("foo?.txt");
  Result<StatShortVec> res = StatFiles(glob);
  ASSERT_TRUE(res);
  ASSERT_THAT(res.value(), SizeIs(2));
  EXPECT_THAT(res.value()[0].name, EndsWith("/foo1.txt"));
  EXPECT_THAT(res.value()[1].name, EndsWith("/foo2.txt"));

  auto res2 = ReadFileToString(path2);
  ASSERT_TRUE(res2);
  EXPECT_EQ(res2.value(), "foo");
}

TEST_F(FileTest, ReadPastEndReturnsEof) {
  string path = base::GetTestTempPath("read-past-end.txt");
  WriteStringToFileOrDie("foo", path);

  auto res = OpenRead(path, ReadonlyFile::Options{});
  ASSERT_TRUE(res);
  unique_ptr<ReadonlyFile> file(*res);

  char buf;
  auto read_res = file->Read(4, ReadonlyFile::MutableBytes{reinterpret_cast<uint8_t*>(&buf), 1});
  ASSERT_TRUE(read_res);
  EXPECT_EQ(*read_res, 0);
}

#ifdef __linux__
TEST_F(FileTest, ReadProcFileAtNonzeroOffset) {
  auto res = OpenRead("/proc/self/status", ReadonlyFile::Options{});
  ASSERT_TRUE(res);
  unique_ptr<ReadonlyFile> file(*res);
  ASSERT_EQ(file->Size(), 0);

  char buf;
  auto read_res = file->Read(1, ReadonlyFile::MutableBytes{reinterpret_cast<uint8_t*>(&buf), 1});
  ASSERT_TRUE(read_res);
  EXPECT_EQ(*read_res, 1);
}
#endif

TEST_F(FileTest, LineReader) {
  string path = base::ProgramRunfile("testdata/ids.txt.zst");
  Result<Source*> src = OpenUncompressed(path);
  ASSERT_TRUE(src);

  LineReader lr(*src, TAKE_OWNERSHIP);
  string_view line;
  uint64_t val;
  while (lr.Next(&line)) {
    ASSERT_TRUE(absl::SimpleHexAtoi(line, &val)) << lr.line_num();
  }
  EXPECT_EQ(48, lr.line_num());
}

TEST_F(FileTest, Direct) {
  string path = base::GetTestTempPath("write.bin");
  WriteFile::Options opts;
  opts.direct = true;
  auto res = OpenWrite(path, opts);
  ASSERT_TRUE(res);
  unique_ptr<WriteFile> file(*res);
  char* src = nullptr;
  constexpr unsigned kLen = 4096;
  CHECK_EQ(0, posix_memalign((void**)&src, 4096, kLen));
  memset(src, 'a', 4096);
  auto ec = file->Write(string_view(src, kLen));
  free(src);
  ASSERT_FALSE(ec) << ec;

  ec = file->Close();
  ASSERT_FALSE(ec);
}

}  // namespace io

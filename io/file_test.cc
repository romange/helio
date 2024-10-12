// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/file.h"

#include <gmock/gmock.h>
#include <absl/strings/numbers.h>

#include "base/gtest.h"
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

TEST_F(FileTest, LineReader) {
  string path = base::ProgramRunfile("testdata/ids.txt");
  ReadonlyFileOrError fl_err = OpenRead(path, ReadonlyFile::Options{});
  ASSERT_TRUE(fl_err);
  FileSource fs(std::move(*fl_err));
  LineReader lr(&fs, DO_NOT_TAKE_OWNERSHIP);
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
  WriteFile* file =  *res;

  auto ec = file->Write(string(4096, 'a'));
  ASSERT_FALSE(ec) << ec;

  ec = file->Close();
  ASSERT_FALSE(ec);
}


}  // namespace io

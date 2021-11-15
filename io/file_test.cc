// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/file.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "io/file_util.h"

namespace io {

using namespace std;
using testing::SizeIs;
using testing::EndsWith;
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
}

}  // namespace io

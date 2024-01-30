// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/uring_file.h"

#include <thread>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/uring_proactor.h"

using namespace std;

namespace util {
namespace fb2 {

class UringFileTest : public testing::Test {
 protected:
  UringFileTest() {
    proactor_.reset(new UringProactor);
  }

  void SetUp() final {
    proactor_thread_ = thread{[this] {
      proactor_->Init(0, 16);
      proactor_->Run();
    }};
  }

  void TearDown() final {
    proactor_->Stop();
    proactor_thread_.join();
  }

  std::unique_ptr<UringProactor> proactor_;
  std::thread proactor_thread_;
};

TEST_F(UringFileTest, Basic) {
  proactor_->Await([this] {
    auto res = OpenLinux("/tmp/1.log", O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_TRUE(res);
    LinuxFile* wf = (*res).get();

    auto ec = wf->Write(io::Buffer("hello"), -1, RWF_APPEND);
    ASSERT_FALSE(ec);

    ec = wf->Write(io::Buffer(" world"), -1, RWF_APPEND);
    ASSERT_FALSE(ec);

    ec = wf->Close();
    EXPECT_FALSE(ec);
  });

  proactor_->Await([this] {
    auto res = OpenLinux("/tmp/1.log", O_RDONLY, 0666);
    ASSERT_TRUE(res);
    LinuxFile* rf = (*res).get();
    char buf[100];
    Done done;
    rf->ReadAsync(io::MutableBuffer(buf), 0, [&](int res) {
      ASSERT_EQ(res, 11);
      EXPECT_EQ("hello world", string(buf, res));
      done.Notify();
    });

    ASSERT_TRUE(done.WaitFor(10ms));
    error_code ec = rf->Close();
    EXPECT_FALSE(ec);
  });
}

}  // namespace fb2
}  // namespace util
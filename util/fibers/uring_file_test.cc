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
  string path = base::GetTestTempPath("1.log");
  proactor_->Await([this, path] {
    auto res = OpenLinux(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_TRUE(res);
    LinuxFile* wf = (*res).get();

    auto ec = wf->Write(io::Buffer("hello"), -1, RWF_APPEND);
    ASSERT_FALSE(ec);

    char buf[] = " world";
    Done done;
    wf->WriteAsync(io::Buffer(buf), 5, [done](int res) mutable {
      done.Notify();
      ASSERT_EQ(res, 6);
    });
    done.WaitFor(100ms);
    ec = wf->Close();
    EXPECT_FALSE(ec);
  });

  proactor_->Await([&] {
    auto res = OpenLinux(path, O_RDWR, 0666);
    ASSERT_TRUE(res);
    unique_ptr<LinuxFile> lf = std::move(*res);
    char buf[100];
    Done done;
    lf->ReadAsync(io::MutableBuffer(buf), 0, [&](int res) {
      ASSERT_EQ(res, 11);
      EXPECT_EQ("hello world", string(buf, res));
      done.Notify();
    });

    ASSERT_TRUE(done.WaitFor(10ms));
    error_code ec = lf->Close();
    EXPECT_FALSE(ec);
  });
}

TEST_F(UringFileTest, WriteAsync) {
  string path = base::GetTestTempPath("file.log");
  char src[] = "hello world";

  proactor_->Await([&] {
    auto res = OpenLinux(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_TRUE(res);
    unique_ptr<LinuxFile> lf = std::move(*res);

    BlockingCounter bc{0};

    auto cb = [bc](int res) mutable {
      ASSERT_GT(res, 0);
      bc->Dec();
    };

    for (unsigned i = 0; i < 100; ++i) {
      bc->Add(1);
      lf->WriteAsync(io::Buffer(src), 5, cb);
    }
    ASSERT_TRUE(bc->WaitFor(100ms));
    auto ec = lf->Close();
    EXPECT_FALSE(ec);
  });
}

}  // namespace fb2
}  // namespace util

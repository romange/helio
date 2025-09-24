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
  proactor_->Await([path] {
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

TEST_F(UringFileTest, FAllocateAndStatX) {
  string path = base::GetTestTempPath("1.log");
  proactor_->Await([path] {
    auto res = OpenLinux(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_TRUE(res);
    LinuxFile* wf = (*res).get();

    Done done;
    auto io_cb = [&](int res) {
      done.Notify();
    };

    wf->FallocateAsync(FALLOC_FL_KEEP_SIZE, 0, 4096, io_cb);
    ASSERT_TRUE(done.WaitFor(100ms));
    done.Reset();

    struct statx stat;
    auto ec = StatX(path.c_str(), &stat, wf->fd());
    ASSERT_FALSE(ec);
    // File size remained zero even if we allocated block size
    ASSERT_EQ(stat.stx_size, 0);

    wf->FallocateAsync(0, 0, 8192, io_cb);
    ASSERT_TRUE(done.WaitFor(100ms));

    ec = StatX(path.c_str(), &stat, wf->fd());
    ASSERT_FALSE(ec);
    ASSERT_EQ(stat.stx_size, 8192);

    ASSERT_FALSE(wf->Close());
  });
}

TEST_F(UringFileTest, FSync) {
  string path = base::GetTestTempPath("1.log");
  proactor_->Await([path] {
    auto res = OpenLinux(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_TRUE(res);
    LinuxFile* wf = (*res).get();

    Done done;
    auto io_cb = [&](int res) {
      done.Notify();
    };

    std::string buf(4096, 'c');

    wf->WriteAsync(io::Bytes{reinterpret_cast<uint8_t*>(buf.data()), buf.size()}, 0, io_cb);
    ASSERT_TRUE(done.WaitFor(100ms));
    done.Reset();

    struct statx stat;
    auto ec = StatX(path.c_str(), &stat, wf->fd());
    ASSERT_FALSE(ec);
    ASSERT_EQ(stat.stx_size, 4096);

    ec = wf->FSync();
    ASSERT_FALSE(ec);

    ASSERT_FALSE(wf->Close());
  });
}

}  // namespace fb2
}  // namespace util

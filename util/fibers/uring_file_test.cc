// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/uring_file.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/flags/reflection.h>
#include <fcntl.h>
#include <liburing.h>

#include <climits>
#include <thread>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/uring_proactor.h"

ABSL_DECLARE_FLAG(uint32_t, uring_direct_table_len);

using namespace std;

namespace util {
namespace fb2 {

namespace {

// Issues an async operation via issue_fn and waits for its completion. Returns the io result.
// Waits without a deadline: the callback references stack state, so returning before it runs
// is unsafe, and durability calls may block on storage writeback. ctest enforces the timeout.
template <typename F> int RunAsync(F&& issue_fn) {
  Done done;
  int res = INT_MIN;
  issue_fn([&](int io_res) {
    res = io_res;
    done.Notify();
  });
  done.Wait();
  return res;
}

io::Bytes AsBytes(string_view s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

string ReadAllContents(LinuxFile* lf) {
  char buf[256];
  int res = RunAsync([&](auto cb) { lf->ReadAsync(io::MutableBuffer(buf), 0, cb); });
  CHECK_GE(res, 0);
  return string(buf, res);
}

// Exercises the calls used by an append-only writer that drives its logic from completions.
void TestAsyncDurability(const string& path) {
  auto res = OpenLinux(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
  ASSERT_TRUE(res);
  unique_ptr<LinuxFile> lf = std::move(*res);

  // rw_flags are passed to the kernel: RWF_APPEND ignores the offset and appends.
  ASSERT_EQ(5, RunAsync([&](auto cb) { lf->WriteAsync(AsBytes("hello"), 0, 0, cb); }));
  ASSERT_EQ(6, RunAsync([&](auto cb) { lf->WriteAsync(AsBytes(" world"), 0, RWF_APPEND, cb); }));
  EXPECT_EQ("hello world", ReadAllContents(lf.get()));

  // RWF_DONTCACHE depends on the kernel and the filesystem.
  string_view tail = "!";
  int io_res = RunAsync([&](auto cb) { lf->WriteAsync(AsBytes(tail), 11, RWF_DONTCACHE, cb); });
  if (io_res == -EOPNOTSUPP) {
    LOG(INFO) << "RWF_DONTCACHE is not supported, falling back";
    ASSERT_EQ(1, RunAsync([&](auto cb) { lf->WriteAsync(AsBytes(tail), 11, cb); }));
  } else {
    ASSERT_EQ(1, io_res);
  }

  // The fallback for RWF_DONTCACHE: write back the range and drop it from the page cache.
  constexpr unsigned kSyncFlags =
      SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
  EXPECT_EQ(0, RunAsync([&](auto cb) { lf->SyncFileRangeAsync(0, 12, kSyncFlags, cb); }));
  EXPECT_EQ(0, RunAsync([&](auto cb) { lf->SyncFileRangeAsync(0, 0, kSyncFlags, cb); }));
  EXPECT_EQ(0, RunAsync([&](auto cb) { lf->FadviseAsync(0, 12, POSIX_FADV_DONTNEED, cb); }));
  EXPECT_EQ(0, RunAsync([&](auto cb) { lf->FadviseAsync(0, 0, POSIX_FADV_DONTNEED, cb); }));

  // fsync and fdatasync.
  EXPECT_EQ(0, RunAsync([&](auto cb) { lf->FSyncAsync(0, cb); }));
  EXPECT_EQ(0, RunAsync([&](auto cb) { lf->FSyncAsync(IORING_FSYNC_DATASYNC, cb); }));
  EXPECT_FALSE(lf->FSync(0));
  EXPECT_FALSE(lf->FSync(IORING_FSYNC_DATASYNC));

  // Errors are propagated to the callback.
  EXPECT_EQ(-EINVAL, RunAsync([&](auto cb) { lf->FSyncAsync(1U << 7, cb); }));
  EXPECT_EQ(-EINVAL, RunAsync([&](auto cb) { lf->FadviseAsync(0, 0, 1000, cb); }));
  EXPECT_EQ(-EINVAL, RunAsync([&](auto cb) { lf->SyncFileRangeAsync(0, 0, 1U << 7, cb); }));

  EXPECT_EQ("hello world!", ReadAllContents(lf.get()));
  EXPECT_FALSE(lf->Close());
}

}  // namespace

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

// Runs the proactor with a direct fd table, so LinuxFile uses registered (fixed) fds.
class UringFileDirectTest : public UringFileTest {
 protected:
  UringFileDirectTest() {
    absl::SetFlag(&FLAGS_uring_direct_table_len, 16);
  }

  absl::FlagSaver flag_saver_;
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
    auto io_cb = [&](int res) { done.Notify(); };

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
    auto io_cb = [&](int res) { done.Notify(); };

    std::string buf(4096, 'c');

    wf->WriteAsync(io::Bytes{reinterpret_cast<uint8_t*>(buf.data()), buf.size()}, 0, io_cb);
    ASSERT_TRUE(done.WaitFor(100ms));
    done.Reset();

    struct statx stat;
    auto ec = StatX(path.c_str(), &stat, wf->fd());
    ASSERT_FALSE(ec);
    ASSERT_EQ(stat.stx_size, 4096);

    ec = wf->FSync(0);
    ASSERT_FALSE(ec);

    ASSERT_FALSE(wf->Close());
  });
}

TEST_F(UringFileTest, AsyncDurability) {
  string path = base::GetTestTempPath("durability.log");
  proactor_->Await([&] { TestAsyncDurability(path); });
}

TEST_F(UringFileDirectTest, AsyncDurability) {
  string path = base::GetTestTempPath("durability_direct.log");
  proactor_->Await([&] {
    ASSERT_TRUE(proactor_->HasDirectFD());
    TestAsyncDurability(path);
  });
}

TEST_F(UringFileTest, WriteFixedAsyncFlags) {
  string path = base::GetTestTempPath("fixed.log");
  proactor_->Await([&] {
    ASSERT_EQ(0u, proactor_->RegisterBuffers(4096));
    auto buf = proactor_->RequestBuffer(4096);
    ASSERT_TRUE(buf && buf->buf_idx);

    auto res = OpenLinux(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
    ASSERT_TRUE(res);
    unique_ptr<LinuxFile> lf = std::move(*res);

    memcpy(buf->bytes.data(), "hello world", 11);
    io::Bytes hello{buf->bytes.data(), 5}, world{buf->bytes.data() + 5, 6};
    unsigned idx = *buf->buf_idx;

    ASSERT_EQ(5, RunAsync([&](auto cb) { lf->WriteFixedAsync(hello, 0, idx, cb); }));
    ASSERT_EQ(6, RunAsync([&](auto cb) { lf->WriteFixedAsync(world, 0, idx, RWF_APPEND, cb); }));
    EXPECT_EQ("hello world", ReadAllContents(lf.get()));

    proactor_->ReturnBuffer(*buf);
    EXPECT_FALSE(lf->Close());
  });
}

}  // namespace fb2
}  // namespace util

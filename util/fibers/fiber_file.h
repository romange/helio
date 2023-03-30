// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "io/file.h"
#include "util/fibers/fiberqueue_threadpool.h"

namespace util {

#ifdef USE_FB2
namespace fb_namesp = fb2;
#else
namespace fb_namesp = fibers_ext;
#endif

// Fiber-friendly file handler. Returns ReadonlyFile* instance that does not block the current
// thread unlike the regular posix implementation. All the read opearations will run
// in FiberQueueThreadPool.
struct FiberReadOptions : public io::ReadonlyFile::Options {
  struct Stats {
    size_t cache_bytes = 0;  // read because of prefetch logic.
    size_t disk_bytes = 0;   // read via  ThreadPool calls.
    size_t read_prefetch_cnt = 0;
    size_t preempt_cnt = 0;
  };

  size_t prefetch_size = 1 << 16;  // 65K
  Stats* stats = nullptr;
};

ABSL_MUST_USE_RESULT io::ReadonlyFileOrError OpenFiberReadFile(
    std::string_view name, fb_namesp::FiberQueueThreadPool* tp,
    const FiberReadOptions& opts = FiberReadOptions{});

struct FiberWriteOptions : public io::WriteFile::Options {
  bool consistent_thread = true;  // whether to send the write request to the thread in the pool.
};

ABSL_MUST_USE_RESULT io::Result<io::WriteFile*> OpenFiberWriteFile(
    std::string_view name, fb_namesp::FiberQueueThreadPool* tp,
    const FiberWriteOptions& opts = FiberWriteOptions());

}  // namespace util

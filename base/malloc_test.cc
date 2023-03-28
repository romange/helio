// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <jemalloc/jemalloc.h>
#include <malloc.h>
#include <mimalloc.h>

// we expose internal types of mimalloc to access its statistics.
// Currently it's not supported officially by the library.
#include <mimalloc-types.h>
#include <sys/mman.h>
#include <sys/resource.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/proc_util.h"

namespace base {

using namespace std;

typedef struct dictEntry {
  void* key;
  union {
    void* val;
    uint64_t u64;
    int64_t s64;
    double d;
  } v;
  struct dictEntry* next;
} dictEntry;

static_assert(sizeof(dictEntry) == 24, "");

class MallocTest : public testing::Test {
 public:
  static void SetUpTestCase() {
    mi_process_init();
  }
};

TEST_F(MallocTest, GoodSize) {
  EXPECT_EQ(32, mi_good_size(24));

  // when jemalloc is configured --with-lg-quantum=3 it produces tight allocations.
  EXPECT_EQ(32, je_nallocx(24, 0));

  EXPECT_EQ(8, mi_good_size(5));
  EXPECT_EQ(8, je_nallocx(5, 0));

  EXPECT_EQ(512, mi_good_size(512));
  EXPECT_EQ(640, mi_good_size(513));
  EXPECT_EQ(16384, mi_good_size(16136));
  EXPECT_EQ(16384, mi_good_size(15240));
  EXPECT_EQ(14336, mi_good_size(13064));
  EXPECT_EQ(20480, mi_good_size(17288));
  EXPECT_EQ(32768, mi_good_size(28816));
}

TEST_F(MallocTest, Oom) {
  errno = 0;
  mi_option_enable(mi_option_limit_os_alloc);

  ASSERT_EQ(0, errno);

  void* ptr = mi_malloc(1 << 10);
  int err = errno;
  ASSERT_TRUE(ptr == NULL);
  ASSERT_EQ(ENOMEM, err);

  mi_option_disable(mi_option_limit_os_alloc);
  ptr = mi_malloc(1 << 10);
  ASSERT_TRUE(ptr != NULL);
  void* ptr2 = mi_malloc(1 << 10);
  ASSERT_TRUE(ptr2 != NULL);
  mi_free(ptr);
  mi_free(ptr2);
}

inline size_t VmmRss() {
  return ProcessStats::Read().vm_rss;
}

TEST_F(MallocTest, OS) {
  int ps = getpagesize();
  struct rusage ru;
  char* map;
  int n = 1024;
  int i;

  memset(&ru, 0, sizeof(ru));
  ASSERT_EQ(0, getrusage(RUSAGE_SELF, &ru));
  LOG(INFO) << "start rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;

  map = (char*)mmap(NULL, ps * n, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_TRUE(map != MAP_FAILED);
  LOG(INFO) << "after mmap rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;
  for (i = 0; i < n; i++) {
    map[ps * i] = i + 10;
  }

  ASSERT_EQ(0, getrusage(RUSAGE_SELF, &ru));
  LOG(INFO) << "after mmap + touch: rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;

  /* Unnecessary call to madvise to fault in that part of libc. */
  ASSERT_EQ(0, madvise(&map[ps], ps, MADV_NORMAL));

  ASSERT_EQ(0, getrusage(RUSAGE_SELF, &ru));
  LOG(INFO) << "after madvise: rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;

  for (i = 0; i < n; i++) {
    map[ps * i] = i + 10;
  }

  ASSERT_EQ(0, getrusage(RUSAGE_SELF, &ru));
  LOG(INFO) << "after madvise + touch: rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;

  ASSERT_EQ(0, madvise(map, ps * n, MADV_DONTNEED));

  ASSERT_EQ(0, getrusage(RUSAGE_SELF, &ru));
  LOG(INFO) << "after madvise + dontneed: rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;

  for (i = 0; i < n; i++) {
    map[ps * i] = i + 10;
  }

  ASSERT_EQ(0, getrusage(RUSAGE_SELF, &ru));
  LOG(INFO) << "after MADV_DONTNEED + touch: rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;

  constexpr auto kDecommitFlags = MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
  mmap(map, ps * n, PROT_NONE, kDecommitFlags, -1, 0);
  LOG(INFO) << "after decommi: rss: " << VmmRss() << ", ru_minflt: " << ru.ru_minflt;

  munmap(map, ps * n);
}

extern "C" mi_stats_t _mi_stats_main;

TEST_F(MallocTest, MimallocVisit) {
  // in version 2.0.2 "area->used" is number of used blocks and not bytes.
  // to get bytes one needs to multiple by block_size reported to the visitor.
  struct Sum {
    size_t reserved = 0, used = 0, committed = 0;
  } sum;

  auto cb_visit = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                     size_t block_size, void* arg) {
    Sum* s = (Sum*)arg;

    LOG(INFO) << "block_size " << block_size << "/" << area->block_size << ", reserved "
              << area->reserved << " comitted " << area->committed << " used: " << area->used;

    s->reserved += area->reserved;
    s->used += area->used * block_size;
    s->committed += area->committed;

    return true;
  };

  auto* myheap = mi_heap_new();

  void* p1 = mi_heap_malloc(myheap, 64);

  for (size_t i = 0; i < 50; ++i)
    p1 = mi_heap_malloc(myheap, 128);
  (void)p1;

  void* ptr[50];

  // allocate 50
  for (size_t i = 0; i < 50; ++i) {
    ptr[i] = mi_heap_malloc(myheap, 256);
  }

  // free 50/50 -
  for (size_t i = 0; i < 50; ++i) {
    mi_free(ptr[i]);
  }

  mi_heap_visit_blocks(myheap, false /* visit all blocks*/, cb_visit, &sum);

  mi_collect(false);
  mi_stats_print_out(NULL, NULL);

#define LOG_STATS(x) LOG(INFO) << #x ": " << _mi_stats_main.x.current

  LOG(INFO) << "visit: committed/reserved/used: " << sum.committed << "/" << sum.reserved << "/"
            << sum.used;

  LOG_STATS(committed);
  LOG_STATS(malloc);
  LOG_STATS(reserved);
  LOG_STATS(normal);
  mi_heap_destroy(myheap);
}

/* Setup to test various resource limiting frameworks:

cgroup2:
  mkdir /sys/fs/cgroup/memory/test
  echo 2000K | sudo tee  /sys/fs/cgroup/test/memory.max
  sudo swapoff -a
  echo 0 | sudo tee  /sys/fs/cgroup/test/memory.swap.max
  ...
  run in debugger this test.
  echo $(pidof malloc_test) | sudo tee  /sys/fs/cgroup/test/cgroup.procs

Summary: cgroups kills the process at "memset" line.
         the behavior stays the same with "echo 2 | sudo tee /proc/sys/vm/overcommit_memory".

         It seems that it's not possible to use cgroups in order to limit of allocations via mmap.

prlimit:
   prlimit --as=25000000  ./malloc_test

   fails on mi_malloc assertion EXPECT_TRUE(ptr != nullptr);
   It seems that je_malloc does not respect prlimit.

   prlimit --data=12000000  ./malloc_test
   again, mi_malloc respects RLIMIT_DATA and je_malloc does not.

   https://eli.thegreenplace.net/2009/10/30/handling-out-of-memory-conditions-in-c
*/

TEST_F(MallocTest, LargeAlloc) {
  constexpr size_t kAllocSz = 1 << 24;  // 16MB
  LOG(INFO) << "Before mi_malloc\n";

  void* ptr = mi_malloc(kAllocSz);
  if (ptr) {
    memset(ptr, 0, kAllocSz);
    mi_free(ptr);
  }

  LOG(INFO) << "Before jemalloc\n";

  ptr = je_malloc(kAllocSz);
  if (ptr) {
    memset(ptr, 0, kAllocSz);
    je_free(ptr);
  }
}

TEST_F(MallocTest, Stats) {
  size_t rss, active;
  size_t sz = sizeof(rss);
  je_mallctl("stats.active", &active, &sz, NULL, 0);
  je_mallctl("stats.resident", &rss, &sz, NULL, 0);
  EXPECT_GT(active, 0);
  EXPECT_GT(rss, active);
}

// When MI_SECURE mode is enabled, double free is detected and the process can be killed.
#if 0
TEST_F(MallocTest, DoubleFree) {
  mi_register_error([](int err, void*) { LOG_IF(FATAL, err == EAGAIN) << "Double free detected"; },
                    nullptr);
  void* p = mi_malloc(64);
  mi_free(p);
  EXPECT_DEATH(mi_free(p), "Double free detected");
}
#endif

/* mimalloc notes:
  segment allocates itself from arena. by default only MI_MEMID_OS is being used for allocations
  which is OS backed proxy arena.
  _mi_arena_alloc_aligned/_mi_arena_alloc is how the internal code allocates from the arena.
  dev-slice branch (latest 2.0.2 tag) does not have region.c in its codebase.
  Segments allocate directly from arena.
*/

#ifdef __GLIBC__  // musl does not have mallinfo

#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
static void BM_MallocStats(benchmark::State& state) {
  while (state.KeepRunning()) {
    mallinfo();  // mallinfo2 is not present on u20.04.
  }
}
BENCHMARK(BM_MallocStats);

#endif

static void BM_MimallocProcessInfo(benchmark::State& state) {
  size_t elapsed_msecs, user_msecs, system_msecs, current_rss, peak_rss, current_commit,
      peak_commit, page_faults;

  // mi_process_info uses getrusage underneath for linux systems.
  while (state.KeepRunning()) {
    mi_process_info(&elapsed_msecs, &user_msecs, &system_msecs, &current_rss, &peak_rss,
                    &current_commit, &peak_commit, &page_faults);
  }
}
BENCHMARK(BM_MimallocProcessInfo);

static bool VisitArena(const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                       size_t block_size, void* arg) {
  return true;
}

static void BM_MimallocHeapVisit(benchmark::State& state) {
  void* ptr[10];

  for (unsigned i = 10; i < 20; ++i) {
    ptr[i - 10] = mi_malloc(1 << i);
  }

  while (state.KeepRunning()) {
    mi_heap_visit_blocks(mi_heap_get_default(), false, &VisitArena, nullptr);
  }

  for (unsigned i = 10; i < 20; ++i) {
    mi_free(ptr[i - 10]);
  }
}
BENCHMARK(BM_MimallocHeapVisit);

}  // namespace base

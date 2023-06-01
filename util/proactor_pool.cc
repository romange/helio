// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/proactor_pool.h"

#include "base/flags.h"
#include "base/logging.h"
#include "base/pthread_utils.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#ifdef __FreeBSD__
#include <pthread_np.h>
#endif

using namespace std;

ABSL_FLAG(uint32_t, proactor_threads, 0, "Number of io threads in the pool");
ABSL_FLAG(string, proactor_affinity_mode, "on", "can be on, off or auto");

namespace util {

using fb2::ProactorBase;

namespace {
enum class AffinityMode {
  ON,
  OFF,
  AUTO,
};

#if defined(__linux__) || defined(__FreeBSD__)

constexpr int kTotalCpus = CPU_SETSIZE;

static cpu_set_t OnlineCpus() {
  cpu_set_t online_cpus;
  CPU_ZERO(&online_cpus);
  CHECK_EQ(0, sched_getaffinity(0, sizeof(online_cpus), &online_cpus));
  return online_cpus;
}

#elif defined(__APPLE__)

#define SYSCTL_CORE_COUNT "machdep.cpu.core_count"

constexpr unsigned kTotalCpus = 128;

typedef struct {
  uint64_t __bits[2];
} cpu_set_t;

void CPU_ZERO(cpu_set_t* cs) {
  cs->__bits[0] = 0;
  cs->__bits[1] = 0;
}

inline void CPU_SET(unsigned num, cpu_set_t* cs) {
  unsigned index = num / 64;
  unsigned rem = num & 63;
  cs->__bits[index] |= (1 << rem);
}

inline unsigned CPU_COUNT(const cpu_set_t* cs) {
  unsigned res = 0;
  for (auto v : cs->__bits) {
    res += __builtin_popcount(v);
  }
  return res;
}

static inline int CPU_ISSET(unsigned num, const cpu_set_t* cs) {
  unsigned index = num / 64;
  unsigned rem = num & 63;

  return cs->__bits[index] & (1 << rem);
}

static cpu_set_t OnlineCpus() {
  cpu_set_t online_cpus;
  CPU_ZERO(&online_cpus);

  int32_t core_count = 0;
  size_t len = sizeof(core_count);

  int ret = sysctlbyname(SYSCTL_CORE_COUNT, &core_count, &len, 0, 0);
  CHECK_EQ(0, ret);

  for (int i = 0; i < core_count; ++i) {
    CPU_SET(i, &online_cpus);
  }

  return online_cpus;
}

#else
#error "unsupported architecture "
#endif

static unsigned NumOnlineCpus() {
  cpu_set_t cpus = OnlineCpus();
  return CPU_COUNT(&cpus);
}

}  // namespace

ProactorPool::ProactorPool(std::size_t pool_size) {
  if (pool_size == 0) {
    auto num_pthreads = absl::GetFlag(FLAGS_proactor_threads);
    // thread::hardware_concurrency() returns number of online cpus but ignores taskset.
    pool_size = num_pthreads > 0 ? num_pthreads : NumOnlineCpus();
    VLOG(1) << "Setting pool size to " << pool_size;
  }

  pool_size_ = pool_size;
  proactor_.reset(new ProactorBase*[pool_size]);
  std::fill(proactor_.get(), proactor_.get() + pool_size, nullptr);
}

ProactorPool::~ProactorPool() {
  Stop();
  for (size_t i = 0; i < pool_size_; ++i) {
    delete proactor_[i];
  }
}

void ProactorPool::CheckRunningState() {
  CHECK_EQ(RUN, state_);
}

void ProactorPool::Run() {
  SetupProactors();

  Await([](unsigned index, auto*) {
  // It seems to simplify things in kernel for io_uring.
  // https://github.com/axboe/liburing/issues/218
  // I am not sure what's how it impacts higher application levels.
#ifdef __linux__
    unshare(CLONE_FS);
#endif
    ProactorBase::SetIndex(index);
  });

  LOG(INFO) << "Running " << pool_size_ << " io threads";
}

void ProactorPool::Stop() {
  if (state_ == STOPPED)
    return;

  for (size_t i = 0; i < pool_size_; ++i) {
    proactor_[i]->Stop();
  }

  VLOG(1) << "Proactors have been stopped";

  for (size_t i = 0; i < pool_size_; ++i) {
    pthread_join(proactor_[i]->thread_id(), nullptr);
    VLOG(2) << "Thread " << i << " has joined";
  }
  state_ = STOPPED;
}

ProactorBase* ProactorPool::GetNextProactor() {
  uint32_t index = next_io_context_.load(std::memory_order_relaxed);
  // Use a round-robin scheme to choose the next io_context to use.
  DCHECK_LT(index, pool_size_);

  ProactorBase* proactor = at(index++);

  // Not-perfect round-robind since this function is non-transactional but it "works".
  if (index >= pool_size_)
    index = 0;

  next_io_context_.store(index, std::memory_order_relaxed);
  return proactor;
}

std::string_view ProactorPool::GetString(std::string_view source) {
  if (source.empty()) {
    return source;
  }

  folly::RWSpinLock::ReadHolder rh(str_lock_);
  auto it = str_set_.find(source);
  if (it != str_set_.end())
    return *it;
  rh.reset();

  folly::RWSpinLock::WriteHolder wh(str_lock_);

  // we check again if str_set_ contains source under write lock to provide strong
  // consistency.
  it = str_set_.find(source);
  if (it != str_set_.end()) {
    return *it;
  }

#if defined(__linux__)
  void* new_block = str_arena_.allocate(source.size(), 1);
#else
  void* new_block = new char[source.size()];
#endif
  memcpy(new_block, source.data(), source.size());
  std::string_view res(reinterpret_cast<char*>(new_block), source.size());
  str_set_.insert(res);

  return res;
}

void ProactorPool::SetupProactors() {
  CHECK_EQ(STOPPED, state_);
  string affinity_flag = absl::GetFlag(FLAGS_proactor_affinity_mode);
  AffinityMode mode = AffinityMode::AUTO;
  if (affinity_flag == "on") {
    mode = AffinityMode::ON;
  } else if (affinity_flag == "off") {
    mode = AffinityMode::OFF;
  } else if (affinity_flag == "auto") {
    mode = AffinityMode::AUTO;
  } else {
    LOG(FATAL) << "Invalid proactor_affinity_mode flag value: " << affinity_flag;
  }

  char buf[32];

  cpu_set_t online_cpus = OnlineCpus();
  unsigned num_online_cpus = CPU_COUNT(&online_cpus);
  unsigned rel_to_abs_cpu[num_online_cpus];
  unsigned rel_cpu_index = 0, abs_cpu_index = 0;

  for (; abs_cpu_index < kTotalCpus; abs_cpu_index++) {
    if (CPU_ISSET(abs_cpu_index, &online_cpus)) {
      rel_to_abs_cpu[rel_cpu_index] = abs_cpu_index;
      rel_cpu_index++;

      if (rel_cpu_index == num_online_cpus)
        break;
    }
  }
  CHECK_EQ(rel_cpu_index, num_online_cpus) << "Such beast is not supported";
  cpu_threads_.resize(abs_cpu_index + 1);

  cpu_set_t cps;
  CPU_ZERO(&cps);

  bool set_affinity = (mode == AffinityMode::ON) ||
                      (mode == AffinityMode::AUTO && pool_size_ > num_online_cpus / 2);

  for (unsigned i = 0; i < pool_size_; ++i) {
    snprintf(buf, sizeof(buf), "Proactor%u", i);

    proactor_[i] = CreateProactor();
    auto cb = [this, i]() mutable {
      this->InitInThread(i);
      proactor_[i]->Run();
    };

    pthread_t tid = base::StartThread(buf, std::move(cb));
#if defined(__linux__) || defined(__FreeBSD__)
    if (set_affinity) {
      // Spread proactor threads across online CPUs.
      int rel_indx = i % num_online_cpus;
      unsigned abs_cpu = rel_to_abs_cpu[rel_indx];
      CHECK_LT(abs_cpu, cpu_threads_.size());
      CPU_SET(abs_cpu, &cps);

      int rc = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cps);
      if (rc == 0) {
        VLOG(1) << "Setting affinity of thread " << i << " on cpu " << abs_cpu;
        cpu_threads_[abs_cpu].push_back(i);
      } else {
        LOG(WARNING) << "Error calling pthread_setaffinity_np: " << strerror(rc) << "\n";
      }

      CPU_CLR(abs_cpu, &cps);
    }
#else
    (void)tid;
    (void)set_affinity;
#endif
  }

  state_ = RUN;
}

const vector<unsigned>& ProactorPool::MapCpuToThreads(unsigned cpu_id) const {
  static vector<unsigned> empty;

  if (cpu_id >= cpu_threads_.size()) {
    return empty;
  }
  return cpu_threads_[cpu_id];
}

}  // namespace util

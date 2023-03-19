// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/detail/scheduler.h"

#include <absl/base/internal/spinlock.h>

#include <condition_variable>
#include <mutex>

#include "base/function2.hpp"
#include "base/logging.h"

namespace util {
namespace fb2 {
namespace detail {

namespace ctx = boost::context;
using namespace std;

namespace {

constexpr size_t kSizeOfCtx = sizeof(FiberInterface);  // because of the virtual +8 bytes.
constexpr size_t kSizeOfSH = sizeof(FI_SleepHook);
constexpr size_t kSizeOfLH = sizeof(FI_ListHook);

using SpinLockType = ::absl::base_internal::SpinLock;

inline void CpuPause() {
#if defined(__i386__) || defined(__amd64__)
  __asm__ __volatile__("pause");
#elif defined(__aarch64__)
  /* Use an isb here as we've found it's much closer in duration to
   * the x86 pause instruction vs. yield which is a nop and thus the
   * loop count is lower and the interconnect gets a lot more traffic
   * from loading the ticket above. */
  __asm__ __volatile__("isb");
#endif
}

template <typename T> void WriteOnce(T src, T* dest) {
  std::atomic_store_explicit(reinterpret_cast<std::atomic<T>*>(dest), src,
                             std::memory_order_relaxed);
}

template <typename T> T ReadOnce(T* src) {
  return std::atomic_load_explicit(reinterpret_cast<std::atomic<T>*>(src),
                                   std::memory_order_relaxed);
}

// Thomas Wang's 64 bit Mix Function.
inline uint64_t MixHash(uint64_t key) {
  key += ~(key << 32);
  key ^= (key >> 22);
  key += ~(key << 13);
  key ^= (key >> 8);
  key += (key << 3);
  key ^= (key >> 15);
  key += ~(key << 27);
  key ^= (key >> 31);
  return key;
}

using WaitQueue = boost::intrusive::slist<
    detail::FiberInterface,
    boost::intrusive::member_hook<detail::FiberInterface, detail::FI_ListHook,
                                  &detail::FiberInterface::list_hook>,
    boost::intrusive::constant_time_size<false>, boost::intrusive::cache_last<true>>;

struct ParkingBucket {
  SpinLockType lock;
  WaitQueue waiters;
  bool was_rehashed = 0;
};

constexpr size_t kSzPB = sizeof(ParkingBucket);

class ParkingHT {
  struct SizedBuckets {
    unsigned num_buckets;
    ParkingBucket* arr;

    SizedBuckets(unsigned shift) : num_buckets(1 << shift) {
      arr = new ParkingBucket[num_buckets];
    }

    unsigned GetBucket(uint64_t hash) const {
      return hash & bucket_mask();
    }

    unsigned bucket_mask() const {
      return num_buckets - 1;
    }
  };

 public:
  ParkingHT();
  ~ParkingHT();

  // if validate returns true, the fiber is not added to the queue.
  bool Emplace(uint64_t token, FiberInterface* fi, fu2::unique_function<bool()> validate);

  FiberInterface* Remove(uint64_t token, fu2::function<void(FiberInterface*)> wakeup_fn);

 private:
  void TryRehash(SizedBuckets* cur_sb);

  atomic<SizedBuckets*> buckets_;
  atomic_uint32_t num_entries_{0};
  atomic_bool rehashing_{false};
};

class DispatcherImpl final : public FiberInterface {
 public:
  DispatcherImpl(ctx::preallocated const& palloc, ctx::fixedsize_stack&& salloc,
                 Scheduler* sched) noexcept;
  ~DispatcherImpl();

  bool is_terminating() const {
    return is_terminating_;
  }

  void Notify() {
    unique_lock<mutex> lk(mu_);
    wake_suspend_ = true;
    cnd_.notify_one();
  }

 private:
  void DefaultDispatch(Scheduler* sched);

  ctx::fiber Run(ctx::fiber&& c);

  bool is_terminating_ = false;

  // This is used to wake up the scheduler from sleep.
  bool wake_suspend_ = false;

  mutex mu_;
  condition_variable cnd_;
};

// Serves as a stub Fiber since it does not allocate any stack.
// It's used as a main fiber of the thread.
class MainFiberImpl final : public FiberInterface {
 public:
  MainFiberImpl() noexcept : FiberInterface{MAIN, 1, "main"} {
  }

  ~MainFiberImpl() {
    use_count_.fetch_sub(1, memory_order_relaxed);
  }

 protected:
  void Terminate() {
  }
};

DispatcherImpl* MakeDispatcher(Scheduler* sched) {
  ctx::fixedsize_stack salloc;
  ctx::stack_context sctx = salloc.allocate();
  ctx::preallocated palloc = MakePreallocated<DispatcherImpl>(sctx);

  void* sp_ptr = palloc.sp;

  // placement new of context on top of fiber's stack
  return new (sp_ptr) DispatcherImpl{std::move(palloc), std::move(salloc), sched};
}

ParkingHT* g_parking_ht = nullptr;
mutex g_scheduler_lock;

using QsbrEpoch = uint32_t;
constexpr QsbrEpoch kEpochInc = 2;
atomic<QsbrEpoch> qsbr_global_epoch{1};  // global is always non-zero.

struct TL_FiberInitializer;
TL_FiberInitializer* g_fiber_thread_list = nullptr;

// Per thread initialization structure.
struct TL_FiberInitializer {
  TL_FiberInitializer* next = nullptr;
  QsbrEpoch local_epoch = 0;

  // Currently active fiber.
  FiberInterface* active;

  // Per-thread scheduler instance.
  // Allows overriding the main dispatch loop
  Scheduler* sched;

  TL_FiberInitializer(const TL_FiberInitializer&) = delete;

  TL_FiberInitializer() noexcept;

  ~TL_FiberInitializer();
};

TL_FiberInitializer::TL_FiberInitializer() noexcept : sched(nullptr) {
  DVLOG(1) << "Initializing FiberLib";

  // main fiber context of this thread.
  // We use it as a stub
  FiberInterface* main_ctx = new MainFiberImpl{};
  active = main_ctx;
  sched = new Scheduler(main_ctx);
  unique_lock lk(g_scheduler_lock);
  if (g_parking_ht == nullptr) {
    g_parking_ht = new ParkingHT{};
  }
  next = g_fiber_thread_list;
  g_fiber_thread_list = this;
}

TL_FiberInitializer::~TL_FiberInitializer() {
  FiberInterface* main_cntx = sched->main_context();
  delete sched;
  delete main_cntx;
  unique_lock lk(g_scheduler_lock);
  TL_FiberInitializer** p = &g_fiber_thread_list;
  while (*p != this) {
    p = &(*p)->next;
  }
  *p = next;
  if (g_fiber_thread_list == nullptr) {
    delete g_parking_ht;
    g_parking_ht = nullptr;
  }
}

TL_FiberInitializer& FbInitializer() noexcept {
  // initialized the first time control passes; per thread
  thread_local static TL_FiberInitializer fb_initializer;
  return fb_initializer;
}

void qsbr_checkpoint() {
  atomic_thread_fence(memory_order_seq_cst);

  // syncs the local_epoch with the global_epoch.
  WriteOnce(qsbr_global_epoch.load(memory_order_relaxed), &FbInitializer().local_epoch);
}

void qsbr_worker_fiber_offline() {
  atomic_thread_fence(memory_order_release);
  WriteOnce(0U, &FbInitializer().local_epoch);
}

void qsbr_worker_fiber_online() {
  WriteOnce(qsbr_global_epoch.load(memory_order_relaxed), &FbInitializer().local_epoch);
  atomic_thread_fence(memory_order_seq_cst);
}

bool qsbr_sync(uint64_t target) {
  unique_lock lk(g_scheduler_lock, try_to_lock);

  if (!lk)
    return false;

  FbInitializer().local_epoch = target;
  for (TL_FiberInitializer* p = g_fiber_thread_list; p != nullptr; p = p->next) {
    auto local_epoch = ReadOnce(&p->local_epoch);
    if (local_epoch && local_epoch != target) {
      return false;
    }
  }

  return true;
}

ParkingHT::ParkingHT() {
  SizedBuckets* sb = new SizedBuckets(6);
  buckets_.store(sb, memory_order_release);
}

ParkingHT::~ParkingHT() {
  SizedBuckets* sb = buckets_.load(memory_order_relaxed);

  DVLOG(1) << "Destroying ParkingHT with " << sb->num_buckets << " buckets";

  for (unsigned i = 0; i < sb->num_buckets; ++i) {
    ParkingBucket* pb = sb->arr + i;
    absl::base_internal::SpinLockHolder h(&pb->lock);
    CHECK(pb->waiters.empty());
  }
  delete[] sb->arr;
  delete sb;
}

bool ParkingHT::Emplace(uint64_t token, FiberInterface* fi, fu2::unique_function<bool()> validate) {
  uint32_t num_items = 0;
  unsigned bucket = 0;
  SizedBuckets* sb = nullptr;
  uint64_t hash = MixHash(token);
  while (true) {
    sb = buckets_.load(memory_order_acquire);
    DCHECK(sb);
    bucket = sb->GetBucket(hash);
    DVLOG(2) << "Emplace: token=" << token << " bucket=" << bucket;

    ParkingBucket* pb = sb->arr + bucket;
    {
      absl::base_internal::SpinLockHolder h(&pb->lock);
      if (validate()) {
        qsbr_checkpoint();
        return false;  // did not park
      }
      if (pb->was_rehashed) {  // has grown
        continue;              // reload the buckets because it could change during the lock
      }
      fi->set_park_token(token);
      pb->waiters.push_front(*fi);
      num_items = num_entries_.fetch_add(1, memory_order_relaxed);
      break;
    }
  }
  DVLOG(2) << "EmplaceEnd: token=" << token << " bucket=" << bucket;

  if (num_items > sb->num_buckets * 2) {
    TryRehash(sb);
  }

  // we do not call qsbr_checkpoint here because we are going to park
  // and call qsbr_worker_fiber_offline.
  return true;
}

FiberInterface* ParkingHT::Remove(uint64_t token, fu2::function<void(FiberInterface*)> wakeup_fn) {
  uint64_t hash = MixHash(token);
  SizedBuckets* sb = nullptr;
  while (true) {
    sb = buckets_.load(memory_order_acquire);
    unsigned bucket = sb->GetBucket(hash);
    ParkingBucket* pb = sb->arr + bucket;
    {
      absl::base_internal::SpinLockHolder h(&pb->lock);
      if (pb->was_rehashed) {
        continue;
      }

      for (auto it = pb->waiters.begin(); it != pb->waiters.end(); ++it) {
        if (it->park_token() == token) {
          FiberInterface* fi = &*it;
          pb->waiters.erase(it);
          auto prev = num_entries_.fetch_sub(1, memory_order_relaxed);
          DCHECK_GT(prev, 0u);
          wakeup_fn(fi);
          qsbr_checkpoint();

          return fi;
        }
      }
    }
  }

  qsbr_checkpoint();
  return nullptr;
}

void ParkingHT::TryRehash(SizedBuckets* cur_sb) {
  if (rehashing_.exchange(true, memory_order_acquire)) {
    return;
  }

  SizedBuckets* sb = buckets_.load(memory_order_relaxed);
  if (sb != cur_sb) {
    rehashing_.store(false, memory_order_release);
    return;
  }

  DVLOG(1) << "Rehashing parking hash table from " << sb->num_buckets;

  // log2(x)
  static_assert(__builtin_ctz(32) == 5);

  SizedBuckets* new_sb = new SizedBuckets(__builtin_ctz(sb->num_buckets) + 2);
  for (unsigned i = 0; i < sb->num_buckets; ++i) {
    sb->arr[i].lock.Lock();
  }
  for (unsigned i = 0; i < sb->num_buckets; ++i) {
    ParkingBucket* pb = sb->arr + i;
    pb->was_rehashed = true;
    while (!pb->waiters.empty()) {
      FiberInterface* fi = &pb->waiters.front();
      pb->waiters.pop_front();
      uint64_t hash = MixHash(fi->park_token());
      unsigned bucket = new_sb->GetBucket(hash);
      ParkingBucket* new_pb = new_sb->arr + bucket;
      new_pb->waiters.push_back(*fi);
    }
  }
  buckets_.store(new_sb, memory_order_release);

  for (unsigned i = 0; i < sb->num_buckets; ++i) {
    sb->arr[i].lock.Unlock();
  }

  uint64_t next_epoch = qsbr_global_epoch.fetch_add(kEpochInc, memory_order_relaxed) + kEpochInc;

  FbInitializer().sched->Defer(next_epoch, [sb] {
    DVLOG(1) << "Destroying old SizedBuckets with " << sb->num_buckets << " buckets";
    delete sb;
  });

  rehashing_.store(false, memory_order_release);
}

// DispatcherImpl implementation.
DispatcherImpl::DispatcherImpl(ctx::preallocated const& palloc, ctx::fixedsize_stack&& salloc,
                               detail::Scheduler* sched) noexcept
    : FiberInterface{DISPATCH, 0, "_dispatch"} {
  entry_ = ctx::fiber(std::allocator_arg, palloc, salloc,
                      [this](ctx::fiber&& caller) { return Run(std::move(caller)); });
  scheduler_ = sched;
}

DispatcherImpl::~DispatcherImpl() {
  DVLOG(1) << "~DispatcherImpl";

  DCHECK(!entry_);
}

ctx::fiber DispatcherImpl::Run(ctx::fiber&& c) {
  if (c) {
    // We context switched from intrusive_ptr_release and this object is destroyed.
    return std::move(c);
  }

  // Normal SwitchTo operation.

  auto& fb_init = detail::FbInitializer();
  if (fb_init.sched->policy()) {
    fb_init.sched->policy()->Run(fb_init.sched);
  } else {
    DefaultDispatch(fb_init.sched);
  }

  DVLOG(1) << "Dispatcher exiting, switching to main_cntx";
  is_terminating_ = true;

  // Like with worker fibers, we switch to another fiber, but in this case to the main fiber.
  // We will come back here during the deallocation of DispatcherImpl from intrusive_ptr_release
  // in order to return from Run() and come back to main context.
  auto fc = fb_init.sched->main_context()->SwitchTo();

  DCHECK(fc);  // Should bring us back to main, into intrusive_ptr_release.
  return fc;
}

void DispatcherImpl::DefaultDispatch(Scheduler* sched) {
  DCHECK(FiberActive() == this);
  DCHECK(!list_hook.is_linked());

  while (true) {
    if (sched->IsShutdown()) {
      if (sched->num_worker_fibers() == 0)
        break;
    }
    sched->DestroyTerminated();
    sched->ProcessRemoteReady();

    if (sched->HasReady()) {
      FiberInterface* fi = sched->PopReady();
      DCHECK(!fi->list_hook.is_linked());
      DCHECK(!fi->sleep_hook.is_linked());
      sched->AddReady(this);

      DVLOG(2) << "Switching to " << fi->name();
      qsbr_worker_fiber_online();
      fi->SwitchTo();
      DCHECK(!list_hook.is_linked());
      DCHECK(FiberActive() == this);
      qsbr_worker_fiber_offline();
    } else {
      unique_lock<mutex> lk{mu_};

      cnd_.wait(lk, [&]() { return wake_suspend_; });
      wake_suspend_ = false;
    }
    sched->RunDeferred();
  }
  sched->DestroyTerminated();
}

}  // namespace

Scheduler::Scheduler(FiberInterface* main_cntx) : main_cntx_(main_cntx) {
  DCHECK(!main_cntx->scheduler_);
  main_cntx->scheduler_ = this;
  dispatch_cntx_.reset(MakeDispatcher(this));
}

Scheduler::~Scheduler() {
  shutdown_ = true;
  DCHECK(main_cntx_ == FiberActive());

  while (HasReady()) {
    FiberInterface* fi = PopReady();
    DCHECK(!fi->list_hook.is_linked());
    DCHECK(!fi->sleep_hook.is_linked());
    fi->SwitchTo();
  }

  DispatcherImpl* dimpl = static_cast<DispatcherImpl*>(dispatch_cntx_.get());
  if (!dimpl->is_terminating()) {
    DVLOG(1) << "~Scheduler switching to dispatch " << dispatch_cntx_->IsDefined();
    auto fc = dispatch_cntx_->SwitchTo();
    CHECK(!fc);
    CHECK(dimpl->is_terminating());
  }
  delete custom_policy_;
  custom_policy_ = nullptr;

  CHECK_EQ(0u, num_worker_fibers_);

  // destroys the stack and the object via intrusive_ptr_release.
  dispatch_cntx_.reset();
  DestroyTerminated();
}

ctx::fiber_context Scheduler::Preempt() {
  if (ready_queue_.empty()) {
    return dispatch_cntx_->SwitchTo();
  }

  DCHECK(!ready_queue_.empty());
  FiberInterface* fi = &ready_queue_.front();
  ready_queue_.pop_front();

  __builtin_prefetch(fi);
  return fi->SwitchTo();
}

void Scheduler::ScheduleFromRemote(FiberInterface* cntx) {
  remote_ready_queue_.Push(cntx);

  if (custom_policy_) {
    custom_policy_->Notify();
  } else {
    DispatcherImpl* dimpl = static_cast<DispatcherImpl*>(dispatch_cntx_.get());
    dimpl->Notify();
  }
}

void Scheduler::Attach(FiberInterface* cntx) {
  cntx->scheduler_ = this;
  if (cntx->type() == FiberInterface::WORKER) {
    ++num_worker_fibers_;
  }
}

void Scheduler::ScheduleTermination(FiberInterface* cntx) {
  terminate_queue_.push_back(*cntx);
  if (cntx->type() == FiberInterface::WORKER) {
    --num_worker_fibers_;
  }
}

void Scheduler::DestroyTerminated() {
  while (!terminate_queue_.empty()) {
    FiberInterface* tfi = &terminate_queue_.front();
    terminate_queue_.pop_front();
    DVLOG(2) << "Releasing terminated " << tfi->name_;

    // maybe someone holds a Fiber handle and waits for the fiber to join.
    intrusive_ptr_release(tfi);
  }
}

void Scheduler::WaitUntil(chrono::steady_clock::time_point tp, FiberInterface* me) {
  DCHECK(!me->sleep_hook.is_linked());
  me->tp_ = tp;
  sleep_queue_.insert(*me);
  auto fc = Preempt();
  DCHECK(!fc);
}

void Scheduler::ProcessRemoteReady() {
  while (true) {
    FiberInterface* fi = remote_ready_queue_.Pop();
    if (!fi)
      break;
    DVLOG(2) << "set ready " << fi->name();

    ready_queue_.push_back(*fi);
  }
}

void Scheduler::ProcessSleep() {
  if (sleep_queue_.empty())
    return;

  chrono::steady_clock::time_point now = chrono::steady_clock::now();
  do {
    auto it = sleep_queue_.begin();
    if (it->tp_ > now)
      break;

    FiberInterface& fi = *it;
    sleep_queue_.erase(it);
    ready_queue_.push_back(fi);
  } while (!sleep_queue_.empty());
}

void Scheduler::AttachCustomPolicy(DispatchPolicy* policy) {
  CHECK(custom_policy_ == nullptr);
  custom_policy_ = policy;
}

void Scheduler::RunDeferred() {
  bool skip_validation = false;

  while (!deferred_cb_.empty()) {
    const auto& k_v = deferred_cb_.back();
    if (skip_validation) {
      k_v.second();
      deferred_cb_.pop_back();

      continue;
    }

    if (!qsbr_sync(k_v.first))
      break;

    k_v.second();
    skip_validation = true;
    deferred_cb_.pop_back();
  }
}

FiberInterface* FiberActive() noexcept {
  return FbInitializer().active;
}

FiberInterface::FiberInterface(Type type, uint32_t cnt, string_view nm)
    : use_count_(cnt), flags_(0), type_(type) {
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  name_[len] = 0;
  if (len) {
    memcpy(name_, nm.data(), len);
  }
}

FiberInterface::~FiberInterface() {
  DVLOG(2) << "Destroying " << name_;
  DCHECK_EQ(use_count_.load(), 0u);
  DCHECK(wait_queue_.empty());
  DCHECK(!list_hook.is_linked());
}

void FiberInterface::SetName(std::string_view nm) {
  if (nm.empty())
    return;
  size_t len = std::min(nm.size(), sizeof(name_) - 1);
  memcpy(name_, nm.data(), len);
  name_[len] = 0;
}

// We can not destroy this instance within the context of the fiber it's been running in.
// The reason: the instance is hosted within the stack region of the fiber itself, and it
// implicitly destroys the stack when destroying its 'entry_' member variable.
// Therefore, to destroy a FiberInterface (WORKER) object, we must call intrusive_ptr_release
// from another fiber. intrusive_ptr_release is smart about how it releases resources too.
ctx::fiber_context FiberInterface::Terminate() {
  DCHECK(this == FiberActive());
  DCHECK(!list_hook.is_linked());

  scheduler_->ScheduleTermination(this);
  DVLOG(2) << "Terminating " << name_;

  while (true) {
    uint16_t fprev = flags_.fetch_or(kTerminatedBit | kBusyBit, memory_order_acquire);
    if ((fprev & kBusyBit) == 0) {
      break;
    }
    CpuPause();
  }

  while (!wait_queue_.empty()) {
    FiberInterface* wait_fib = &wait_queue_.front();
    wait_queue_.pop_front();
    DVLOG(2) << "Scheduling " << wait_fib;

    ActivateOther(wait_fib);
  }

  flags_.fetch_and(~kBusyBit, memory_order_release);

  // usually Preempt returns empty fc but here we return the value of where
  // to switch to when this fiber completes. See intrusive_ptr_release for more info.
  return scheduler_->Preempt();
}

void FiberInterface::Start(Launch launch) {
  auto& fb_init = detail::FbInitializer();
  fb_init.sched->Attach(this);

  switch (launch) {
    case Launch::post:
      fb_init.sched->AddReady(this);
      break;
    case Launch::dispatch:
      fb_init.sched->AddReady(fb_init.active);
      {
        auto fc = SwitchTo();
        DCHECK(!fc);
      }
      break;
  }
}

void FiberInterface::Join() {
  FiberInterface* active = FiberActive();

  CHECK(active != this);

  while (true) {
    uint16_t fprev = flags_.fetch_or(kBusyBit, memory_order_acquire);
    if (fprev & kTerminatedBit) {
      if ((fprev & kBusyBit) == 0) {                        // Caller became the owner.
        flags_.fetch_and(~kBusyBit, memory_order_relaxed);  // release the lock
      }
      return;
    }

    if ((fprev & kBusyBit) == 0) {  // Caller became the owner.
      break;
    }
    CpuPause();
  }

  wait_queue_.push_front(*active);
  flags_.fetch_and(~kBusyBit, memory_order_release);  // release the lock
  active->scheduler_->Preempt();
}

void FiberInterface::ActivateOther(FiberInterface* other) {
  DCHECK(other->scheduler_);

  // Check first if we the fiber belongs to the active thread.
  if (other->scheduler_ == scheduler_) {
    scheduler_->AddReady(other);
  } else {
    other->scheduler_->ScheduleFromRemote(other);
  }
}

void FiberInterface::Suspend() {
  scheduler_->Preempt();
}

void FiberInterface::WakeupOther(FiberInterface* other) {
  DCHECK(other->scheduler_ && other->scheduler_ != scheduler_);

  uintptr_t token = uintptr_t(other);
  FiberInterface* item = g_parking_ht->Remove(
      token, [](FiberInterface* fibi) { fibi->flags_.fetch_or(kWakeupBit, memory_order_relaxed); });

  CHECK(item == other);
  other->scheduler_->ScheduleFromRemote(other);
}

void FiberInterface::SuspendUntilWakeup() {
  uintptr_t token = uintptr_t(this);
  bool parked = g_parking_ht->Emplace(
      token, this, [this] { return flags_.load(memory_order_relaxed) & kWakeupBit; });
  if (parked) {
    scheduler_->Preempt();
  }
}

ctx::fiber_context FiberInterface::SwitchTo() {
  FiberInterface* prev = this;

  std::swap(FbInitializer().active, prev);

  // pass pointer to the context that resumes `this`
  return std::move(entry_).resume_with([prev](ctx::fiber_context&& c) {
    DCHECK(!prev->entry_);

    prev->entry_ = std::move(c);  // update the return address in the context we just switch from.
    return ctx::fiber_context{};
  });
}

}  // namespace detail

void SetCustomDispatcher(DispatchPolicy* policy) {
  detail::TL_FiberInitializer& fb_init = detail::FbInitializer();
  fb_init.sched->AttachCustomPolicy(policy);
}

DispatchPolicy::~DispatchPolicy() {
}

}  // namespace fb2
}  // namespace util

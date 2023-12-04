// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "util/fibers/detail/scheduler.h"

#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>

#include <condition_variable>
#include <mutex>

#include "base/logging.h"
#include "util/fibers/stacktrace.h"
#include "util/fibers/detail/utils.h"

namespace util {
namespace fb2 {
namespace detail {

namespace ctx = boost::context;

using namespace std;

namespace {

#if PARKING_ENABLED
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
  bool Emplace(uint64_t token, FiberInterface* fi, absl::FunctionRef<bool()> validate);

  FiberInterface* Remove(uint64_t token, absl::FunctionRef<void(FiberInterface*)> on_hit,
                         absl::FunctionRef<void()> on_miss);
  void RemoveAll(uint64_t token, WaitQueue* wq);

 private:
  void TryRehash(SizedBuckets* cur_sb);

  atomic<SizedBuckets*> buckets_;
  atomic_uint32_t num_entries_{0};
  atomic_bool rehashing_{false};
};

#endif

// ParkingHT* g_parking_ht = nullptr;

using QsbrEpoch = uint32_t;

#if PARKING_ENABLED
constexpr QsbrEpoch kEpochInc = 2;
atomic<QsbrEpoch> qsbr_global_epoch{1};  // global is always non-zero.

// TODO: we could move this checkpoint to the proactor loop.
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
    SpinLockHolder h(&pb->lock);
    CHECK(pb->waiters.empty());
  }
  delete[] sb->arr;
  delete sb;
}

// if validate returns true we do not park
bool ParkingHT::Emplace(uint64_t token, FiberInterface* fi, absl::FunctionRef<bool()> validate) {
  uint32_t num_items = 0;
  unsigned bucket = 0;
  SizedBuckets* sb = nullptr;
  uint64_t hash = MixHash(token);
  bool res = false;

  while (true) {
    sb = buckets_.load(memory_order_acquire);
    DCHECK(sb);
    bucket = sb->GetBucket(hash);
    VLOG(1) << "Emplace: token=" << token << " bucket=" << bucket;

    ParkingBucket* pb = sb->arr + bucket;
    {
      SpinLockHolder h(&pb->lock);

      if (!pb->was_rehashed) {  // has grown
        if (validate()) {
          break;
        }

        fi->set_park_token(token);
        pb->waiters.push_front(*fi);
        num_items = num_entries_.fetch_add(1, memory_order_relaxed);
        res = true;
        break;
      }
    }
  }

  if (res) {
    DVLOG(2) << "EmplaceEnd: token=" << token << " bucket=" << bucket;

    if (num_items > sb->num_buckets) {
      TryRehash(sb);
    }
  } else {
    qsbr_checkpoint();
  }

  // we do not call qsbr_checkpoint here because we are going to park
  // and call qsbr_worker_fiber_offline.
  return res;
}

FiberInterface* ParkingHT::Remove(uint64_t token, absl::FunctionRef<void(FiberInterface*)> on_hit,
                                  absl::FunctionRef<void()> on_miss) {
  uint64_t hash = MixHash(token);
  SizedBuckets* sb = nullptr;
  while (true) {
    sb = buckets_.load(memory_order_acquire);
    unsigned bucket = sb->GetBucket(hash);
    ParkingBucket* pb = sb->arr + bucket;
    {
      SpinLockHolder h(&pb->lock);
      VLOG(1) << "Remove: token=" << token << " bucket=" << bucket;

      if (!pb->was_rehashed) {
        for (auto it = pb->waiters.begin(); it != pb->waiters.end(); ++it) {
          if (it->park_token() == token) {
            FiberInterface* fi = &*it;
            pb->waiters.erase(it);
            auto prev = num_entries_.fetch_sub(1, memory_order_relaxed);
            DCHECK_GT(prev, 0u);
            on_hit(fi);
            // qsbr_checkpoint();

            return fi;
          }
        }
        on_miss();
        return nullptr;
      }
    }
  }

  qsbr_checkpoint();
  return nullptr;
}

void ParkingHT::RemoveAll(uint64_t token, WaitQueue* wq) {
  uint64_t hash = MixHash(token);
  SizedBuckets* sb = nullptr;

  while (true) {
    sb = buckets_.load(memory_order_acquire);
    unsigned bucket = sb->GetBucket(hash);
    ParkingBucket* pb = sb->arr + bucket;
    {
      SpinLockHolder h(&pb->lock);
      if (!pb->was_rehashed) {
        auto it = pb->waiters.begin();
        while (it != pb->waiters.end()) {
          if (it->park_token() != token) {
            ++it;
            continue;
          }
          FiberInterface* fi = &*it;
          it = pb->waiters.erase(it);
          wq->push_back(*fi);
          auto prev = num_entries_.fetch_sub(1, memory_order_relaxed);
          DCHECK_GT(prev, 0u);
        }
        break;
      }
    }
  }
  qsbr_checkpoint();
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
#endif

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

DispatcherImpl* MakeDispatcher(Scheduler* sched) {
  ctx::fixedsize_stack salloc;
  ctx::stack_context sctx = salloc.allocate();
  ctx::preallocated palloc = MakePreallocated<DispatcherImpl>(sctx);

  void* sp_ptr = palloc.sp;

  // placement new of context on top of fiber's stack
  return new (sp_ptr) DispatcherImpl{std::move(palloc), std::move(salloc), sched};
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

  // auto& fb_init = detail::FbInitializer();
  if (scheduler_->policy()) {
    scheduler_->policy()->Run(scheduler_);
  } else {
    DefaultDispatch(scheduler_);
  }

  DVLOG(1) << "Dispatcher exiting, switching to main_cntx";
  is_terminating_ = true;

  // Like with worker fibers, we switch to another fiber, but in this case to the main fiber.
  // We will come back here during the deallocation of DispatcherImpl from intrusive_ptr_release
  // in order to return from Run() and come back to main context.
  auto fc = scheduler_->main_context()->SwitchTo();

  DCHECK(fc);  // Should bring us back to main, into intrusive_ptr_release.
  return fc;
}

void DispatcherImpl::DefaultDispatch(Scheduler* sched) {
  DCHECK(FiberActive() == this);

  while (true) {
    if (sched->IsShutdown()) {
      if (sched->num_worker_fibers() == 0)
        break;
    }

    sched->ProcessRemoteReady(nullptr);
    if (sched->HasSleepingFibers()) {
      sched->ProcessSleep();
    }

    if (sched->HasReady()) {
      FiberInterface* fi = sched->PopReady();
      DCHECK(!fi->list_hook.is_linked());
      DCHECK(!fi->sleep_hook.is_linked());
      sched->AddReady(this);

      DVLOG(2) << "Switching to " << fi->name();

      fi->SwitchTo();
      DCHECK(!list_hook.is_linked());
      DCHECK(FiberActive() == this);
    } else {
      sched->DestroyTerminated();

      bool has_sleeping = sched->HasSleepingFibers();
      auto cb = [this]() { return wake_suspend_; };

      unique_lock<mutex> lk{mu_};
      if (has_sleeping) {
        auto next_tp = sched->NextSleepPoint();
        cnd_.wait_until(lk, next_tp, std::move(cb));
      } else {
        cnd_.wait(lk, std::move(cb));
      }
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

  fibers_.push_back(*main_cntx);
  fibers_.push_back(*dispatch_cntx_);
}

Scheduler::~Scheduler() {
  shutdown_ = true;
  DCHECK(main_cntx_ == FiberActive());

  while (HasReady()) {
    FiberInterface* fi = PopReady();
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

  fibers_.erase(fibers_.iterator_to(*dispatch_cntx_));
  fibers_.erase(fibers_.iterator_to(*main_cntx_));

  // destroys the stack and the object via intrusive_ptr_release.
  dispatch_cntx_.reset();
  DestroyTerminated();
}

ctx::fiber_context Scheduler::Preempt() {
  DCHECK(FiberActive() != dispatch_cntx_.get()) << "Should not preempt dispatcher";
  DCHECK(!IsFiberAtomicSection()) << "Preempting inside of atomic section";

  if (ready_queue_.empty()) {
    // All user fibers are inactive, we should switch back to the dispatcher.
    return dispatch_cntx_->SwitchTo();
  }

  DCHECK(!ready_queue_.empty());
  FiberInterface* fi = &ready_queue_.front();
  ready_queue_.pop_front();

  return fi->SwitchTo();
}

void Scheduler::AddReady(FiberInterface* fibi) {
  DCHECK(!fibi->list_hook.is_linked());
  DVLOG(1) << "Adding " << fibi->name() << " to ready_queue_";

  fibi->cpu_tsc_ = CycleClock::Now();
  ready_queue_.push_back(*fibi);

  // Case of notifications coming to a sleeping fiber.
  if (fibi->sleep_hook.is_linked()) {
    sleep_queue_.erase(sleep_queue_.iterator_to(*fibi));
  }
}

void Scheduler::ScheduleFromRemote(FiberInterface* cntx) {
  // This function is called from FiberInterface::ActivateOther from a remote scheduler.
  // But the fiber belongs to this scheduler.
  DCHECK(cntx->scheduler_ == this);

  // If someone else holds the bit - give up on scheduling by this call.
  // This should not happen as ScheduleFromRemote should be called under a WaitQueue lock.
  if ((cntx->flags_.fetch_or(FiberInterface::kScheduleRemote, memory_order_acquire) &
       FiberInterface::kScheduleRemote) == 1) {
    LOG(DFATAL) << "Already scheduled remotely " << cntx->name();
    return;
  }

  if (cntx->IsScheduledRemotely()) {
    // We schedule a fiber remotely only once.
    // This should not happen in general, because we usually schedule a fiber under
    // a spinlock when pulling it from the WaitQueue. However, there are ActivateOther calls
    // that happen due to I/O events that might break this assumption. To see if this happens,
    // I log the case and will investigate if it happens.
    LOG(ERROR) << "Fiber " << cntx->name() << " is already scheduled remotely";

    // revert the flags.
    cntx->flags_.fetch_and(~FiberInterface::kScheduleRemote, memory_order_release);
  } else {
    remote_ready_queue_.Push(cntx);

    // clear the bit after we pushed to the queue.
    cntx->flags_.fetch_and(~FiberInterface::kScheduleRemote, memory_order_release);

    DVLOG(1) << "ScheduleFromRemote " << cntx->name() << " " << cntx->use_count_.load();

    if (custom_policy_) {
      custom_policy_->Notify();
    } else {
      DispatcherImpl* dimpl = static_cast<DispatcherImpl*>(dispatch_cntx_.get());
      dimpl->Notify();
    }
  }
}

void Scheduler::Attach(FiberInterface* cntx) {
  cntx->scheduler_ = this;

  fibers_.push_back(*cntx);

  if (cntx->type() == FiberInterface::WORKER) {
    ++num_worker_fibers_;
  }
}

void Scheduler::DetachWorker(FiberInterface* cntx) {
  fibers_.erase(fibers_.iterator_to(*cntx));
  --num_worker_fibers_;
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

    fibers_.erase(fibers_.iterator_to(*tfi));

    // maybe someone holds a Fiber handle and waits for the fiber to join.
    intrusive_ptr_release(tfi);
  }
}

bool Scheduler::WaitUntil(chrono::steady_clock::time_point tp, FiberInterface* me) {
  DCHECK(!me->sleep_hook.is_linked());
  DCHECK(!me->list_hook.is_linked());

  me->tp_ = tp;
  sleep_queue_.insert(*me);
  auto fc = Preempt();
  DCHECK(!fc);
  bool has_timed_out = (me->tp_ == chrono::steady_clock::time_point::max());

  return has_timed_out;
}

void Scheduler::ProcessRemoteReady(FiberInterface* active) {
  while (true) {
    FiberInterface* fi = remote_ready_queue_.Pop();
    if (!fi)
      break;

    // Marks as free.
    fi->remote_next_.store((FiberInterface*)FiberInterface::kRemoteFree, memory_order_relaxed);

    DVLOG(1) << "Pulled " << fi->name() << " " << fi->DEBUG_use_count();

    DCHECK(fi->scheduler_ == this);

    // Remote thread can add the same fiber exactly once to the remote_ready_queue.
    // This is why each fiber is removed from its waitqueue and added to the remote queue
    // under the same lock.
    DCHECK(!fi->list_hook.is_linked());

    // ProcessRemoteReady can be called by a FiberInterface::PullMyselfFromRemoteReadyQueue
    // i.e. it is already active. In that case we should not add it to the ready queue.
    if (fi != active && !fi->list_hook.is_linked()) {
      DVLOG(2) << "set ready " << fi->name();
      AddReady(fi);
    }
  }
}

void Scheduler::ProcessSleep() {
  DCHECK(!sleep_queue_.empty());
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  DVLOG(3) << "now " << now.time_since_epoch().count();

  do {
    auto it = sleep_queue_.begin();
    if (it->tp_ > now)
      break;

    FiberInterface& fi = *it;
    sleep_queue_.erase(it);

    DCHECK(!fi.list_hook.is_linked());
    DVLOG(2) << "timeout for " << fi.name();
    fi.tp_ = chrono::steady_clock::time_point::max();  // meaning it has timed out.
    fi.cpu_tsc_ = CycleClock::Now();
    ready_queue_.push_back(fi);
  } while (!sleep_queue_.empty());
}

void Scheduler::AttachCustomPolicy(DispatchPolicy* policy) {
  CHECK(custom_policy_ == nullptr);
  custom_policy_ = policy;
}

void Scheduler::RunDeferred() {
#if PARKING_ENABLED
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
#endif
}

void Scheduler::ExecuteOnAllFiberStacks(FiberInterface::PrintFn fn) {
  for (auto& fiber : fibers_) {
    DCHECK(fiber.scheduler() == this);
    fiber.ExecuteOnFiberStack(fn);
  }
}

void Scheduler::PrintAllFiberStackTraces() {
  auto* active = FiberActive();
  if (!sleep_queue_.empty()) {
    LOG(INFO) << "Sleep queue size " << sleep_queue_.size();
  }
  auto print_fn = [active](FiberInterface* fb) {
    string state = "suspended";
    if (fb->list_hook.is_linked()) {
      state = "ready";
    } else if (active == fb) {
      state = "active";
    } else if (fb->sleep_hook.is_linked()) {
      state = absl::StrCat("sleeping until ", fb->tp_.time_since_epoch().count(), " now is ",
                           chrono::steady_clock::now().time_since_epoch().count());
    }

    LOG(INFO) << "------------ Fiber " << fb->name_ << " (" << state << ") ------------\n"
              << GetStacktrace();
  };

  ExecuteOnAllFiberStacks(print_fn);
}

}  // namespace detail

DispatchPolicy::~DispatchPolicy() {
}

}  // namespace fb2
}  // namespace util

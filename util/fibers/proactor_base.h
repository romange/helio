// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <pthread.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Waddress"
#include "base/function2.hpp"
#pragma GCC diagnostic pop

#include <absl/container/flat_hash_map.h>

#include <functional>

#include "base/mpmc_bounded_queue.h"
#include "util/fibers/detail/result_mover.h"
#include "util/fibers/synchronization.h"
#include "util/fibers/fibers.h"
#include "util/fiber_socket_base.h"

namespace util {
class LinuxSocketBase;

namespace fb2 {

// A proxy class that binds ProactorBase to fibers scheduler.
class ProactorDispatcher;

class ProactorBase  {
  ProactorBase(const ProactorBase&) = delete;
  void operator=(const ProactorBase&) = delete;
  friend class ProactorDispatcher;

 public:
  enum { kTaskQueueLen = 256 };

  enum Kind { EPOLL = 1, IOURING = 2 };
  enum EpollFlags { EPOLL_IN = 1, EPOLL_OUT = 4 };

  // Corresponds to level 0.
  // Idle tasks will rest at least kIdleCycleMaxMicros / (2^level) time between runs.
  static const uint32_t kIdleCycleMaxMicros = 1000000u;

  // The "hottest" task can have this maxlevel.
  // In that case, proactor will always spin calling that task until it will cool down.
  static const uint32_t kOnIdleMaxLevel = 21;

  ProactorBase();
  virtual ~ProactorBase();

  // Runs the poll-loop. Stalls the calling thread which will become the "Proactor" thread.
  void Run();

  //! Signals proactor to stop. Does not wait for it.
  void Stop();

  //! Creates a socket that can be used with this proactor.
  virtual FiberSocketBase* CreateSocket(int fd = -1) = 0;

  /**
   * @brief Returns true if the called is running in this Proactor thread.
   *
   * @return true
   * @return false
   */
  bool InMyThread() const {
    return pthread_self() == thread_id_;
  }

  // pthread id.
  auto thread_id() const {
    return thread_id_;
  }

  static bool IsProactorThread() {
    return tl_info_.owner != nullptr;
  }

  // Returns thread-local instance of proactor in this thread.
  // Returns null if no Proactor is registered in this thread.
  static ProactorBase* me() {
    return tl_info_.owner;
  }

  void RegisterSignal(std::initializer_list<uint16_t> l, std::function<void(int)> cb);

  void ClearSignal(std::initializer_list<uint16_t> l) {
    RegisterSignal(l, nullptr);
  }

  // Returns an approximate (cached) time with nano-sec granularity.
  // The caller must run in the same thread as the proactor.
  static uint64_t GetMonotonicTimeNs() {
    return tl_info_.monotonic_time;
  }

  // Returns an 0 <= index < N, where N is the number of proactor threads in the pool of called
  // from Proactor thread. Returns -1 if called from some other thread.
  static int32_t GetIndex() {
    return tl_info_.proactor_index;
  }

  // Internal, used by ProactorPool
  static void SetIndex(uint32_t index) {
    tl_info_.proactor_index = index;
  }

  bool IsTaskQueueFull() const {
    return task_queue_.is_full();
  }

  //! Fire and forget - does not wait for the function to run called.
  //! `f` should not block, lock on mutexes or Await.
  //! Might block the calling fiber if the queue is full.
  template <typename Func> bool DispatchBrief(Func&& brief);

  //! Similarly to DispatchBrief but 'f' is wrapped in fiber.
  //! f is allowed to fiber-block or await.
  template <typename Func, typename... Args> void Dispatch(Func&& f, Args&&... args) {
    // Ideally we want to forward args into lambda but it's too complicated before C++20.
    // So I just copy them into capture.
    // We forward captured variables so we need lambda to be mutable.
    DispatchBrief([f = std::forward<Func>(f), args...]() mutable {
      Fiber("Dispatched", std::forward<Func>(f), std::forward<Args>(args)...).Detach();
    });
  }

  //! Similarly to DispatchBrief but waits 'f' to return.
  template <typename Func> auto AwaitBrief(Func&& brief) -> decltype(brief());

  // Runs possibly awating function 'f' safely in Proactor thread and waits for it to finish,
  // If we are in his thread already, runs 'f' directly, otherwise
  // runs it wrapped in a fiber. Should be used instead of 'AwaitBrief' when 'f' itself
  // awaits on something.
  // To summarize: 'f' may not block its thread, but allowed to block its fiber.
  template <typename Func> auto Await(Func&& f) -> decltype(f());

  // Please note that this function uses Await, therefore can not be used inside
  // Proactor main fiber (i.e. Async callbacks).
  template <typename... Args> Fiber LaunchFiber(Args&&... args) {
    Fiber fb;

    // It's safe to use & capture since we await before returning.
    AwaitBrief([&] { fb = Fiber(std::forward<Args>(args)...); });
    return fb;
  }

  using OnIdleTask = std::function<uint32_t()>;
  using PeriodicTask = std::function<void()>;

  /**
   * @brief Adds a task that should run when Proactor loop is idle. The task should return
   *        true if keep it running or false if it finished its job.
   *
   *        Must be called from the proactor thread.
   * @tparam Func
   * @param f
   * @return uint32_t an unique ids denoting this task. Can be used for cancellation.
   */
  uint32_t AddOnIdleTask(OnIdleTask f);

  //! Must be called from the proactor thread.
  //! PeriodicTask should not block since it runs from I/O loop.
  uint32_t AddPeriodic(uint32_t ms, PeriodicTask f);

  //! Must be called from the proactor thread.
  //! Blocking until the task has been cancelled. Should not run directly from I/O loop
  //! i.e. only from Await or another fiber.
  void CancelPeriodic(uint32_t id);

  bool RemoveOnIdleTask(uint32_t id);

  // Migrates the calling fibers to the destination proactor.
  // Calling fiber must belong to this proactor.
  void Migrate(ProactorBase* dest);

  // returns seqnum in the Proactor.
  // if it equals to WAIT_SECTION_STATE it means proactor is going to block on
  // IO.
  uint32_t RequestDispatcher() {
    return tq_seq_.fetch_or(1, std::memory_order_relaxed);
  }

  virtual Kind GetKind() const = 0;

 protected:
  enum { WAIT_SECTION_STATE = 1UL << 31 };
  static constexpr unsigned kMaxSpinLimit = 5;

  struct PeriodicItem {
    PeriodicTask task;

    // We must keep it as timespec because io-uring accesses timespec asynchronously
    // after the submition.
    timespec period;

    uint32_t val1;  // implementation dependent payload.
    uint32_t val2;  // implementation dependent payload.
    bool in_map = true;
  };

  // Called only from external threads.
  virtual void WakeRing() = 0;
  virtual void MainLoop(detail::Scheduler* sched) = 0;

  void WakeupIfNeeded();

  virtual void SchedulePeriodic(uint32_t id, PeriodicItem* item) = 0;
  virtual void CancelPeriodicInternal(uint32_t val1, uint32_t val2) = 0;

  // Returns true if we should continue spinning or false otherwise.
  bool RunOnIdleTasks();

  static void Pause(unsigned strength);
  static void ModuleInit();

  static uint64_t GetClockNanos() {
    // absl::GetCurrentTimeNanos() might be non-monotonic and sync with the system clock.
    // but it's much more efficient than clock_gettime.
    return absl::GetCurrentTimeNanos();
  }

  pthread_t thread_id_ = 0U;
  int wake_fd_ = -1;
  bool is_stopped_ = true;

  std::atomic_uint32_t tq_seq_{0}, tq_full_ev_{0};
  std::atomic_uint32_t tq_wakeup_ev_{0}, tq_wakeup_save_ev_{0};

  // We use fu2 function to allow moveable semantics.
  using Fu2Fun =
      fu2::function_base<true /*owns*/, false /*non-copyable*/, fu2::capacity_fixed<16, 8>,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/, void()>;
  struct Tasklet : public Fu2Fun {
    using Fu2Fun::Fu2Fun;
    using Fu2Fun::operator=;
  };
  static_assert(sizeof(Tasklet) == 32, "");

  using FuncQ = base::mpmc_bounded_queue<Tasklet>;

  FuncQ task_queue_;
  EventCount task_queue_avail_;

  uint32_t next_task_id_{1};

  // Runs tasks when there is available cpu time and no I/O events demand it.
  struct OnIdleWrapper {
    OnIdleTask task;
    uint64_t next_ts;  // when to run the next time in nano seconds.
  };

  std::vector<OnIdleWrapper> on_idle_arr_;
  uint32_t on_idle_next_ = 0;

  absl::flat_hash_map<uint32_t, PeriodicItem*> periodic_map_;

  struct TLInfo {
    int32_t proactor_index = -1;
    uint64_t monotonic_time = 0;  // in nanoseconds
    ProactorBase* owner = nullptr;
  };
  static __thread TLInfo tl_info_;

 private:
  template <typename Func> bool EmplaceTaskQueue(Func&& f) {
    if (task_queue_.try_enqueue(std::forward<Func>(f))) {
      WakeupIfNeeded();

      return true;
    }
    return false;
  }
};

class ProactorDispatcher : public DispatchPolicy {
public:
  explicit ProactorDispatcher(ProactorBase* proactor) : proactor_(proactor) {
  }

private:
  void Run(detail::Scheduler* sched);
  void Notify() final;

  ProactorBase* proactor_;
};


// Implementation
// **********************************************************************
//

inline void ProactorBase::WakeupIfNeeded() {
  auto current = tq_seq_.fetch_add(2, std::memory_order_relaxed);
  if (current == WAIT_SECTION_STATE) {
    // We protect WakeRing using tq_seq_. That means only one thread at a time
    // can enter here. Moreover tq_seq_ == WAIT_SECTION_STATE only when
    // proactor enters WAIT section, therefore we do not race over SQE ring with proactor thread.
    std::atomic_thread_fence(std::memory_order_acquire);
    WakeRing();
  } else {
    tq_wakeup_save_ev_.fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename Func> bool ProactorBase::DispatchBrief(Func&& f) {
  if (EmplaceTaskQueue(std::forward<Func>(f)))
    return false;

  tq_full_ev_.fetch_add(1, std::memory_order_relaxed);

  // If EventCount::Wait appears on profiler radar, it's most likely because the task queue is
  // too overloaded. It's either the CPU is overloaded or we are sending too many tasks through it.
  while (true) {
    EventCount::Key key = task_queue_avail_.prepareWait();

    if (EmplaceTaskQueue(std::forward<Func>(f))) {
      break;
    }
    task_queue_avail_.wait(key.epoch());
  }

  return true;
}

template <typename Func> auto ProactorBase::AwaitBrief(Func&& f) -> decltype(f()) {
  if (InMyThread()) {
    return f();
  }

  if (IsProactorThread()) {
    // TODO:
  }

  // auto* active = detail::FiberActive();
  using ResultType = decltype(f());
  util::detail::ResultMover<ResultType> mover;
  Done done;


  // active->StartParking();

  // Store done-ptr by value to increase the refcount while lambda is running.
  DispatchBrief([&mover, f = std::forward<Func>(f), done]() mutable {
    mover.Apply(f);
    // detail::FiberActive()->NotifyParked(active);
    done.Notify();
  });

  // active->SuspendUntilWakeup();
  done.Wait();

  return std::move(mover).get();
}

// Runs possibly awating function 'f' safely in ContextThread and waits for it to finish,
// If we are in the context thread already, runs 'f' directly, otherwise
// runs it wrapped in a fiber. Should be used instead of 'Await' when 'f' itself
// awaits on something.
// To summarize: 'f' should not block its thread, but allowed to block its fiber.
template <typename Func> auto ProactorBase::Await(Func&& f) -> decltype(f()) {
  if (InMyThread()) {
    return f();
  }

  using ResultType = decltype(f());
  util::detail::ResultMover<ResultType> mover;
  auto fb = LaunchFiber([&] { mover.Apply(std::forward<Func>(f)); });
  fb.Join();

  return std::move(mover).get();
}

}  // namespace fb2

// TODO:
using fb2::ProactorBase;

namespace detail {

// GLIBC/MUSL has 2 flavors of strerror_r.
// this wrappers work around these incompatibilities.
inline char const* strerror_r_helper(char const* r, char const*) noexcept {
  return r;
}

inline char const* strerror_r_helper(int r, char const* buffer) noexcept {
  return r == 0 ? buffer : "Unknown error";
}

inline std::string SafeErrorMessage(int ev) noexcept {
  char buf[128];

  return strerror_r_helper(strerror_r(ev, buf, sizeof(buf)), buf);
}

}  // namespace detail
}  // namespace util

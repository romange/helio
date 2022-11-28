// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/proactor_base.h"

#include <absl/base/attributes.h>
#include <signal.h>
#include <sys/eventfd.h>

#include "base/logging.h"

using namespace boost;
namespace ctx = boost::context;
using namespace std;

namespace util {

namespace {

struct signal_state {
  struct Item {
    ProactorBase* proactor = nullptr;
    std::function<void(int)> cb;
  };

  Item signal_map[_NSIG];
};

signal_state* get_signal_state() {
  static signal_state state;

  return &state;
}

void SigAction(int signal, siginfo_t*, void*) {
  signal_state* state = get_signal_state();
  DCHECK_LT(signal, _NSIG);

  auto& item = state->signal_map[signal];
  auto cb = [signal, &item] { item.cb(signal); };

  if (item.proactor && item.cb) {
    item.proactor->Dispatch(std::move(cb));
  } else {
    LOG(ERROR) << "Tangling signal handler " << signal;
  }
}

unsigned pause_amplifier = 50;
std::once_flag module_init;

}  // namespace

thread_local ProactorBase::TLInfo ProactorBase::tl_info_;

ProactorBase::ProactorBase() : task_queue_(512) {
  call_once(module_init, &ModuleInit);

  wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK_GE(wake_fd_, 0);
  VLOG(1) << "Created wake_fd is " << wake_fd_;

  volatile ctx::fiber dummy;  // For some weird reason I need this to pull
                              // boost::context into linkage.
}

ProactorBase::~ProactorBase() {
  close(wake_fd_);

  signal_state* ss = get_signal_state();
  for (size_t i = 0; i < ABSL_ARRAYSIZE(ss->signal_map); ++i) {
    if (ss->signal_map[i].proactor == this) {
      ss->signal_map[i].proactor = nullptr;
      ss->signal_map[i].cb = nullptr;
    }
  }
}

void ProactorBase::Stop() {
  DispatchBrief([this] { is_stopped_ = true; });
  VLOG(1) << "Proactor::StopFinish";
}

uint32_t ProactorBase::AddOnIdleTask(OnIdleTask f) {
  DCHECK(InMyThread());

  uint32_t res = on_idle_arr_.size();
  on_idle_arr_.push_back(OnIdleWrapper{.task = std::move(f), .next_ts = 0});

  return res;
}

bool ProactorBase::RunOnIdleTasks() {
  if (on_idle_arr_.empty())
    return false;

  uint64_t start = GetClockNanos();
  uint64_t curr_ts = start;

  bool should_spin = false;

  DCHECK_LT(on_idle_next_, on_idle_arr_.size());

  // Perform round robin with on_idle_next_ saving the position between runs.
  do {
    OnIdleWrapper& on_idle = on_idle_arr_[on_idle_next_];

    if (on_idle.task && on_idle.next_ts <= curr_ts) {
      tl_info_.monotonic_time = curr_ts;

      uint32_t level = on_idle.task();  // run the task

      curr_ts = GetClockNanos();

      if (level >= kOnIdleMaxLevel) {
        level = kOnIdleMaxLevel;
        should_spin = true;
      } else {
        uint64_t delta_ns = uint64_t(kIdleCycleMaxMicros) * 1000 / (1 << level);
        on_idle.next_ts = curr_ts + delta_ns;
      }
    }

    ++on_idle_next_;
    if (on_idle_next_ == on_idle_arr_.size()) {
      on_idle_next_ = 0;
      break;
    }
  } while (curr_ts < start + 10000);  // 10usec for the run.

  return should_spin;
}

bool ProactorBase::RemoveOnIdleTask(uint32_t id) {
  if (id >= on_idle_arr_.size() || !on_idle_arr_[id].task)
    return false;

  on_idle_arr_[id].task = OnIdleTask{};

  return true;
}

uint32_t ProactorBase::AddPeriodic(uint32_t ms, PeriodicTask f) {
  DCHECK(InMyThread());

  auto id = next_task_id_++;

  PeriodicItem* item = new PeriodicItem;
  item->task = std::move(f);
  item->period.tv_sec = ms / 1000;
  item->period.tv_nsec = (ms % 1000) * 1000000;

  auto [it, inserted] = periodic_map_.emplace(id, item);
  CHECK(inserted);

  SchedulePeriodic(id, item);

  return id;
}

void ProactorBase::CancelPeriodic(uint32_t id) {
  DCHECK(InMyThread());

  auto it = periodic_map_.find(id);
  CHECK(it != periodic_map_.end());
  uint32_t val1 = it->second->val1;
  uint32_t val2 = it->second->val2;
  it->second->in_map = false;

  // we never deallocate here since there is a callback that holds pointer to the item.
  periodic_map_.erase(it);
  CancelPeriodicInternal(val1, val2);
}

void ProactorBase::Migrate(ProactorBase* dest) {
  CHECK(dest != this);

  fibers::context* me = fibers::context::active();
  fibers::fiber fb2 = LaunchFiber([&] {
    me->detach();
    VLOG(1) << "After me detach";
    dest->AwaitBrief([me] {
      fibers::context::active()->attach(me);
      VLOG(1) << "After me attach";
    });
    VLOG(1) << "After Migrate/AwaitBrief";
  });
  fb2.join();
}

void ProactorBase::RegisterSignal(std::initializer_list<uint16_t> l, std::function<void(int)> cb) {
  auto* state = get_signal_state();

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));

  if (cb) {
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = &SigAction;

    for (uint16_t val : l) {
      CHECK(!state->signal_map[val].cb) << "Signal " << val << " was already registered";
      state->signal_map[val].cb = cb;
      state->signal_map[val].proactor = this;

      CHECK_EQ(0, sigaction(val, &sa, NULL));
    }
  } else {
    sa.sa_handler = SIG_DFL;

    for (uint16_t val : l) {
      CHECK(state->signal_map[val].cb) << "Signal " << val << " was already registered";
      state->signal_map[val].cb = nullptr;
      state->signal_map[val].proactor = nullptr;

      CHECK_EQ(0, sigaction(val, &sa, NULL));
    }
  }
}

// Remember, WakeRing is called from external threads.
void ProactorBase::WakeRing() {
  DVLOG(2) << "Wake ring " << tq_seq_.load(memory_order_relaxed);

  tq_wakeup_ev_.fetch_add(1, memory_order_relaxed);

  /**
   * It's tempting to use io_uring_prep_nop() here in order to resume wait_cqe() call.
   * However, it's not that staightforward. io_uring_get_sqe and io_uring_submit
   * are not thread-safe and this function is called from another thread.
   * Even though tq_seq_ == WAIT_SECTION_STATE ensured that Proactor thread
   * is going to stall we can not guarantee that it will not wake up before we reach the next line.
   * In that case, Proactor loop will continue and both threads could call
   * io_uring_get_sqe and io_uring_submit at the same time. This will cause data-races.
   * It's possible to fix this by guarding with spinlock the section below as well as
   * the section after the wait_cqe() call but I think it's overcomplicated and not worth it.
   * Therefore we gonna stick with event_fd descriptor to wake up Proactor thread.
   */

  uint64_t val = 1;
  CHECK_EQ(8, write(wake_fd_, &val, sizeof(uint64_t)));
}

void ProactorBase::Pause(unsigned count) {
  auto pc = pause_amplifier;

  for (unsigned i = 0; i < count * pc; ++i) {
#if defined(__i386__) || defined(__amd64__)
    __asm__ __volatile__("pause");
#elif defined(__aarch64__)
      /* Use an isb here as we've found it's much closer in duration to
      * the x86 pause instruction vs. yield which is a nop and thus the
      * loop count is lower and the interconnect gets a lot more traffic
      * from loading the ticket above. */
    __asm__ __volatile__ ("isb");
#endif
  }
}

void ProactorBase::ModuleInit() {
  uint64_t delta;
  while (true) {
    uint64_t now = GetClockNanos();
    for (unsigned i = 0; i < 10; ++i) {
      Pause(kMaxSpinLimit);
    }
    delta = GetClockNanos() - now;
    VLOG(1) << "Running 10 Pause() took " << delta / 1000 << "us";

    if (delta < 2000 || pause_amplifier == 1)  // 2us
      break;
    pause_amplifier -= (pause_amplifier + 7) / 8;
  };
}

}  // namespace util

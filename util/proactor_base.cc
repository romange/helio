// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/proactor_base.h"

#include <absl/base/attributes.h>
#include <signal.h>
#include <sys/eventfd.h>

#include "base/logging.h"

using namespace boost;
namespace ctx = boost::context;

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
    item.proactor->AsyncFiber(std::move(cb));
  } else {
    LOG(ERROR) << "Tangling signal handler " << signal;
  }
}

}  // namespace

thread_local ProactorBase::TLInfo ProactorBase::tl_info_;

ProactorBase::ProactorBase() : task_queue_(512) {
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
  AsyncBrief([this] { is_stopped_ = true; });
  VLOG(1) << "Proactor::StopFinish";
}

uint32_t ProactorBase::AddIdleTask(IdleTask f) {
  auto id = next_task_id_++;
  auto res = idle_map_.emplace(id, std::move(f));
  CHECK(res.second);
  return id;
}

uint32_t ProactorBase::AddPeriodic(uint32_t ms, PeriodicTask f) {
  auto id = next_task_id_++;

  std::shared_ptr<PeriodicItem> item = std::make_shared<PeriodicItem>();
  item->task = std::move(f);
  item->ts.tv_sec = ms / 1000;
  item->ts.tv_nsec = (ms % 1000) * 1000000;

  auto res = periodic_map_.emplace(id, std::move(item));
  CHECK(res.second);

  SchedulePeriodic(id, res.first->second);
  return id;
}

void ProactorBase::CancelPeriodic(uint32_t id) {
  auto it = periodic_map_.find(id);
  CHECK(it != periodic_map_.end());
  auto item = std::move(it->second);
  periodic_map_.erase(it);
  CancelPeriodicInternal(std::move(item));
}

void ProactorBase::Migrate(ProactorBase* dest) {
  CHECK(dest != this);

  fibers::context* me = fibers::context::active();
  fibers::fiber fb2 = LaunchFiber([&] {
    me->detach();
    dest->AwaitBrief([me] { fibers::context::active()->attach(me); });
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
  DVLOG(2) << "Wake ring " << tq_seq_.load(std::memory_order_relaxed);

  tq_wakeup_ev_.fetch_add(1, std::memory_order_relaxed);

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

}  // namespace util

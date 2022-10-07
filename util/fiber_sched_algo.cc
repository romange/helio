// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/fiber_sched_algo.h"

#include <poll.h>
#include <sys/timerfd.h>

#include "base/logging.h"
#include "util/proactor_base.h"

// TODO: We should replace DVLOG macros with RAW_VLOG if we do glog sync integration.

namespace util {
using namespace boost;
using namespace std;
using chrono::nanoseconds;
using chrono::time_point_cast;

FiberSchedAlgo::FiberSchedAlgo(ProactorBase* proactor) : proactor_(proactor) {
  main_cntx_ = fibers::context::active();
  CHECK(main_cntx_->is_context(fibers::type::main_context));
  timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  CHECK_GE(timer_fd_, 0);
  prev_picked_ = nullptr;

  VLOG(1) << "ioloop is " << fibers_ext::short_id(main_cntx_);
}

FiberSchedAlgo::~FiberSchedAlgo() {
  close(timer_fd_);
}

void FiberSchedAlgo::awakened(FiberContext* ctx, FiberProps& props) noexcept {
  DCHECK(!ctx->ready_is_linked()) << props.name();

  if (ctx->is_context(fibers::type::dispatcher_context)) {
    DVLOG(1) << "Awakened dispatch";
  } else {
    DVLOG(1) << "Awakened " << props.name();

    ++ready_cnt_;  // increase the number of awakened/ready fibers.

    uint64_t now = ProactorBase::GetClockNanos();
    props.awaken_ts_ = now;

    if (ctx != main_cntx_ && MainHasSwitched() && !main_cntx_->ready_is_linked()) {
      uint64_t delta_us = (now - suspend_main_ts_) / 1000;
      if (delta_us > 1000) {  // 1ms
        DVLOG(2) << "Preemptively awakened io_loop after " << delta_us << " usec";
        ++ready_cnt_;
        main_cntx_->ready_link(rqueue_);
        FiberProps* main_props = static_cast<FiberProps*>(main_cntx_->get_properties());
        main_props->awaken_ts_ = now;
        flags_.ioloop_yielded = 0;
        flags_.ioloop_woke = 1;
      }
    }
  }

  ctx->ready_link(rqueue_); /*< fiber, enqueue on ready queue >*/
}

auto FiberSchedAlgo::pick_next() noexcept -> FiberContext* {
  DVLOG(2) << "pick_next: " << ready_cnt_ << "/" << rqueue_.size();

  if (rqueue_.empty()) {
    prev_picked_ = nullptr;
    return nullptr;
  }

  FiberContext* ctx;

  // simplest 2-level priority queue.
  // choose main context first
  if (flags_.ioloop_woke && main_cntx_->ready_is_linked()) {
    ctx = main_cntx_;
    ctx->ready_unlink();
    flags_.ioloop_woke = 0;
  } else {
    ctx = &rqueue_.front();
    rqueue_.pop_front();

    if (flags_.ioloop_suspended) {
      flags_.ioloop_yielded = 1;
    }
  }

  uint64_t now = ProactorBase::GetClockNanos();

  if (ctx->is_context(boost::fibers::type::dispatcher_context)) {
    DVLOG(2) << "Switching to dispatch "
             << fibers_ext::short_id(ctx);  // TODO: to switch to RAW_LOG.
  } else {
    --ready_cnt_;
    FiberProps* props = (FiberProps*)ctx->get_properties();

    DVLOG(2) << "Switching to " << fibers_ext::short_id(ctx) << ":"
             << props->name();  // TODO: to switch to RAW_LOG.

    uint64_t last_dur_usec = prev_pick_ts_ ? (now - prev_pick_ts_) / 1000 : 0;
    if (last_dur_usec > 3000) {
      VLOG(1) << "Execution of " << fibers_ext::short_id(prev_picked_) << " took too long "
              << last_dur_usec << " usec: " << props->name();
    }

    props->resume_ts_ = now;
    ProactorBase::tl_info_.monotonic_time = now;
    uint64_t delta_micros = (now - props->awaken_ts_) / 1000;
    if (delta_micros > 30000) {
      LOG(INFO) << "Took " << delta_micros / 1000 << " ms since it woke and till became active "
                << fibers_ext::short_id(ctx) << ":" << props->name();
    }
  }

  prev_picked_ = ctx;
  prev_pick_ts_ = now;
  return ctx;
}

void FiberSchedAlgo::property_change(FiberContext* ctx, FiberProps& props) noexcept {
  if (!ctx->ready_is_linked()) {
    return;
  }

  // Found ctx: unlink it
  ctx->ready_unlink();
  if (!ctx->is_context(fibers::type::dispatcher_context)) {
    --ready_cnt_;
  }

  // Here we know that ctx was in our ready queue, but we've unlinked
  // it. We happen to have a method that will (re-)add a context* to the
  // right place in the ready queue.
  awakened(ctx, props);
}

bool FiberSchedAlgo::has_ready_fibers() const noexcept {
  return ready_cnt_ > 0;
}

// This function is called from remote threads, to wake this thread in case it's sleeping.
// In our case, "sleeping" means - might stuck the wait function waiting for completion events.
// wait_for_cqe is the only place where the thread can be stalled.
void FiberSchedAlgo::notify() noexcept {
  DVLOG(2) << "notify from thread";  // thread id is in the log.

  // We signal so that
  // 1. Main context should awake if it is not
  // 2. it needs to yield to dispatch context that will put active fibers into
  // ready queue.
  uint32_t seqnum = proactor_->RequestDispatcher();
  if (seqnum == ProactorBase::WAIT_SECTION_STATE) {
    ProactorBase* from = ProactorBase::me();
    if (from)
      from->algo_notify_cnt_.fetch_add(1, memory_order_relaxed);
    proactor_->WakeRing();
  }
}

// suspend_until halts the thread in case there are no active fibers to run on it.
// This function is called by dispatcher fiber.
void FiberSchedAlgo::suspend_until(time_point const& abs_time) noexcept {
  FiberContext* cur_cntx = fibers::context::active();

  DCHECK(cur_cntx->is_context(fibers::type::dispatcher_context));
  CHECK(flags_.ioloop_suspended) << "Deadlock is detected";
  DVLOG(2) << "suspend_until abs_time "
           << time_point_cast<nanoseconds>(abs_time).time_since_epoch().count();

  flags_.suspenduntil_called = 1;

  if (time_point::max() != abs_time) {
    SuspendWithTimer(abs_time);
  }

  // schedule does not block just awakens main_cntx_.
  main_cntx_->get_scheduler()->schedule(main_cntx_);
  prev_pick_ts_ = 0;
}

bool FiberSchedAlgo::SuspendIoLoop(uint64_t now) {
  // block this fiber till all (ready) fibers are processed
  // or when  AsioScheduler::suspend_until() has been called or awaken() decided to resume it.
  flags_.ioloop_suspended = 1;
  flags_.suspenduntil_called = 0;

  DVLOG(2) << "WaitTillFibersSuspend:Start";
  suspend_main_ts_ = now;

  main_cntx_->suspend();
  flags_.ioloop_suspended = 0;
  flags_.ioloop_yielded = 0;
  flags_.ioloop_woke = 0;

  DVLOG(2) << "WaitTillFibersSuspend:End " << int(flags_.suspenduntil_called);

  return flags_.suspenduntil_called;
}

}  // namespace util

// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/fiber/scheduler.hpp>
#include <string>

namespace util {

class ProactorBase;
class FiberSchedAlgo;

class FiberProps : public ::boost::fibers::fiber_properties {
  friend class FiberSchedAlgo;

 public:
  FiberProps(::boost::fibers::context* ctx) : fiber_properties(ctx) {
  }

  void set_name(std::string nm) {
    name_ = std::move(nm);
  }

  const std::string& name() const {
    return name_;
  }

  uint64_t resume_ts() const { return resume_ts_;}

 private:
  uint64_t resume_ts_ = 0, awaken_ts_ = 0;
  std::string name_;
};

class FiberSchedAlgo : public ::boost::fibers::algo::algorithm_with_properties<FiberProps> {
  using ready_queue_type = ::boost::fibers::scheduler::ready_queue_type;

 public:
  using FiberContext = ::boost::fibers::context;
  using time_point = std::chrono::steady_clock::time_point;

  FiberSchedAlgo(ProactorBase* proactor);
  virtual ~FiberSchedAlgo();

  void awakened(FiberContext* ctx, FiberProps& props) noexcept override;

  FiberContext* pick_next() noexcept override;

  void property_change(FiberContext* ctx, FiberProps& props) noexcept final;

  bool has_ready_fibers() const noexcept final;

  //! suspend_until halts the thread in case there are no active fibers to run on it.
  //! This is done by dispatcher fiber.
  void suspend_until(time_point const& abs_time) noexcept final;

  // This function is called from remote threads, to wake this thread in case it's sleeping.
  // In our case, "sleeping" means - might stuck the wait function waiting for completion events.
  void notify() noexcept final;

  // Returns true if suspend_until has been called before resuming back to ioloop.
  bool SuspendIoLoop(uint64_t now);

  uint32_t ready_cnt() const { return ready_cnt_;}

 protected:
  virtual void SuspendWithTimer(const time_point& tp) noexcept = 0;

  bool MainHasSwitched() const {
    return flags_.ioloop_suspended & flags_.ioloop_yielded;
  }

  ProactorBase* proactor_;

  ready_queue_type rqueue_;
  FiberContext* main_cntx_, *prev_picked_;
  uint64_t suspend_main_ts_ = 0, prev_pick_ts_ = 0;  // in nanos.
  uint32_t ready_cnt_ = 0;
  int timer_fd_ = -1;

  union {
    uint8_t flag_val_ = 0;
    struct {
      uint8_t ioloop_suspended: 1;  // io loop fiber is suspended
      uint8_t ioloop_yielded: 1;   // while suspended, ioloop switched to another fiber.
      uint8_t ioloop_woke: 1;
      uint8_t suspenduntil_called: 1;
    } flags_;
  };

};

}  // namespace util

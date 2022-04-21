// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include <memory_resource>
#include <string_view>

#include "base/RWSpinLock.h"
#include "base/type_traits.h"
#include "util/proactor_base.h"

namespace util {

class ProactorPool {
  template <typename Func, typename... Args>
  using AcceptArgsCheck =
      typename std::enable_if<base::is_invocable<Func, Args...>::value, int>::type;
  ProactorPool(const ProactorPool&) = delete;
  void operator=(const ProactorPool&) = delete;

 public:
  //! Constructs io_context pool with number of threads equal to 'pool_size'.
  //! pool_size = 0 chooses automatically pool size equal to number of cores in
  //! the system.
  explicit ProactorPool(std::size_t pool_size);

  virtual ~ProactorPool();

  //! Starts running all Proactor objects in the pool.
  //! Blocks until all the proactors up and spinning.
  void Run();

  /*! @brief Stops all io_context objects in the pool.
   *
   *  Waits for all the threads to finish. Requires that Run has been called.
   *  Blocks the current thread until all the pool threads exited.
   */
  void Stop();

  //! Get a Proactor to use. Thread-safe.
  ProactorBase* GetNextProactor();

  ProactorBase& operator[](size_t i) {
    return *at(i);
  }

  ProactorBase* at(size_t i) {
    return proactor_[i];
  }

  size_t size() const {
    return pool_size_;
  }

  /*! @brief Runs func in all IO threads asynchronously.
   *
   * The task must be CPU-only non IO-blocking code because it runs directly in
   * IO-fiber. DispatchBrief runs asynchronously and will exit before  the task
   * finishes. The 'func' must accept Proactor& as its argument.
   */
  template <typename Func, AcceptArgsCheck<Func, ProactorBase*> = 0>
  void DispatchBrief(Func&& func) {
    CheckRunningState();
    for (unsigned i = 0; i < size(); ++i) {
      ProactorBase* p = proactor_[i];
      // func must be copied, it can not be moved, because we dsitribute it into
      // multiple Proactors.
      p->DispatchBrief([p, func]() mutable { func(p); });
    }
  }

  /*! @brief Runs func in all IO threads asynchronously.
   *
   * The task must be CPU-only non IO-blocking code because it runs directly in
   * IO-loop. DispatchBrief runs asynchronously and will exit once func is
   * submitted but before it has finished running. The 'func' must accept
   * unsigned int (io context index) and Proactor& as its arguments.
   */
  template <typename Func, AcceptArgsCheck<Func, unsigned, ProactorBase*> = 0>
  void DispatchBrief(Func&& func) {
    CheckRunningState();
    for (unsigned i = 0; i < size(); ++i) {
      ProactorBase* p = proactor_[i];
      // Copy func on purpose, see above.
      p->DispatchBrief([p, i, func]() mutable { func(i, p); });
    }
  }

  /**
   * @brief Runs the funcion in all IO threads asynchronously.
   * Blocks until all the asynchronous calls return.
   *
   * Func must accept "ProactorBase&" and it should not block.
   */
  template <typename Func, AcceptArgsCheck<Func, ProactorBase*> = 0> void Await(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [func = std::forward<Func>(func), bc](ProactorBase* context) mutable {
      func(context);
      bc.Dec();
    };
    DispatchBrief(std::move(cb));
    bc.Wait();
  }

  /**
   * @brief Blocks until all the asynchronous calls to func return. Func
   * receives both the index and Proactor&. func must not block.
   *
   */
  template <typename Func, AcceptArgsCheck<Func, unsigned, ProactorBase*> = 0>
  void Await(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [func = std::forward<Func>(func), bc](unsigned index, ProactorBase* p) mutable {
      func(index, p);
      bc.Dec();
    };
    DispatchBrief(std::move(cb));
    bc.Wait();
  }

  /**
   * @brief Runs `func` in a fiber asynchronously. func must accept Proactor&.
   *        func may fiber-block.
   *
   * @param func
   *
   * 'func' callback runs inside a wrapping fiber.
   */
  template <typename Func, AcceptArgsCheck<Func, unsigned, ProactorBase*> = 0>
  void DispatchOnAll(Func&& func) {
    DispatchBrief([func = std::forward<Func>(func)](unsigned i, ProactorBase* context) {
      ::boost::fibers::fiber(func, i, context).detach();
    });
  }

  /**
   * @brief Runs `func` in a fiber asynchronously. func must accept Proactor&.
   *        func may fiber-block.
   *
   * @param func
   *
   * 'func' callback runs inside a wrapping fiber.
   */
  template <typename Func, AcceptArgsCheck<Func, ProactorBase*> = 0>
  void DispatchOnAll(Func&& func) {
    DispatchBrief([func = std::forward<Func>(func)](ProactorBase* context) {
      ::boost::fibers::fiber(func, context).detach();
    });
  }

  /**
   * @brief Runs `func` wrapped in fiber on all IO threads in parallel. func
   * must accept (unsigned, ProactorBase*). func may fiber-block.
   *
   * @param func
   *
   * Waits for all the callbacks to finish.
   */
  template <typename Func, AcceptArgsCheck<Func, unsigned, ProactorBase*> = 0>
  void AwaitFiberOnAll(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [func = std::forward<Func>(func), bc](unsigned i, ProactorBase* context) mutable {
      func(i, context);
      bc.Dec();
    };
    DispatchOnAll(std::move(cb));
    bc.Wait();
  }

  /**
   * @brief Runs `func` wrapped in fiber on all IO threads in parallel. func
   * must accept ProactorBase*. func may fiber-block.
   *
   * @param func
   *
   * Waits for all the callbacks to finish.
   */
  template <typename Func, AcceptArgsCheck<Func, ProactorBase*> = 0>
  void AwaitFiberOnAll(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [func = std::forward<Func>(func), bc](ProactorBase* context) mutable {
      func(context);
      bc.Dec();
    };
    DispatchOnAll(std::move(cb));
    bc.Wait();
  }

  // Returns vector of proactor thread indiced pinned to cpu_id.
  // Returns an empty vector if no threads are pinned to this cpu_id.
  std::vector<unsigned> MapCpuToThreads(unsigned cpu_id) const;

  // Auxillary functions

  // Returns a string owned by pool's global storage. Allocates only once for each new string blob.
  // Currently has average performance as it employs RW spinlock underneath.
  std::string_view GetString(std::string_view source);

 protected:
  virtual ProactorBase* CreateProactor() = 0;
  virtual void InitInThread(unsigned index) = 0;

  std::unique_ptr<ProactorBase*[]> proactor_;

 private:
  void SetupProactors();

  void WrapLoop(size_t index, fibers_ext::BlockingCounter* bc);
  void CheckRunningState();

  /// The next io_context to use for a connection.
  std::atomic_uint_fast32_t next_io_context_{0};
  uint32_t pool_size_;

  folly::RWSpinLock str_lock_;
  absl::flat_hash_set<std::string_view> str_set_;
  std::pmr::monotonic_buffer_resource str_arena_;

  enum State { STOPPED, RUN } state_ = STOPPED;

  // maps cpu_id to thread array.
  std::vector<std::vector<unsigned>> cpu_threads_;
};

}  // namespace util

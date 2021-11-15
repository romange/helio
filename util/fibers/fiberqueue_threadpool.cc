// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fibers/fiberqueue_threadpool.h"

#include "absl/strings/str_cat.h"
#include "base/pthread_utils.h"

namespace util {
namespace fibers_ext {
using namespace boost;
using namespace std;

FiberQueue::FiberQueue(unsigned queue_size) : queue_(queue_size) {
}

void FiberQueue::Run() {
  bool is_closed = false;
  CbFunc func;

  auto cb = [&] {
    if (queue_.try_dequeue(func)) {
      push_ec_.notify();
      return true;
    }

    if (is_closed_.load(std::memory_order_acquire)) {
      is_closed = true;
      return true;
    }
    return false;
  };

  while (true) {
    pull_ec_.await(cb);

    if (is_closed)
      break;
    try {
      func();
    } catch (std::exception& e) {
      // std::exception_ptr p = std::current_exception();
      LOG(FATAL) << "Exception " << e.what();
    }
  }
}

void FiberQueue::Shutdown() {
  is_closed_.store(true, memory_order_seq_cst);
  pull_ec_.notify();
}

FiberQueueThreadPool::FiberQueueThreadPool(unsigned num_threads, unsigned queue_size) {
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
  }
  worker_size_ = num_threads;
  workers_.reset(new Worker[num_threads]);

  for (unsigned i = 0; i < num_threads; ++i) {
    string name = absl::StrCat("fq_pool", i);

    auto fn = std::bind(&FiberQueueThreadPool::WorkerFunction, this, i);
    workers_[i].q.reset(new FiberQueue(queue_size));
    workers_[i].tid = base::StartThread(name.c_str(), fn);
  }
}

FiberQueueThreadPool::~FiberQueueThreadPool() {
  VLOG(1) << "FiberQueueThreadPool::~FiberQueueThreadPool";

  Shutdown();
}

void FiberQueueThreadPool::Shutdown() {
  if (!workers_)
    return;

  for (size_t i = 0; i < worker_size_; ++i) {
    workers_[i].q->is_closed_.store(true, memory_order_seq_cst);
    workers_[i].q->pull_ec_.notifyAll();
  }

  for (size_t i = 0; i < worker_size_; ++i) {
    auto& w = workers_[i];
    pthread_join(w.tid, nullptr);
  }

  workers_.reset();
  VLOG(1) << "FiberQueueThreadPool::ShutdownEnd";
}

void FiberQueueThreadPool::WorkerFunction(unsigned index) {
  /*
  sched_param param;
  param.sched_priority = 1;
  int err = pthread_setschedparam(pthread_self(), SCHED_FIFO, &param);
  if (err) {
    LOG(INFO) << "Could not set FIFO priority in fiber-queue-thread";
  }*/

  workers_[index].q->Run();

  VLOG(1) << "FiberQueueThreadPool::Exit";
}

}  // namespace fibers_ext
}  // namespace util

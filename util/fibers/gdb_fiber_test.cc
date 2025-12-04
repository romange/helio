// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
// Test program for fiber GDB debugging extensions.
// This program creates multiple fibers and can be used to test the GDB commands.
//
// To test GDB scripts manually:
//   gdb -x test_gdb_commands.gdb ./gdb_fiber_test

#include <atomic>
#include <iostream>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

using namespace std;
using namespace util;
using namespace util::fb2;

class GdbFiberTest : public testing::Test {
 public:
};

// Global barrier for synchronization
static atomic<int> barrier_count{0};
static atomic<bool> should_continue{false};

void WaitAtBarrier(int expected) {
  barrier_count.fetch_add(1);
  while (barrier_count.load() < expected) {
    ThisFiber::Yield();
  }
}

// A simple fiber function that will be visible in stack traces
void FiberWorkFunction(const string& name, int work_units) {
  LOG(INFO) << "Fiber " << name << " starting with " << work_units << " work units";

  for (int i = 0; i < work_units; ++i) {
    ThisFiber::Yield();
  }

  LOG(INFO) << "Fiber " << name << " completed";
}

// A function that creates a deep stack for testing backtrace
void DeepStackFunction(int depth, const string& name) {
  if (depth > 0) {
    DeepStackFunction(depth - 1, name);
  } else {
    LOG(INFO) << "At bottom of stack for " << name;
    ThisFiber::Yield();
  }
}

TEST_F(GdbFiberTest, MultipleFibers) {
  // This test creates multiple fibers that can be inspected with GDB
  LOG(INFO) << "Creating multiple fibers for GDB inspection";

  Fiber fb1("worker_alpha", FiberWorkFunction, "alpha", 5);
  Fiber fb2("worker_beta", FiberWorkFunction, "beta", 10);
  Fiber fb3("deep_stack", DeepStackFunction, 10, "deep");

  // Let fibers start
  ThisFiber::Yield();

  LOG(INFO) << "Fibers created, they can now be inspected with GDB";
  LOG(INFO) << "Use 'helio fiber list' to see all fibers";

  fb1.Join();
  fb2.Join();
  fb3.Join();
}

TEST_F(GdbFiberTest, FiberStates) {
  // Test different fiber states
  EventCount ec;
  bool ready = false;

  Fiber sleeping_fiber("sleeper", [&] {
    LOG(INFO) << "Sleeper fiber starting";
    ThisFiber::SleepFor(100ms);
    LOG(INFO) << "Sleeper fiber done";
  });

  Fiber waiting_fiber("waiter", [&] {
    LOG(INFO) << "Waiter fiber starting";
    ec.await([&] { return ready; });
    LOG(INFO) << "Waiter fiber done";
  });

  // Give fibers time to reach their wait points
  ThisFiber::SleepFor(10ms);
  LOG(INFO) << "Fibers in various wait states - good time to inspect with GDB";

  // Wake up waiting fiber
  ready = true;
  ec.notify();

  sleeping_fiber.Join();
  waiting_fiber.Join();
}

TEST_F(GdbFiberTest, PrintStackTraces) {
  // This test uses the built-in stack trace printing
  LOG(INFO) << "Testing built-in fiber stack trace printing";

  Fiber fb1("traced_fiber_1", [&] {
    for (int i = 0; i < 5; ++i) {
      ThisFiber::Yield();
    }
  });

  Fiber fb2("traced_fiber_2", [&] {
    for (int i = 0; i < 5; ++i) {
      ThisFiber::Yield();
    }
  });

  ThisFiber::Yield();

  // Use built-in function to print all fiber stack traces
  detail::PrintAllFiberStackTraces();

  fb1.Join();
  fb2.Join();
}

TEST_F(GdbFiberTest, FiberInfo) {
  // Test that exposes fiber information for GDB inspection
  LOG(INFO) << "Current fiber info:";
  LOG(INFO) << "  Name: " << ThisFiber::GetName();
  LOG(INFO) << "  Preempt count: " << ThisFiber::GetPreemptCount();
  LOG(INFO) << "  Worker fibers count: " << WorkerFibersCount();
  LOG(INFO) << "  Worker fibers stack size: " << WorkerFibersStackSize();

  Fiber fb("info_test_fiber", [&] {
    LOG(INFO) << "Inside test fiber:";
    LOG(INFO) << "  Name: " << ThisFiber::GetName();
    LOG(INFO) << "  Preempt count: " << ThisFiber::GetPreemptCount();
  });

  fb.Join();

  LOG(INFO) << "After fiber completion:";
  LOG(INFO) << "  Worker fibers count: " << WorkerFibersCount();
}

// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
// Checks that event_count.h is self-contained. This file includes no other fibers header, so it
// builds only if every inline and template member it uses is defined in event_count.h:
//  - An inline member defined in another header is a compile error: it is declared inline, and
//    the target turns GCC's "used but never defined" (with -Werror) and clang's
//    -Wundefined-inline (with -Werror=undefined-inline) into errors.
//  - A template member defined in another header is a link error: its instantiations for the
//    lambdas below exist nowhere else.
// Unlike a link failure of an optimized build, both happen in every build type.

#include "util/fibers/event_count.h"

#include <chrono>
#include <mutex>

#include "base/gtest.h"

namespace util {
namespace fb2 {

// Uses every inline and template member of event_count.h. Never called: waiting needs fibers.
void UseEventCountApi(EventCount* ec, EmbeddedBlockingCounter* counter, detail::Waiter* w,
                      base::SpinLock* mu) {
  ec->notify();
  ec->notifyAll();
  ec->await([] { return true; });
  {
    std::unique_lock lk(*mu);
    ec->await(lk, [] { return true; });
  }
  ec->await_until([] { return true; }, std::chrono::steady_clock::now());
  std::optional<EventCount::SubKey> sub = ec->check_or_subscribe([] { return false; }, w);
  EventCount::SubKey persistent = ec->subscribe_persistent(w);
  EventCount::Key key = ec->prepareWait();
  if (ec->subscribe(key.epoch(), w)) {
    ec->unsubscribe(w);
  }
  ec->finishWait();
  counter->Wait();
}

TEST(EventCountTest, NotifyWithoutWaiters) {
  EventCount ec;
  EXPECT_FALSE(ec.notify());
  EXPECT_FALSE(ec.notifyAll());
}

}  // namespace fb2
}  // namespace util

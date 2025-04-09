## Helio: High-Performance I/O and Concurrency

An opinionated library for backend development

[github.com/romange/helio](http://github.com/romange/helio)

---

## Repository Overview

**Purpose**:
- Efficient backend development using modern C++
- Lightweight fibers for task scheduling
- Supports Linux(epoll/io_uring)/FreeBSD/MacOS
- Key components:
  - Integrated logging: `VLOG/DVLOG` and invariant checks: `(D)CHECK_EQ`
  - Unit testing
  - Non-blocking file operations (even for non-io_uring)

Note: Helio is designed to work across multiple UNIX-like operating systems including Linux, FreeBSD, and MacOS.
On Linux, it leverages epoll and io_uring for efficient I/O operations. It's opinionated because
it dictates on how to parse arguments, how to use logging.

---

## Repository Structure

<div style="text-align: center;">
  <img src="assets/folders.svg" alt="Repository Structure" style="width: 20%; height: auto;">
</div>

 - `base/` - abstract data structures, memory management, absl and posix wrappers
 - `io/` - sinks, sources, files and fs utilities.
 - `strings/` - string related utilities that are not present in `absl/strings`.
 - `util` - advanced abstractions: fibers, proactors, html, http etc.

Note: The library is not very big, because it "extends" absl and compliments it with respect to io
and thread management. `base` does not have much third-party dependencies,
while `util` is dependent on everything else.

---

## Proactor

- Fibers scheduling & I/O handling
- Run-time checks (aka FiberAtomicGuard)
- Message passing

```cpp
// Example of cross-thread fiber execution
thread_local int j = 10;
j = 15;

int res = proactor->AwaitBrief([] { return j});
EXPECT_EQ(10, res);
```

Note: I will skip `base` and `io` and focus on `util/fibers`,
because I think it's the most distinct feature of helio.

---

## IoUring Dispatcher Loop

```cpp
while (true) {
  bool dirty = false;
  if (cb = deque_brief_task(queue)) {
    run(cb)
    dirty = true;
  }

  dirty |= sumbit_io_and_process_completions(uring);
  if (ScheduleReadFiber()) {
    dirty = true;
  }

  if (!dirty) {
    wait_for_io_or_task();  // The only place it waits.
  }
}
```

Notes: I would like to expand on this term "brief".
ring0 loop, never stops unless everything is idle, and then it waits on i/o.


---

## Messaging Proactor

```cpp
auto cb = [] { j++;};

// Brief - means cb can not preempt.
proactor->DispatchBrief(cb);
```


1. Add `cb` to proactor.brief_queue
4. (Maybe) notify proactor

---

## Messaging Proactor

```cpp
proactor->AwaitBrief([] { j++;});
```

1. Setup a condition variable `cv`
2. Wrap `cb` with `fun = [&] { cb(); cv.notify(); }`
3. Add `fun` to proactor.brief_queue.
4. Maybe notify proactor
5. call `cv.wait()`

---

## Unique condvar algorithm

Posix:
```cpp
unique_lock lk(mu)
state = true;
cnd.notify(lk);
lk.unlock();
```

Helio: EventCount.

```cpp
atomic_var.store(true);
event_count.notify();   // lock free
```

Notes: can be used from ring0 scheduler loop.

---
## Usual primitives

1. fb2::Mutex
2. fb2::CondVar
3. fb2::CondVarAny / NoOpLock
4. fb2::Barrier - coordinates N processes.
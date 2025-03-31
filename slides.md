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
On Linux, it leverages epoll and io_uring for efficient I/O operations.

---

## Repository Structure

<div style="text-align: center;">
  <img src="assets/folders.svg" alt="Repository Structure" style="width: 20%; height: auto;">
</div>

 - `base/` - data structures, memory management, absl and posix wrappers
 - `io/` - sinks, sources, files and fs utilities.
 - `strings/` - string related utilities that are not present in `absl/strings`.
 - `util` - advanced abstractions: fibers, proactors, html, http etc.

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

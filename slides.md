## Helio: High-Performance I/O and Concurrency

An opinionated library for backend development

[github.com/romange/helio](http://github.com/romange/helio)

---

## Repository Overview

**Purpose**:
- Efficient I/O and concurrency using modern C++
- Key components:
  - Proactor classes
  - Fiber-based coroutines
  - Non-blocking file operations

**Features**:

✔️ Lightweight fibers for task scheduling </br>
✔️ Proactor architecture for scalable I/O </br>
✔️ Mostly posix-based abstractions </br>

---

## Proactor

- Fibers scheduling fibers
- I/O handling
- Run-time checks (FiberAtomicGuard)
- Message passing

```cpp


thread_local int j = 10;
j = 15;

int res = proactor->AwaitBrief([] { return j});
EXPECT_EQ(10, res);
```
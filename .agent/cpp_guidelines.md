# Antigravity Agent Guidelines for Helio C++ Project

This document provides comprehensive guidelines for writing high-quality C++ code in the Helio project. These guidelines help ensure consistency, safety, and maintainability across the codebase.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Code Style and Formatting](#code-style-and-formatting)
3. [Fiber-Safe Programming](#fiber-safe-programming)
4. [Error Handling](#error-handling)
5. [Memory Management](#memory-management)
6. [Logging and Debugging](#logging-and-debugging)
7. [Testing](#testing)
8. [Build System](#build-system)
9. [Common Patterns](#common-patterns)

---

## Project Overview

**Helio** is a backend development framework in C++ using io_uring and epoll event-loops. Key characteristics:

- **C++17** standard
- **Dependencies**: abseil-cpp, Boost 1.71+
- **Build system**: CMake + Ninja
- **Event loops**: io_uring (primary), epoll (fallback)
- **Concurrency**: Custom fiber library (fb2) derived from Boost.Fiber
- **Logging**: glog or absl logging
- **HTTP**: Boost.Beast integration
- **Namespaces**: `util`, `base`, `io`, `util::fb2` (fibers)

---

## Code Style and Formatting

### Formatting Rules

The project uses **Google C++ Style** with modifications defined in `.clang-format`:

- **Indentation**: 2 spaces
- **Column limit**: 100 characters
- **Pointer alignment**: Left (`int* ptr`, not `int *ptr`)
- **No short functions/loops/ifs on single line**
- **Template declarations**: Not always broken

### File Headers

All source files must include a copyright header:

```cpp
// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
```

### Naming Conventions

- **Classes/Structs**: PascalCase (`UringSocket`, `FiberInterface`)
- **Functions/Methods**: PascalCase (`WriteSome`, `GetProactor`)
- **Variables**: snake_case (`error_cb_wrapper_`, `recv_poll_id_`)
- **Private members**: Trailing underscore (`flags_`, `bufring_id_`)
- **Constants**: kPascalCase (`kFdShift`, `kHighCycleBound`)
- **Namespaces**: snake_case (`util::fb2`, `base`)

### Include Order

1. Related header (for .cc files)
2. C system headers
3. C++ standard library headers
4. Third-party library headers (Boost, abseil)
5. Project headers

Use quotes for project headers, angle brackets for system/third-party:

```cpp
#include "util/fibers/uring_socket.h"

#include <liburing.h>

#include <chrono>
#include <condition_variable>

#include <absl/strings/str_cat.h>
#include <boost/context/fiber.hpp>

#include "util/fiber_socket_base.h"
#include "util/fibers/uring_proactor.h"
```

---

## Fiber-Safe Programming

### Critical Rules

The Helio project uses a custom fiber library (`util::fb2`) for cooperative multitasking. **Fiber-unsafe code can block entire threads.**

#### ❌ Never Use Standard Synchronization Primitives in Fiber Code

```cpp
// WRONG - blocks entire thread
std::mutex mutex;
std::condition_variable cv;
std::this_thread::sleep_for(std::chrono::seconds(1));
```

#### ✅ Use Fiber-Safe Alternatives

```cpp
// CORRECT - fiber-aware
util::fb2::Mutex mutex;
util::fb2::CondVar cv;
util::fb2::Fiber::SleepFor(std::chrono::seconds(1));
```

### Fiber-Safe Primitives

| Standard Library | Fiber-Safe Alternative |
|-----------------|------------------------|
| `std::mutex` | `util::fb2::Mutex` |
| `std::condition_variable` | `util::fb2::CondVar` |
| `std::this_thread::sleep_for` | `util::fb2::Fiber::SleepFor` |
| Blocking syscalls | Proactor facilities |

### Proactor Pattern

Use the Proactor for I/O operations:

```cpp
// Get the current proactor
UringProactor* proactor = GetProactor();

// Schedule fiber-friendly I/O
proactor->Await([&](auto* proactor) {
  // I/O operation here
});
```

---

## Error Handling

### Use `io::Result<T>` for Error Reporting

The project uses a Rust-inspired `io::Result<T>` type for error handling instead of exceptions:

```cpp
// Function returning Result
io::Result<size_t> WriteSome(const iovec* v, uint32_t len);

// Check for errors
io::Result<size_t> result = socket->WriteSome(buf, len);
if (!result) {
  // Handle error
  std::error_code ec = result.error();
  LOG(ERROR) << "Write failed: " << ec.message();
  return;
}

// Use the value
size_t bytes_written = *result;
```

### Use `std::error_code` for System Errors

```cpp
error_code Connect(const endpoint_type& ep);

error_code ec = socket->Connect(endpoint);
if (ec) {
  LOG(ERROR) << "Connection failed: " << ec.message();
}
```

### Mark Functions with `ABSL_MUST_USE_RESULT`

```cpp
ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep);
ABSL_MUST_USE_RESULT AcceptResult Accept() final;
```

This ensures callers don't ignore error returns.

### Avoid Exceptions

The project prefers error codes over exceptions. Use exceptions only when:
- Interfacing with third-party libraries that require them
- Handling truly exceptional, unrecoverable errors

---

## Memory Management

### RAII and Smart Pointers

Use RAII for resource management:

```cpp
// Use unique_ptr for owned resources
std::unique_ptr<FiberSocketBase> socket = CreateSocket();

// Use shared_ptr when shared ownership is needed
std::shared_ptr<Context> ctx = std::make_shared<Context>();
```

### Use `absl::Cleanup` for Scope Guards

```cpp
#include <absl/cleanup/cleanup.h>

void ProcessRequest() {
  SSL_CTX* ctx = CreateContext();
  absl::Cleanup cleanup([ctx] {
    FreeContext(ctx);
  });

  // Use ctx...
  // Automatically cleaned up on scope exit
}
```

### Avoid Raw `new`/`delete`

Prefer smart pointers or factory functions that return smart pointers.

---

## Logging and Debugging

### Logging Macros

The project uses glog or absl logging:

```cpp
#include "base/logging.h"

// Standard logging
LOG(INFO) << "Server started on port " << port;
LOG(WARNING) << "Connection timeout: " << timeout_ms << "ms";
LOG(ERROR) << "Failed to bind socket: " << ec.message();
LOG(FATAL) << "Critical error, terminating";

// Verbose logging (controlled at runtime)
VLOG(1) << "Detailed debug info";
VLOG(2) << "Very detailed debug info";

// Conditional logging
LOG_IF(WARNING, retries > 3) << "Multiple retries: " << retries;
```

### Assertions and Invariants

Use `CHECK` and `DCHECK` macros:

```cpp
// CHECK - Always enabled, terminates on failure
CHECK(ptr != nullptr);
CHECK_EQ(expected, actual);
CHECK_GT(value, 0);
CHECK_LT(index, size);
CHECK_GE(count, min_count);
CHECK_LE(count, max_count);
CHECK_NE(a, b);

// DCHECK - Only enabled in debug builds
DCHECK(invariant_holds);
DCHECK_EQ(ref_count, 0);
DCHECK_GT(buffer_size, 0);
```

**Guidelines:**
- Use `CHECK` for critical invariants that must always hold
- Use `DCHECK` for expensive checks or debug-only validation
- Prefer `CHECK`/`DCHECK` over `assert()` for better error messages

### Avoid `std::cout`/`std::cerr`

```cpp
// WRONG
std::cout << "Debug message" << std::endl;

// CORRECT
LOG(INFO) << "Debug message";
VLOG(1) << "Debug message";
```

---

## Testing

### Test Framework

The project uses **GoogleTest** and **GoogleMock**:

```cpp
#include <gtest/gtest.h>
#include <gmock/gmock.h>

TEST(UringSocketTest, BasicConnect) {
  // Test implementation
}

TEST_F(FiberTestFixture, SwitchAndExecute) {
  // Test with fixture
}
```

### Test File Naming

- Test files: `*_test.cc`
- Place tests alongside the code they test

### Running Tests

```bash
# Build tests
cd build-opt && ninja -j4 fiber_socket_test

# Run specific test
./fiber_socket_test

# Run with ASAN (sanitizers)
cd build-san && ninja -j4 fiber_socket_test
./fiber_socket_test
```

### Test Coverage

The project uses codecov for coverage tracking. Ensure new code has adequate test coverage.

---

## Build System

### CMake Conventions

- **Third-party packages**: `TRDP::` prefix
- **Abseil libraries**: `absl::` prefix
- **Build types**:
  - `build-opt`: Optimized release build
  - `build-dbg`: Debug build
  - `build-san`: Sanitizer build (ASAN/USAN)

### Building the Project

```bash
# Install dependencies
sudo ./install-dependencies.sh

# Build release version
./blaze.sh -release
cd build-opt && ninja -j4

# Build debug version
./blaze.sh -debug
cd build-dbg && ninja -j4

# Build with sanitizers
./blaze.sh -sanitize
cd build-san && ninja -j4
```

### Compiler Flags

The project supports:
- **ASAN**: Address Sanitizer (`-fsanitize=address`)
- **USAN**: Undefined Behavior Sanitizer (`-fsanitize=undefined`)
- **Thread safety**: Clang thread safety annotations (`-Wthread-safety`)


### Namespace Closing Comments

```cpp
}  // namespace fb2
}  // namespace util
```

---

## Additional Best Practices

### String Handling

Prefer `std::string_view` or `absl::string_view` for read-only string parameters:

```cpp
// GOOD
void ProcessName(std::string_view name);

// LESS GOOD
void ProcessName(const std::string& name);
```

### Const Correctness

- Mark methods `const` when they don't modify object state
- Use `const` references for large objects passed as parameters
- Provide both const and non-const overloads when appropriate

```cpp
const UringProactor* GetProactor() const {
  return static_cast<const UringProactor*>(proactor());
}

UringProactor* GetProactor() {
  return static_cast<UringProactor*>(proactor());
}
```

### Move Semantics

Use move semantics for expensive-to-copy objects:

```cpp
void SetCallback(std::function<void()> cb) {
  callback_ = std::move(cb);
}
```

### Initialization

Prefer in-class member initialization:

```cpp
class Socket {
 private:
  ErrorCbRefWrapper* error_cb_wrapper_ = nullptr;
  uint16_t bufring_id_ = 0;
};
```

---

## Summary Checklist

When writing code for Helio, ensure:

- ✅ Code follows Google C++ style (2-space indent, 100 char limit)
- ✅ Copyright header is present
- ✅ No `std::mutex` or `std::condition_variable` in fiber code
- ✅ Use `io::Result<T>` for error-prone operations
- ✅ Mark error-returning functions with `ABSL_MUST_USE_RESULT`
- ✅ Use `LOG()/VLOG()` instead of `std::cout`
- ✅ Use `CHECK`/`DCHECK` for invariants
- ✅ Use `absl::Cleanup` for RAII cleanup
- ✅ Prefer smart pointers over raw `new`/`delete`
- ✅ Include tests for new functionality
- ✅ Use `override`/`final` for virtual methods
- ✅ Prefer `string_view` for read-only strings
- ✅ Close namespaces with comments

---

## References

- [Project README](../README.md)
- [Code Review Guidelines](../.augment/code_review_guidelines.yaml)
- [.clang-format](../.clang-format)
- [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
- [Abseil Documentation](https://abseil.io/docs/cpp/)

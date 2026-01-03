# Helio Agent Guidelines

**Helio** is a high-performance C++17/20 backend framework leveraging `io_uring` and fibers.

## ⚡ Critical Core Mandates

1.  **Fiber Safety is Paramount:**
    *   **NEVER** use `std::mutex`, `std::condition_variable`, or `std::thread::sleep_for`. These block the entire thread.
    *   **ALWAYS** use `util::fb2::Mutex`, `util::fb2::CondVar`, and `util::fb2::Fiber::SleepFor`.
    *   Use `Proactor` for all I/O operations.

2.  **Error Handling:**
    *   Use `io::Result<T>` (Rust-style) for operations that can fail.
    *   Use `std::error_code` for system errors.
    *   **Avoid exceptions** unless absolutely necessary (e.g., 3rd party libs).
    *   Mark error-returning functions with `ABSL_MUST_USE_RESULT`.

3.  **Code Style & Conventions:**
    *   **Style:** Google C++ Style with 2-space indentation (enforced by `.clang-format`).
    *   **Prefixes:** `TRDP::` for third-party, `absl::` for Abseil, `util::fb2::` for fibers.
    *   **Logging:** Use `glog`: `LOG(INFO)`, `VLOG(1)`, `CHECK(ptr)`. Avoid `std::cout`.
    *   **Headers:** Project headers use quotes (`"util/fibers/uring_socket.h"`).
    *   **Namespaces:** Close with comments (`}  // namespace fb2`).

## 🛠️ Build & Test Cheatsheet

| Task | Command |
| :--- | :--- |
| **Configure (Release)** | `./blaze.sh -release` |
| **Configure (Debug)** | `./blaze.sh` |
| **Build Target** | `cd build-opt && ninja -j4 <target_name>` |
| **Build All** | `cd build-opt && ninja -j4 base/all io/all util/all` |
| **Run Tests (CI)** | `GLOG_logtostderr=1 ctest -V -L CI` |
| **Run Specific Test** | `./fiber_test --logtostderr` (in build dir) |
| **Format Code** | `clang-format -i <file>` |

## 📂 Key Directories

*   **`util/fibers/`**: Core fiber implementation (sockets, synchronization).
*   **`util/http/`**: HTTP server/client implementation.
*   **`examples/`**: Reference implementations (e.g., `echo_server.cc`).
*   **`base/`**: Low-level utilities (logging, flags).
*   **`io/`**: File I/O utilities.

## 🚀 Workflow

1.  **Read:** Check `README.md` for project context.
2.  **Implement:** Adhere strictly to Fiber Safety rules. Use smart pointers (`std::unique_ptr`) and RAII (`absl::Cleanup`).
3.  **Verify:** Always run related tests and `ctest -L CI`.

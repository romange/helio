# Helio - Copilot Coding Agent Instructions

## Repository Overview
Helio is a C++17/20 backend development framework for Linux systems using io_uring and epoll event-loops. It provides fiber-based asynchronous I/O primitives, HTTP server implementation, and fiber-friendly synchronization primitives.

**Key dependencies:** abseil-cpp, Boost (1.76+), CMake 3.16+, Ninja, GoogleTest/GoogleMock

## Project Structure
```
helio/
├── base/           # Core utilities: logging, flags, hashing, histograms, memory pools
├── io/             # File I/O utilities, zstd compression
├── strings/        # String manipulation utilities
├── util/           # Main framework code
│   ├── fibers/     # Fiber implementation (proactors, sockets, synchronization)
│   ├── http/       # HTTP server/client implementation
│   ├── tls/        # TLS support
│   ├── aws/        # AWS S3 client
│   └── cloud/      # Cloud providers (GCP, Azure)
├── examples/       # Example applications (echo_server, s3_demo, etc.)
├── cmake/          # CMake modules (third_party.cmake, internal.cmake)
├── patches/        # Patches for third-party dependencies
└── tools/docker/   # Docker configurations
```

## Build System
This project uses **CMake with Ninja** generator. Build artifacts are created in `build-dbg/` (debug) or `build-opt/` (release).

### Build Commands
```bash
# Configure debug build (creates build-dbg/)
./blaze.sh

# Configure release build (creates build-opt/)
./blaze.sh -release

# Build all targets in a module (from build directory)
cd build-dbg && ninja base/all io/all strings/all util/all

# Build specific executables
ninja echo_server https_client_cli s3_demo

# Run all CI-labeled tests
GLOG_logtostderr=1 ctest -V -L CI
```

### Build Options
- `-clang` - Use clang compiler (creates clang-dbg/ or clang-opt/)
- `-DHELIO_USE_SANITIZER=ON` - Enable address/undefined sanitizers
- `-DWITH_AWS=ON/OFF` - Enable/disable AWS S3 client
- `-DWITH_GCP=ON/OFF` - Enable/disable GCP client
- `-DWITH_GPERF=ON/OFF` - Enable/disable gperf profiling

## Testing
Tests use GoogleTest framework. Tests with `LABELS CI` are run in CI pipelines.

```bash
# Run specific test with logging
./fibers_test --logtostderr

# Run all CI tests
GLOG_logtostderr=1 ctest -V -L CI

# Run with gdb for debugging segfaults
gdb --batch -ex r --args ./fibers_test --logtostderr
```

### Test Files
Tests are defined using `helio_cxx_test()` macro in CMakeLists.txt files with format:
```cmake
helio_cxx_test(test_name library_deps LABELS CI)
```

## Code Style & Formatting
- **Style:** Google C++ Style Guide (modified)
- **Formatter:** clang-format v14 (config in `.clang-format`)
- **Column limit:** 100 characters
- **Indent:** 2 spaces

### Pre-commit Hooks
```yaml
# .pre-commit-config.yaml
- trailing-whitespace removal
- end-of-file-fixer
- clang-format (v14.0.6)
```

Run formatting: `clang-format -i <file>`

## Key Conventions
- Third-party libraries use `TRDP::` prefix (e.g., `TRDP::xxhash`, `TRDP::uring`)
- Abseil libraries use `absl::` prefix
- Boost libraries use `Boost::` prefix
- Tests are linked with `gtest_main_ext` which includes gmock and base library
- Use `cxx_link()` for linking targets, `helio_cxx_test()` for test targets
- The `USE_FB2` define is enabled by default for fibers support
- Logging uses glog library (`LOG(INFO)`, `VLOG()`, `CHECK()`)

## CI Pipeline (.github/workflows/ci.yml)
Tests run on multiple containers: ubuntu-dev:20, ubuntu-dev:24, alpine-dev, fedora:30

Key CI steps:
1. CMake configure with Ninja generator
2. Build: `ninja -k 5 base/all io/all strings/all util/all echo_server`
3. Test: `GLOG_logtostderr=1 ctest -V -L CI`

**Important:** io_uring requires `--security-opt seccomp=unconfined` in Docker.

## Adding New Code
### New source file in existing module
1. Add file to module's `CMakeLists.txt` library definition
2. Link required dependencies with `cxx_link()`

### New test
```cmake
helio_cxx_test(my_test library_deps LABELS CI)
```

### New executable
```cmake
add_executable(my_app my_app.cc)
cxx_link(my_app base fibers2 <other_deps>)
```

## Common Issues & Solutions
- **Boost not found:** Ensure Boost 1.76+ is installed. CI uses pre-built containers.
- **io_uring failures:** May need kernel 5.10+ or Docker seccomp disabled
- **Third-party build failures:** Check `build-*/third_party/src/*-stamp/*.log`
- **Sanitizer conflicts:** Don't use ASAN with mimalloc simultaneously

## Quick Reference
| Action | Command |
|--------|---------|
| Configure debug | `./blaze.sh` |
| Configure release | `./blaze.sh -release` |
| Build all | `cd build-dbg && ninja` |
| Run CI tests | `GLOG_logtostderr=1 ctest -V -L CI` |
| Format code | `clang-format -i <file>` |

This documentation is comprehensive. Additional exploration should only be needed for edge cases not covered here.
